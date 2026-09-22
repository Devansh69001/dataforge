"""Pipeline task implementations.

Each task takes a `RunContext` and returns a small, JSON-serialisable summary. Tasks are
process-independent: everything they need from earlier tasks is read from the run
document (`tracker.state`), so the same functions back the sequential CLI runner and the
distributed Airflow DAG.
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from typing import Any

from ..config import REPO_ROOT, Settings, get_settings
from ..db import apply_ddl, ping
from ..ingestion.bronze import API_DATASETS, SOURCE_LAYOUT, IngestResult, ingest_file, source_path
from ..ingestion.bronze import ingest_reference as ingest_reference_dataset
from ..ingestion.ledger import IngestionLedger
from ..ingestion.schemas import SchemaDriftError
from ..ingestion.sources.api_source import ReferenceApiClient
from ..ingestion.sources.base import SourceError
from ..logging_utils import get_logger
from ..monitoring.tracker import RunTracker, read_watermarks
from ..quality.suites import SUITES
from ..quality.warehouse_checks import run_warehouse_checks, summarize

log = get_logger("pipeline")

INGEST_GROUPS: dict[str, list[str]] = {
    "ingest_customers": ["customers"],
    "ingest_catalog": ["products", "categories", "suppliers"],
    "ingest_orders": ["orders", "order_items", "payments"],
    "ingest_inventory": ["inventory_events"],
    "ingest_shipping": ["shipping_events"],
    "ingest_reference": list(API_DATASETS),
}
SILVER_GROUPS: dict[str, list[str]] = {
    "silver_reference": ["regions", "warehouses", "categories", "suppliers", "exchange_rates"],
    "silver_customers": ["customers"],
    "silver_products": ["products"],
    "silver_orders": ["orders"],
    "silver_order_items": ["order_items"],
    "silver_payments": ["payments"],
    "silver_inventory": ["inventory_events"],
    "silver_shipping": ["shipping_events"],
}
EVENT_DATASETS = ["orders", "order_items", "payments", "inventory_events", "shipping_events"]


@dataclass
class RunContext:
    tracker: RunTracker
    batch_id: str
    mode: str  # initial | incremental | rerun
    settings: Settings
    force_ingest: bool = False

    @property
    def run_id(self) -> str:
        return self.tracker.run_id

    @property
    def full_rebuild(self) -> bool:
        return self.mode == "initial"


def make_context(
    run_id: str,
    batch_id: str,
    mode: str | None = None,
    settings: Settings | None = None,
    triggered_by: str = "cli",
    force_ingest: bool = False,
) -> RunContext:
    """Create (or re-open) the run document. Mode is inferred from watermarks when omitted:
    no watermark -> initial; watermark < batch -> incremental; watermark >= batch -> rerun."""
    settings = settings or get_settings()
    if mode is None:
        wm = read_watermarks(settings)
        last = max((w["last_batch_id"] for w in wm.values() if w.get("last_batch_id")), default=None)
        mode = "initial" if not last else ("rerun" if last >= batch_id else "incremental")
    path = settings.monitoring_dir / "runs" / f"{run_id}.json"
    tracker = (
        RunTracker.load(run_id, settings)
        if path.exists()
        else RunTracker(run_id, mode, batch_id, settings, triggered_by)
    )
    return RunContext(
        tracker=tracker, batch_id=batch_id, mode=tracker.mode, settings=settings, force_ingest=force_ingest
    )


# ------------------------------------------------------------------ tasks
def check_sources(ctx: RunContext) -> dict[str, Any]:
    with ctx.tracker.task("check_sources") as rec:
        missing = []
        seen = set()
        for ds in SOURCE_LAYOUT:
            p = source_path(ds, ctx.batch_id, ctx.settings.raw_dir)
            if p in seen:
                continue
            seen.add(p)
            if not p.exists():
                missing.append(str(p))
        client = ReferenceApiClient(ctx.settings.reference_api_url)
        try:
            client.fetch("regions")
            api_ok = True
        except SourceError as e:
            api_ok = False
            missing.append(f"reference API: {e}")
        rec["details"] = {"files_checked": len(seen), "api_ok": api_ok, "missing": missing}
        if missing:
            raise SourceError(f"batch {ctx.batch_id} is incomplete: {missing}")
        return rec["details"]


def init_warehouse(ctx: RunContext) -> dict[str, Any]:
    with ctx.tracker.task("init_warehouse") as rec:
        if not ping(ctx.settings):
            raise RuntimeError(f"PostgreSQL unreachable at {ctx.settings.redacted_dsn()}")
        files = apply_ddl(ctx.settings)
        rec["details"] = {"ddl_files": files}
        ctx.tracker.start()  # (re)register the run now that monitoring tables exist
        return rec["details"]


def _ingest(ctx: RunContext, task_name: str) -> dict[str, Any]:
    with ctx.tracker.task(task_name) as rec:
        results: list[IngestResult] = []
        for ds in INGEST_GROUPS[task_name]:
            if ds in API_DATASETS:
                results.append(ingest_reference_dataset(ds, ctx.batch_id, ctx.run_id, ctx.settings))
            else:
                results.append(
                    ingest_file(ds, ctx.batch_id, ctx.run_id, force=ctx.force_ingest, settings=ctx.settings)
                )
        rows = sum(r.rows for r in results if not r.skipped)
        rejected = sum(r.rejected for r in results)
        rec["rows_in"], rec["rows_out"], rec["rows_rejected"] = rows + rejected, rows, rejected
        rec["details"] = {
            r.dataset: {
                "rows": r.rows,
                "rejected": r.rejected,
                "skipped": r.skipped,
                "bronze_path": r.bronze_path,
            }
            for r in results
        }
        ctx.tracker.add_totals(rows_ingested=rows, rows_rejected=rejected)
        events = [e.as_dict() for r in results for e in r.schema_events]
        ctx.tracker.record_schema_events(events)
        ctx.tracker.state.setdefault("schema_events", []).extend(events)
        ctx.tracker.state.setdefault("ingested", {}).update({r.dataset: (not r.skipped) for r in results})
        ctx.tracker.save()
        ledger = IngestionLedger(ctx.settings.bronze_dir / "_ledger.json")
        ctx.tracker.record_ingestion([e for e in ledger.entries() if e["run_id"] == ctx.run_id])
        return rec["details"]


def ingest_customers(ctx: RunContext) -> dict[str, Any]:
    return _ingest(ctx, "ingest_customers")


def ingest_catalog(ctx: RunContext) -> dict[str, Any]:
    return _ingest(ctx, "ingest_catalog")


def ingest_orders(ctx: RunContext) -> dict[str, Any]:
    return _ingest(ctx, "ingest_orders")


def ingest_inventory(ctx: RunContext) -> dict[str, Any]:
    return _ingest(ctx, "ingest_inventory")


def ingest_shipping(ctx: RunContext) -> dict[str, Any]:
    return _ingest(ctx, "ingest_shipping")


def ingest_reference(ctx: RunContext) -> dict[str, Any]:
    return _ingest(ctx, "ingest_reference")


def detect_schema_drift(ctx: RunContext) -> dict[str, Any]:
    with ctx.tracker.task("detect_schema_drift") as rec:
        events = ctx.tracker.state.get("schema_events", [])
        by_sev: dict[str, int] = {}
        for e in events:
            by_sev[e["severity"]] = by_sev.get(e["severity"], 0) + 1
        new_cols = [f"{e['dataset']}.{e['column']}" for e in events if e["change_type"] == "new_column"]
        rec["details"] = {"events": len(events), "by_severity": by_sev, "new_columns": new_cols}
        if by_sev.get("ERROR"):
            raise SchemaDriftError(
                f"breaking schema drift detected: {[e for e in events if e['severity'] == 'ERROR']}"
            )
        if new_cols:
            log.warning(
                "new source columns detected; kept in bronze, ignored by silver until the registry is updated",
                columns=new_cols,
            )
        return rec["details"]


def _silver(ctx: RunContext, task_name: str) -> dict[str, Any]:
    from ..spark.session import get_spark
    from ..spark.silver.runner import JOB_BY_DATASET

    with ctx.tracker.task(task_name) as rec:
        spark = get_spark(settings=ctx.settings)
        batches = [ctx.batch_id]
        summary: dict[str, Any] = {}
        rows_in = rows_out = rejected = 0
        for ds in SILVER_GROUPS[task_name]:
            res = JOB_BY_DATASET[ds](spark, batches, ctx.run_id, ctx.settings)
            summary[ds] = res.as_dict()
            rows_in += res.input_rows
            rows_out += res.valid_rows
            rejected += res.rejected_rows
            if res.rule_stats:
                # a silver rule "passes" while its failure rate stays under the dataset's circuit breaker;
                # the rejected rows themselves are the expected outcome and live in quarantine
                threshold = SUITES[ds].max_reject_rate
                ctx.tracker.record_quality(
                    "silver",
                    ds,
                    [
                        {
                            "rule_id": s["rule_id"],
                            "severity": s["severity"],
                            "total_rows": s["total"],
                            "failed_rows": s["failed"],
                            "failure_rate": s["failure_rate"],
                            "threshold": threshold,
                            "passed": s["failure_rate"] <= threshold,
                        }
                        for s in res.rule_stats
                    ],
                )
            if res.partitions_written:
                touched = ctx.tracker.state.setdefault("touched", {})
                touched[ds] = sorted(set(touched.get(ds, [])) | set(res.partitions_written))
        rec["rows_in"], rec["rows_out"], rec["rows_rejected"] = rows_in, rows_out, rejected
        rec["details"] = {ds: {k: v for k, v in s.items() if k != "rule_stats"} for ds, s in summary.items()}
        ctx.tracker.add_totals(rows_quarantined=rejected)
        ctx.tracker.save()
        return rec["details"]


def silver_reference(ctx: RunContext) -> dict[str, Any]:
    return _silver(ctx, "silver_reference")


def silver_customers(ctx: RunContext) -> dict[str, Any]:
    return _silver(ctx, "silver_customers")


def silver_products(ctx: RunContext) -> dict[str, Any]:
    return _silver(ctx, "silver_products")


def silver_orders(ctx: RunContext) -> dict[str, Any]:
    return _silver(ctx, "silver_orders")


def silver_order_items(ctx: RunContext) -> dict[str, Any]:
    return _silver(ctx, "silver_order_items")


def silver_payments(ctx: RunContext) -> dict[str, Any]:
    return _silver(ctx, "silver_payments")


def silver_inventory(ctx: RunContext) -> dict[str, Any]:
    return _silver(ctx, "silver_inventory")


def silver_shipping(ctx: RunContext) -> dict[str, Any]:
    return _silver(ctx, "silver_shipping")


def gold_dimensions(ctx: RunContext) -> dict[str, Any]:
    from ..spark.gold.dimensions import build_dimensions
    from ..spark.session import get_spark

    with ctx.tracker.task("gold_dimensions") as rec:
        res = build_dimensions(get_spark(settings=ctx.settings), ctx.settings)
        rec["rows_out"] = sum(r.rows for r in res.values())
        rec["details"] = {k: v.as_dict() for k, v in res.items()}
        return rec["details"]


def gold_facts(ctx: RunContext) -> dict[str, Any]:
    from ..spark.gold.aggregates import agg_inventory_position, agg_product_daily_sales
    from ..spark.gold.facts import fact_inventory, fact_order_items, fact_orders, fact_shipping
    from ..spark.gold.runner import _union, shipping_order_months
    from ..spark.session import get_spark

    with ctx.tracker.task("gold_facts") as rec:
        spark = get_spark(settings=ctx.settings)
        t = ctx.tracker.state.get("touched", {})
        if ctx.full_rebuild:
            order_months = inv_months = ship_months = None
        else:
            order_months = _union(t.get("orders"), t.get("order_items"), t.get("payments"))
            inv_months = _union(t.get("inventory_events"))
            ship_months = _union(
                t.get("orders"), shipping_order_months(spark, t.get("shipping_events"), ctx.settings)
            )
        log.info(
            "gold scope",
            full_rebuild=ctx.full_rebuild,
            order_months=order_months,
            inventory_months=inv_months,
            shipping_months=ship_months,
        )
        results = {
            "fact_orders": fact_orders(spark, order_months, ctx.settings),
            "fact_order_items": fact_order_items(spark, order_months, ctx.settings),
            "fact_inventory": fact_inventory(spark, inv_months, ctx.settings),
            "fact_shipping": fact_shipping(spark, ship_months, ctx.settings),
            "agg_product_daily_sales": agg_product_daily_sales(spark, order_months, ctx.settings),
            "agg_inventory_position": agg_inventory_position(spark, ctx.settings),
        }
        ctx.tracker.state["gold_partitions"] = {
            k: (v.partitions if not ctx.full_rebuild else None) for k, v in results.items()
        }
        ctx.tracker.save()
        rec["rows_out"] = sum(r.rows for r in results.values())
        rec["details"] = {k: v.as_dict() for k, v in results.items()}
        return rec["details"]


def load_dimensions(ctx: RunContext) -> dict[str, Any]:
    from ..warehouse.loader import DIMENSIONS, load_table

    with ctx.tracker.task("load_dimensions") as rec:
        res = {t: load_table(t, None, ctx.settings) for t in DIMENSIONS}
        rec["rows_in"] = sum(r.rows_read for r in res.values())
        rec["rows_out"] = sum(r.rows_inserted + r.rows_updated for r in res.values())
        rec["details"] = {k: v.as_dict() for k, v in res.items()}
        ctx.tracker.add_totals(rows_loaded=rec["rows_out"])
        return rec["details"]


def load_facts(ctx: RunContext) -> dict[str, Any]:
    from ..warehouse.loader import FACTS, load_table

    with ctx.tracker.task("load_facts") as rec:
        parts = ctx.tracker.state.get("gold_partitions", {})
        res = {t: load_table(t, parts.get(t), ctx.settings) for t in FACTS}
        rec["rows_in"] = sum(r.rows_read for r in res.values())
        rec["rows_out"] = sum(r.rows_inserted + r.rows_updated for r in res.values())
        rec["details"] = {k: v.as_dict() for k, v in res.items()}
        ctx.tracker.add_totals(rows_loaded=rec["rows_out"])
        return rec["details"]


def _dbt(ctx: RunContext, command: str) -> dict[str, Any]:
    dbt_dir = REPO_ROOT / "dbt"
    env = {
        **os.environ,
        "POSTGRES_HOST": ctx.settings.postgres_host,
        "POSTGRES_PORT": str(ctx.settings.postgres_port),
        "POSTGRES_USER": ctx.settings.postgres_user,
        "POSTGRES_PASSWORD": ctx.settings.postgres_password,
        "POSTGRES_DB": ctx.settings.postgres_db,
        "DBT_TARGET": ctx.settings.dbt_target,
    }
    exe = os.environ.get("DBT_EXECUTABLE", "dbt")
    args = [exe, command, "--profiles-dir", str(dbt_dir), "--project-dir", str(dbt_dir), "--no-use-colors"]
    if command == "run" and ctx.full_rebuild:
        args.append("--full-refresh")
    log.info("running dbt", args=" ".join(args[:2]))
    proc = subprocess.run(args, env=env, capture_output=True, text=True, cwd=str(REPO_ROOT))
    results_file = dbt_dir / "target" / "run_results.json"
    summary: dict[str, Any] = {
        "returncode": proc.returncode,
        "stdout_tail": proc.stdout[-3000:],
        "stderr_tail": proc.stderr[-1500:],
    }
    if results_file.exists():
        rr = json.loads(results_file.read_text(encoding="utf-8"))
        statuses: dict[str, int] = {}
        failures = []
        for r in rr.get("results", []):
            statuses[r["status"]] = statuses.get(r["status"], 0) + 1
            if r["status"] in ("error", "fail"):
                failures.append(
                    {"node": r["unique_id"], "status": r["status"], "message": (r.get("message") or "")[:300]}
                )
        summary["statuses"] = statuses
        summary["failures"] = failures
        summary["elapsed"] = rr.get("elapsed_time")
    if proc.returncode != 0:
        raise RuntimeError(
            f"dbt {command} failed: {summary.get('failures') or proc.stdout[-800:] or proc.stderr[-800:]}"
        )
    return summary


def dbt_run(ctx: RunContext) -> dict[str, Any]:
    with ctx.tracker.task("dbt_run") as rec:
        rec["details"] = _dbt(ctx, "run")
        return rec["details"]


def dbt_test(ctx: RunContext) -> dict[str, Any]:
    with ctx.tracker.task("dbt_test") as rec:
        rec["details"] = _dbt(ctx, "test")
        st = rec["details"].get("statuses", {})
        ctx.tracker.record_quality(
            "dbt",
            "marts",
            [
                {
                    "rule_id": "dbt.tests",
                    "severity": "error",
                    "total_rows": sum(st.values()),
                    "failed_rows": st.get("fail", 0) + st.get("error", 0),
                    "failure_rate": 0.0,
                    "threshold": 0.0,
                    "passed": not (st.get("fail") or st.get("error")),
                    "details": st,
                }
            ],
        )
        return rec["details"]


def warehouse_quality_checks(ctx: RunContext) -> dict[str, Any]:
    with ctx.tracker.task("warehouse_quality_checks") as rec:
        results = run_warehouse_checks(ctx.batch_id, ctx.settings)
        status = summarize(results)
        ctx.tracker.record_quality("warehouse", "warehouse", results)
        ctx.tracker.doc["quality_status"] = status
        ctx.tracker.save()
        failed = [r for r in results if not r["passed"]]
        rec["details"] = {"checks": len(results), "status": status, "failed": failed}
        if status == "fail":
            raise RuntimeError(
                f"warehouse quality checks failed: {[f['rule_id'] for f in failed if f['severity'] == 'error']}"
            )
        return rec["details"]


def publish(ctx: RunContext) -> dict[str, Any]:
    with ctx.tracker.task("publish") as rec:
        from ..db import fetch_one

        hw: dict[str, str | None] = {}
        try:
            row = fetch_one(
                "SELECT (SELECT max(order_ts) FROM warehouse.fact_orders) AS o, (SELECT max(event_ts) FROM warehouse.fact_inventory) AS i",
                settings=ctx.settings,
            )
            hw = {
                "orders": str(row["o"]) if row and row["o"] else None,
                "inventory_events": str(row["i"]) if row and row["i"] else None,
            }
        except Exception:
            pass
        datasets = list(SOURCE_LAYOUT) + list(API_DATASETS)
        ctx.tracker.set_watermarks(datasets, ctx.batch_id, hw)
        rec["details"] = {"watermark_batch": ctx.batch_id, "high_watermarks": hw}
        return rec["details"]


TASK_FUNCTIONS = {
    "check_sources": check_sources,
    "init_warehouse": init_warehouse,
    "ingest_customers": ingest_customers,
    "ingest_catalog": ingest_catalog,
    "ingest_orders": ingest_orders,
    "ingest_inventory": ingest_inventory,
    "ingest_shipping": ingest_shipping,
    "ingest_reference": ingest_reference,
    "detect_schema_drift": detect_schema_drift,
    "silver_reference": silver_reference,
    "silver_customers": silver_customers,
    "silver_products": silver_products,
    "silver_orders": silver_orders,
    "silver_order_items": silver_order_items,
    "silver_payments": silver_payments,
    "silver_inventory": silver_inventory,
    "silver_shipping": silver_shipping,
    "gold_dimensions": gold_dimensions,
    "gold_facts": gold_facts,
    "load_dimensions": load_dimensions,
    "load_facts": load_facts,
    "dbt_run": dbt_run,
    "dbt_test": dbt_test,
    "warehouse_quality_checks": warehouse_quality_checks,
    "publish": publish,
}
