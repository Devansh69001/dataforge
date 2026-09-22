"""Sequential pipeline runner (the same graph Airflow executes in Docker).

python -m dataforge.pipeline.runner --batch 2025-11-30              # mode inferred from watermarks
python -m dataforge.pipeline.runner --batch 2025-12-31 --mode incremental
python -m dataforge.pipeline.runner --batch 2025-12-31 --tasks silver_orders,gold_facts
python -m dataforge.pipeline.runner --batch 2025-12-31 --from-task load_dimensions --run-id run_x  # resume
"""

from __future__ import annotations

import argparse
import sys
import time

from ..config import get_settings
from ..logging_utils import get_logger
from ..monitoring.tracker import new_run_id
from .spec import PIPELINE, TASK_BY_NAME, topological_order, validate_spec
from .tasks import TASK_FUNCTIONS, make_context

log = get_logger("pipeline.runner")


def run_pipeline(
    batch_id: str,
    mode: str | None = None,
    run_id: str | None = None,
    tasks: list[str] | None = None,
    from_task: str | None = None,
    force_ingest: bool = False,
    triggered_by: str = "cli",
    stop_spark_on_exit: bool = True,
) -> dict:
    validate_spec()
    settings = get_settings()
    run_id = run_id or new_run_id()
    ctx = make_context(run_id, batch_id, mode, settings, triggered_by, force_ingest)
    order = topological_order()
    if tasks:
        unknown = [t for t in tasks if t not in TASK_BY_NAME]
        if unknown:
            raise SystemExit(f"unknown tasks: {unknown}")
        order = [t for t in order if t in tasks]
    if from_task:
        order = order[order.index(from_task) :]

    ctx.tracker.start()
    t0 = time.time()
    status, error = "success", None
    try:
        for name in order:
            TASK_FUNCTIONS[name](ctx)
    except Exception as e:  # the failing task already recorded its error
        status, error = "failed", f"{type(e).__name__}: {e}"
        log.error("pipeline aborted", error=error)
    finally:
        if stop_spark_on_exit:
            try:
                from ..spark.session import stop_spark

                stop_spark()
            except Exception:
                pass
        ctx.tracker.finish(status, error)
    return {
        "run_id": run_id,
        "mode": ctx.mode,
        "batch_id": batch_id,
        "status": status,
        "error": error,
        "seconds": round(time.time() - t0, 1),
        "totals": ctx.tracker.doc["totals"],
        "quality_status": ctx.tracker.doc.get("quality_status"),
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Run the DataForge pipeline for one batch")
    p.add_argument("--batch", required=True, help="batch id (YYYY-MM-DD cutoff of the raw delivery)")
    p.add_argument("--mode", choices=["initial", "incremental", "rerun"], default=None)
    p.add_argument("--run-id", default=None)
    p.add_argument("--tasks", default=None, help="comma-separated subset of tasks")
    p.add_argument("--from-task", default=None, help="resume from this task (requires --run-id)")
    p.add_argument("--force-ingest", action="store_true", help="re-land files even if already in the ledger")
    p.add_argument("--list", action="store_true", help="print the task graph and exit")
    a = p.parse_args(argv)
    if a.list:
        for t in PIPELINE:
            print(f"{t.name:28s} <- {', '.join(t.upstream) or '-'}")
        return 0
    result = run_pipeline(
        a.batch, a.mode, a.run_id, a.tasks.split(",") if a.tasks else None, a.from_task, a.force_ingest
    )
    print(result)
    return 0 if result["status"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
