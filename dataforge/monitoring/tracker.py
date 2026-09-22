"""Pipeline telemetry: run / task / quality / schema events.

Every event is written twice:
  * a JSON document per run under data/monitoring/runs/<run_id>.json (always works, even
    when the warehouse is down - useful for post-mortems)
  * rows in monitoring.* in PostgreSQL when it is reachable (what the API and dashboard read)

The tracker is also the shared state between tasks (`state`), persisted to the JSON file so
that Airflow tasks running in separate processes see what earlier tasks produced.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from ..config import Settings, get_settings
from ..db import DatabaseUnavailable, transaction
from ..logging_utils import get_logger, set_log_context

log = get_logger("monitoring")


def new_run_id(prefix: str = "run") -> str:
    return f"{prefix}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"


def _now() -> datetime:
    return datetime.now(timezone.utc)


class RunTracker:
    def __init__(
        self,
        run_id: str,
        mode: str,
        batch_id: str | None,
        settings: Settings | None = None,
        triggered_by: str = "cli",
    ):
        self.settings = settings or get_settings()
        self.run_id = run_id
        self.mode = mode
        self.batch_id = batch_id
        self.triggered_by = triggered_by
        self.path = self.settings.monitoring_dir / "runs" / f"{run_id}.json"
        self.doc: dict[str, Any] = {}
        if self.path.exists():
            self.doc = json.loads(self.path.read_text(encoding="utf-8"))
        else:
            self.doc = {
                "run_id": run_id,
                "mode": mode,
                "batch_id": batch_id,
                "status": "running",
                "triggered_by": triggered_by,
                "started_at": _now().isoformat(),
                "finished_at": None,
                "tasks": [],
                "quality": [],
                "schema_events": [],
                "totals": {"rows_ingested": 0, "rows_rejected": 0, "rows_quarantined": 0, "rows_loaded": 0},
                "state": {},
            }
        self.db_ok = True
        set_log_context(run_id=run_id)

    # ------------------------------------------------------------- persistence
    @property
    def state(self) -> dict[str, Any]:
        return self.doc["state"]

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.path.parent, suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(self.doc, f, indent=1, default=str)
        os.replace(tmp, self.path)

    @classmethod
    def load(cls, run_id: str, settings: Settings | None = None) -> RunTracker:
        settings = settings or get_settings()
        path = settings.monitoring_dir / "runs" / f"{run_id}.json"
        if not path.exists():
            raise FileNotFoundError(f"no run document for {run_id}")
        doc = json.loads(path.read_text(encoding="utf-8"))
        t = cls(run_id, doc["mode"], doc.get("batch_id"), settings, doc.get("triggered_by", "cli"))
        return t

    def _db(self, fn) -> None:
        if not self.db_ok:
            return
        try:
            with transaction(self.settings) as conn:
                fn(conn)
        except DatabaseUnavailable as e:
            self.db_ok = False
            log.warning("monitoring database unavailable; continuing with file telemetry only", error=str(e))
        except Exception as e:  # e.g. monitoring tables not created yet
            log.warning("monitoring write failed", error=str(e)[:200])

    # ------------------------------------------------------------------- run
    def start(self) -> None:
        self.save()
        self._db(
            lambda c: c.execute(
                "INSERT INTO monitoring.pipeline_runs (run_id, mode, batch_id, status, started_at, triggered_by) VALUES (%s,%s,%s,'running',%s,%s) "
                "ON CONFLICT (run_id) DO UPDATE SET status='running', mode=EXCLUDED.mode, batch_id=EXCLUDED.batch_id",
                (self.run_id, self.mode, self.batch_id, self.doc["started_at"], self.triggered_by),
            )
        )
        log.info("pipeline run started", mode=self.mode, batch_id=self.batch_id)

    def finish(self, status: str, error: str | None = None, quality_status: str | None = None) -> None:
        self.doc["status"] = status
        self.doc["finished_at"] = _now().isoformat()
        started = datetime.fromisoformat(self.doc["started_at"])
        self.doc["duration_seconds"] = round((_now() - started).total_seconds(), 2)
        self.doc["error"] = error
        self.doc["quality_status"] = quality_status or self.doc.get("quality_status")
        self.save()
        t = self.doc["totals"]
        self._db(
            lambda c: c.execute(
                "UPDATE monitoring.pipeline_runs SET status=%s, finished_at=%s, duration_seconds=%s, rows_ingested=%s, rows_rejected=%s, "
                "rows_quarantined=%s, rows_loaded=%s, quality_status=%s, error=%s, details=%s WHERE run_id=%s",
                (
                    status,
                    self.doc["finished_at"],
                    self.doc["duration_seconds"],
                    t["rows_ingested"],
                    t["rows_rejected"],
                    t["rows_quarantined"],
                    t["rows_loaded"],
                    self.doc["quality_status"],
                    error,
                    json.dumps(self.doc["state"], default=str),
                    self.run_id,
                ),
            )
        )
        log.info("pipeline run finished", status=status, seconds=self.doc["duration_seconds"], **t)

    # ------------------------------------------------------------------ tasks
    @contextmanager
    def task(self, name: str):
        """Context manager recording a task's status, duration and row counts.
        Yields a dict the task may fill with rows_in / rows_out / rows_rejected / details."""
        set_log_context(task_name=name)
        rec: dict[str, Any] = {
            "task_name": name,
            "status": "running",
            "started_at": _now().isoformat(),
            "finished_at": None,
            "duration_seconds": None,
            "rows_in": None,
            "rows_out": None,
            "rows_rejected": None,
            "error": None,
            "details": {},
        }
        self.doc["tasks"] = [t for t in self.doc["tasks"] if t["task_name"] != name] + [rec]
        self.save()
        t0 = time.time()
        log.info("task started")
        try:
            yield rec
            rec["status"] = "success"
        except Exception as e:
            rec["status"] = "failed"
            rec["error"] = f"{type(e).__name__}: {e}"
            rec["details"]["traceback"] = traceback.format_exc()[-4000:]
            log.error("task failed", error=rec["error"])
            raise
        finally:
            rec["finished_at"] = _now().isoformat()
            rec["duration_seconds"] = round(time.time() - t0, 2)
            self.save()
            self._db(
                lambda c: c.execute(
                    "INSERT INTO monitoring.task_runs (run_id, task_name, status, started_at, finished_at, duration_seconds, rows_in, rows_out, rows_rejected, error, details) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        self.run_id,
                        name,
                        rec["status"],
                        rec["started_at"],
                        rec["finished_at"],
                        rec["duration_seconds"],
                        rec["rows_in"],
                        rec["rows_out"],
                        rec["rows_rejected"],
                        rec["error"],
                        json.dumps(
                            {k: v for k, v in rec["details"].items() if k != "traceback"}, default=str
                        ),
                    ),
                )
            )
            log.info("task finished", status=rec["status"], seconds=rec["duration_seconds"])
            set_log_context(task_name="")

    def add_totals(self, **counts: int) -> None:
        for k, v in counts.items():
            self.doc["totals"][k] = self.doc["totals"].get(k, 0) + int(v or 0)

    # -------------------------------------------------------------- quality
    def record_quality(self, stage: str, dataset: str, results: list[dict[str, Any]]) -> None:
        """results items: rule_id, severity, total_rows, failed_rows, failure_rate, threshold, passed, details?"""
        for r in results:
            self.doc["quality"].append({"stage": stage, "dataset": dataset, **r})
        self.save()

        def _write(conn):
            for r in results:
                conn.execute(
                    "INSERT INTO monitoring.quality_results (run_id, stage, dataset, rule_id, severity, total_rows, failed_rows, failure_rate, threshold, passed, details) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        self.run_id,
                        stage,
                        dataset,
                        r["rule_id"],
                        r["severity"],
                        r.get("total_rows"),
                        r.get("failed_rows"),
                        r.get("failure_rate"),
                        r.get("threshold"),
                        r["passed"],
                        json.dumps(r.get("details", {}), default=str),
                    ),
                )

        self._db(_write)

    def record_schema_events(self, events: list[dict[str, Any]]) -> None:
        if not events:
            return
        self.doc["schema_events"].extend(events)
        self.save()

        def _write(conn):
            for e in events:
                conn.execute(
                    "INSERT INTO monitoring.schema_events (run_id, dataset, change_type, column_name, severity, batch_id, source_file, detected_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        self.run_id,
                        e["dataset"],
                        e["change_type"],
                        e["column"],
                        e["severity"],
                        e.get("batch_id"),
                        e.get("source_file"),
                        e.get("detected_at"),
                    ),
                )

        self._db(_write)

    def record_ingestion(self, entries: list[dict[str, Any]]) -> None:
        def _write(conn):
            for e in entries:
                conn.execute(
                    "INSERT INTO monitoring.ingestion_batches (dataset, file_hash, source_file, batch_id, rows, rejected, bronze_path, run_id, status, ingested_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (dataset, file_hash) DO NOTHING",
                    (
                        e["dataset"],
                        e["file_hash"],
                        e["source_file"],
                        e["batch_id"],
                        e["rows"],
                        e["rejected"],
                        e["bronze_path"],
                        e["run_id"],
                        e["status"],
                        e["ingested_at"],
                    ),
                )

        self._db(_write)

    def set_watermarks(
        self, datasets: list[str], batch_id: str, high_watermark: dict[str, str | None]
    ) -> None:
        self.doc["state"]["watermarks"] = {
            d: {"last_batch_id": batch_id, "high_watermark_ts": high_watermark.get(d)} for d in datasets
        }
        self.save()

        def _write(conn):
            for d in datasets:
                conn.execute(
                    "INSERT INTO monitoring.watermarks (dataset, last_batch_id, high_watermark_ts, last_run_id, updated_at) VALUES (%s,%s,%s,%s,now()) "
                    "ON CONFLICT (dataset) DO UPDATE SET last_batch_id=EXCLUDED.last_batch_id, high_watermark_ts=EXCLUDED.high_watermark_ts, last_run_id=EXCLUDED.last_run_id, updated_at=now()",
                    (d, batch_id, high_watermark.get(d), self.run_id),
                )

        self._db(_write)


def read_watermarks(settings: Settings | None = None) -> dict[str, dict[str, Any]]:
    settings = settings or get_settings()
    try:
        with transaction(settings) as conn:
            rows = conn.execute(
                "SELECT dataset, last_batch_id, high_watermark_ts, last_run_id, updated_at FROM monitoring.watermarks"
            ).fetchall()
        return {r["dataset"]: dict(r) for r in rows}
    except Exception:
        return {}


def list_runs(settings: Settings | None = None, limit: int = 20) -> list[dict[str, Any]]:
    """Runs from the file zone (works without a database), newest first."""
    settings = settings or get_settings()
    d = settings.monitoring_dir / "runs"
    if not d.exists():
        return []
    docs = []
    for p in sorted(d.glob("*.json"), reverse=True)[:limit]:
        try:
            docs.append(json.loads(p.read_text(encoding="utf-8")))
        except json.JSONDecodeError:
            continue
    return docs
