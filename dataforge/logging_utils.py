"""Structured logging shared by every layer.

Every log line carries the pipeline run_id / task name when available so logs can be
correlated with rows in `monitoring.task_runs`. Set DATAFORGE_LOG_FORMAT=json for
machine-readable output (used in Docker/Airflow).
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

_run_id: ContextVar[str | None] = ContextVar("run_id", default=None)
_task_name: ContextVar[str | None] = ContextVar("task_name", default=None)


def set_log_context(run_id: str | None = None, task_name: str | None = None) -> None:
    if run_id is not None:
        _run_id.set(run_id)
    if task_name is not None:
        _task_name.set(task_name)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if _run_id.get():
            payload["run_id"] = _run_id.get()
        if _task_name.get():
            payload["task"] = _task_name.get()
        extra = getattr(record, "extra_fields", None)
        if extra:
            payload.update(extra)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class PlainFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ctx = f"[{_task_name.get()}] " if _task_name.get() else ""
        base = f"{time.strftime('%H:%M:%S')} {record.levelname:<7} {ctx}{record.getMessage()}"
        extra = getattr(record, "extra_fields", None)
        if extra:
            base += " " + json.dumps(extra, default=str)
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base


class _ExtraAdapter(logging.LoggerAdapter):
    """Allows logger.info("msg", rows=10) -> structured extra_fields."""

    def process(self, msg, kwargs):
        reserved = {"exc_info", "stack_info", "stacklevel"}
        fields = {k: kwargs.pop(k) for k in list(kwargs) if k not in reserved}
        kwargs["extra"] = {"extra_fields": fields} if fields else {}
        return msg, kwargs


_configured = False


def configure_logging(level: str | None = None) -> None:
    global _configured
    if _configured:
        return
    level = level or os.environ.get("DATAFORGE_LOG_LEVEL", "INFO")
    fmt = os.environ.get("DATAFORGE_LOG_FORMAT", "plain")
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter() if fmt == "json" else PlainFormatter())
    root = logging.getLogger("dataforge")
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    root.propagate = False
    _configured = True


def get_logger(name: str) -> _ExtraAdapter:
    configure_logging()
    return _ExtraAdapter(logging.getLogger(f"dataforge.{name}"), {})
