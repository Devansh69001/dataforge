"""Quarantine zone writer.

Bad records are never deleted. Each quarantined record is stored as one JSON line with
enough context for an engineer to investigate and, if appropriate, replay:

    {
      "record":      {...original record, exactly as delivered...},
      "dataset":     "order_items",
      "source":      "orders/order_items_2025-11-30.csv",
      "batch_id":    "2025-11-30",
      "stage":       "parse" | "validate",
      "rule_ids":    ["order_items.quantity_positive"],
      "errors":      ["quantity must be greater than zero"],
      "detected_at": "2026-01-05T10:22:13.120Z",
      "run_id":      "run_20260105_102200"
    }

Layout: quarantine/<dataset>/batch=<batch_id>/<stage>_<run_id>.ndjson
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..config import get_settings


def quarantine_path(dataset: str, batch_id: str, stage: str, run_id: str, root: Path | None = None) -> Path:
    root = root or get_settings().quarantine_dir
    return root / dataset / f"batch={batch_id}" / f"{stage}_{run_id}.ndjson"


def write_quarantine(
    dataset: str,
    batch_id: str,
    stage: str,
    run_id: str,
    source: str,
    records: Iterable[dict[str, Any]],
    root: Path | None = None,
) -> dict[str, Any]:
    """`records` items must contain at least `record` and `errors` (list[str]);
    `rule_ids` is optional. Returns a summary with counts per rule."""
    path = quarantine_path(dataset, batch_id, stage, run_id, root)
    path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    n = 0
    per_rule: Counter[str] = Counter()
    with open(path, "w", encoding="utf-8") as f:
        for item in records:
            errors = item.get("errors") or ([item["error"]] if item.get("error") else [])
            rule_ids = item.get("rule_ids") or []
            doc = {
                "record": item.get("record"),
                "dataset": dataset,
                "source": source,
                "batch_id": batch_id,
                "stage": stage,
                "rule_ids": rule_ids,
                "errors": errors,
                "detected_at": now,
                "run_id": run_id,
            }
            if "line" in item:
                doc["line"] = item["line"]
            f.write(json.dumps(doc, default=str) + "\n")
            n += 1
            for rid in rule_ids or ["parse"]:
                per_rule[rid] += 1
    if n == 0:
        path.unlink(missing_ok=True)
    return {"path": str(path) if n else None, "count": n, "per_rule": dict(per_rule)}


def read_quarantine(
    dataset: str, root: Path | None = None, batch_id: str | None = None
) -> list[dict[str, Any]]:
    root = root or get_settings().quarantine_dir
    base = root / dataset
    if not base.exists():
        return []
    out = []
    for p in sorted(base.rglob("*.ndjson")):
        if batch_id and f"batch={batch_id}" not in p.parts:
            continue
        with open(p, encoding="utf-8") as f:
            out.extend(json.loads(line) for line in f if line.strip())
    return out
