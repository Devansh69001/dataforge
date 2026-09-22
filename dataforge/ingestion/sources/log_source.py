"""Application-log reader for shipping tracker events (SOURCE 6).

Expected line shape:
    <iso-ts> <LEVEL> [shipping-tracker] key=value key="quoted value" ...

Lines that do not match the envelope, or that lack the required keys, are parse rejects.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .base import ReadResult, SourceError, records_to_string_table

ENVELOPE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)\s+(?P<level>[A-Z]+)\s+\[(?P<component>[\w-]+)\]\s+(?P<body>.*)$"
)
KV = re.compile(r'(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?:"(?P<quoted>[^"]*)"|(?P<bare>\S*))')

REQUIRED_KEYS = ("event", "shipment_id")
COLUMNS = [
    "event_ts",
    "level",
    "component",
    "event",
    "shipment_id",
    "order_id",
    "carrier",
    "warehouse_id",
    "region",
    "location",
]


def parse_line(line: str) -> dict[str, Any] | None:
    m = ENVELOPE.match(line)
    if not m:
        return None
    rec: dict[str, Any] = {
        "event_ts": m.group("ts"),
        "level": m.group("level"),
        "component": m.group("component"),
    }
    for kv in KV.finditer(m.group("body")):
        rec[kv.group("key")] = kv.group("quoted") if kv.group("quoted") is not None else kv.group("bare")
    return rec


class LogSource:
    format = "log"

    def read(self, path: Path) -> ReadResult:
        if not path.exists():
            raise SourceError(f"source file not found: {path}")
        good: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        with open(path, encoding="utf-8") as f:
            for line_no, raw in enumerate(f, start=1):
                line = raw.rstrip("\n")
                if not line or line.startswith("#"):
                    continue
                rec = parse_line(line)
                if rec is None:
                    rejected.append(
                        {"record": line, "line": line_no, "error": "line does not match log envelope"}
                    )
                    continue
                missing = [k for k in REQUIRED_KEYS if not rec.get(k)]
                if missing:
                    rejected.append(
                        {"record": line, "line": line_no, "error": f"missing required keys: {missing}"}
                    )
                    continue
                good.append(rec)
        extra = sorted({k for r in good for k in r} - set(COLUMNS))
        table = records_to_string_table(good, columns=COLUMNS + extra)
        return ReadResult(table=table, rejected=rejected, stats={"columns": table.column_names})
