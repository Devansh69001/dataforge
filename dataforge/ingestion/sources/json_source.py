"""JSON readers.

* `JsonDocumentSource` - one JSON document holding named arrays (SOURCE 2 product catalog:
  `products`, `categories`, `suppliers`). Nested objects are kept as JSON text.
* `NdjsonSource`       - newline-delimited JSON events (SOURCE 4 inventory). Lines that do
  not parse are returned as parse rejects, never dropped silently.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .base import ReadResult, SourceError, records_to_string_table


class JsonDocumentSource:
    format = "json"

    def __init__(self, array_key: str):
        self.array_key = array_key

    def read(self, path: Path) -> ReadResult:
        if not path.exists():
            raise SourceError(f"source file not found: {path}")
        try:
            with open(path, encoding="utf-8") as f:
                doc = json.load(f)
        except json.JSONDecodeError as e:
            raise SourceError(f"malformed JSON document {path}: {e}") from e
        if not isinstance(doc, dict) or self.array_key not in doc:
            raise SourceError(f"{path} does not contain a top-level '{self.array_key}' array")
        records: list[dict[str, Any]] = doc[self.array_key]
        rejected = []
        good = []
        for i, rec in enumerate(records):
            if isinstance(rec, dict):
                good.append(rec)
            else:
                rejected.append({"record": rec, "line": i, "error": "record is not a JSON object"})
        table = records_to_string_table(good)
        stats = {
            "schema_version": doc.get("schema_version"),
            "generated_at": doc.get("generated_at"),
            "columns": table.column_names,
        }
        return ReadResult(table=table, rejected=rejected, stats=stats)


class NdjsonSource:
    format = "ndjson"

    def read(self, path: Path) -> ReadResult:
        if not path.exists():
            raise SourceError(f"source file not found: {path}")
        good: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        with open(path, encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                s = line.strip()
                if not s:
                    continue
                try:
                    rec = json.loads(s)
                except json.JSONDecodeError as e:
                    rejected.append(
                        {
                            "record": s,
                            "line": line_no,
                            "error": f"malformed JSON line: {e.msg} at col {e.colno}",
                        }
                    )
                    continue
                if not isinstance(rec, dict):
                    rejected.append({"record": s, "line": line_no, "error": "line is not a JSON object"})
                    continue
                good.append(rec)
        table = records_to_string_table(good)
        return ReadResult(
            table=table,
            rejected=rejected,
            stats={
                "lines": line_no if good or rejected else 0,
                "columns": table.column_names,
                "read_at": datetime.now(timezone.utc).isoformat(),
            },
        )
