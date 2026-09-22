"""Common contracts for source readers.

Every reader turns one delivered file (or API response) into a `ReadResult`:

* `table`     - a pyarrow Table where EVERY column is a string (bronze is schema-on-read;
                keeping the raw text means nothing is lost or silently coerced)
* `rejected`  - records that could not even be parsed (malformed JSON / log lines).
                These are quarantined at ingestion time with stage="parse".
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import pyarrow as pa


class SourceError(Exception):
    """Raised when a source is missing, unreadable, or structurally invalid."""


@dataclass
class ReadResult:
    table: pa.Table
    rejected: list[dict[str, Any]] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)

    @property
    def rows(self) -> int:
        return self.table.num_rows


class SourceReader(Protocol):
    format: str

    def read(self, path: Path) -> ReadResult: ...


def file_sha256(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def records_to_string_table(records: list[dict[str, Any]], columns: list[str] | None = None) -> pa.Table:
    """Build an all-string Arrow table from dicts; nested values are JSON-encoded."""
    import json

    if columns is None:
        seen: dict[str, None] = {}
        for r in records:
            for k in r:
                seen.setdefault(k, None)
        columns = list(seen)
    arrays = []
    for c in columns:
        vals = []
        for r in records:
            v = r.get(c)
            if v is None:
                vals.append(None)
            elif isinstance(v, (dict, list)):
                vals.append(json.dumps(v, separators=(",", ":"), default=str))
            elif isinstance(v, bool):
                vals.append("true" if v else "false")
            else:
                vals.append(str(v))
        arrays.append(pa.array(vals, type=pa.string()))
    return pa.table(arrays, names=columns) if columns else pa.table({})
