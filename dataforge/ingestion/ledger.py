"""Ingestion ledger: which source files have already landed in bronze.

Keyed by (dataset, sha256 of the file). Re-delivering a byte-identical file is a no-op
(duplicate ingestion protection); a changed file with the same name is a new entry.
The ledger lives in the lake (`bronze/_ledger.json`) so it works without a database, and
is mirrored into `monitoring.ingestion_batches` when PostgreSQL is available.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class LedgerEntry:
    dataset: str
    source_file: str
    file_hash: str
    batch_id: str
    rows: int
    rejected: int
    bronze_path: str
    run_id: str
    status: str = "success"
    ingested_at: str = ""

    def __post_init__(self):
        if not self.ingested_at:
            self.ingested_at = datetime.now(timezone.utc).isoformat()


class IngestionLedger:
    def __init__(self, path: Path):
        self.path = path
        self._entries: dict[str, dict[str, Any]] = {}
        if path.exists():
            with open(path, encoding="utf-8") as f:
                self._entries = json.load(f)

    @staticmethod
    def key(dataset: str, file_hash: str) -> str:
        return f"{dataset}:{file_hash}"

    def get(self, dataset: str, file_hash: str) -> dict[str, Any] | None:
        return self._entries.get(self.key(dataset, file_hash))

    def already_ingested(self, dataset: str, file_hash: str) -> bool:
        e = self.get(dataset, file_hash)
        return bool(e and e.get("status") == "success")

    def record(self, entry: LedgerEntry) -> None:
        self._entries[self.key(entry.dataset, entry.file_hash)] = asdict(entry)
        self._flush()

    def entries(self, dataset: str | None = None) -> list[dict[str, Any]]:
        vals = list(self._entries.values())
        if dataset:
            vals = [v for v in vals if v["dataset"] == dataset]
        return sorted(vals, key=lambda v: v["ingested_at"])

    def _flush(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.path.parent, suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(self._entries, f, indent=1)
        os.replace(tmp, self.path)  # atomic on POSIX and Windows
