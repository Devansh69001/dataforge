"""Bronze landing: raw sources -> immutable, batch-partitioned parquet.

Bronze is "as delivered": every column is a string, nothing is cleaned, nothing is dropped
(except lines that cannot be parsed at all, which go to quarantine with stage=parse).
Each row carries lineage metadata:

    _batch_id      delivery batch (also the directory partition `batch=<id>`)
    _source_file   path of the delivered file relative to the raw zone
    _ingested_at   UTC ingestion timestamp
    _run_id        pipeline run that landed it
    _row_number    1-based position in the source file (for tracing back to the line)

Layout: bronze/<dataset>/batch=<batch_id>/<dataset>-<sha256[:12]>.parquet
The file name is derived from the content hash, so re-ingesting the same file overwrites
the same object instead of creating a duplicate (idempotent landing).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ..config import Settings, get_settings
from ..logging_utils import get_logger
from ..quality.quarantine import write_quarantine
from .ledger import IngestionLedger, LedgerEntry
from .schemas import REGISTRY, SchemaEvent, detect_drift, raise_on_breaking
from .sources.api_source import ReferenceApiClient
from .sources.base import ReadResult, SourceError, file_sha256
from .sources.csv_source import CsvSource
from .sources.json_source import JsonDocumentSource, NdjsonSource
from .sources.log_source import LogSource

log = get_logger("ingestion.bronze")

# dataset -> (raw sub-directory, file name pattern, reader factory)
SOURCE_LAYOUT: dict[str, tuple[str, str, Any]] = {
    "customers": ("customers", "customers_{batch}.csv", lambda: CsvSource()),
    "products": ("products", "product_catalog_{batch}.json", lambda: JsonDocumentSource("products")),
    "categories": ("products", "product_catalog_{batch}.json", lambda: JsonDocumentSource("categories")),
    "suppliers": ("products", "product_catalog_{batch}.json", lambda: JsonDocumentSource("suppliers")),
    "orders": ("orders", "orders_{batch}.csv", lambda: CsvSource()),
    "order_items": ("orders", "order_items_{batch}.csv", lambda: CsvSource()),
    "payments": ("orders", "payments_{batch}.csv", lambda: CsvSource()),
    "inventory_events": ("inventory", "inventory_events_{batch}.ndjson", lambda: NdjsonSource()),
    "shipping_events": ("shipping", "shipping_events_{batch}.log", lambda: LogSource()),
}
API_DATASETS = ("regions", "warehouses", "exchange_rates")


@dataclass
class IngestResult:
    dataset: str
    batch_id: str
    source_file: str
    rows: int = 0
    rejected: int = 0
    skipped: bool = False
    bronze_path: str | None = None
    file_hash: str | None = None
    schema_events: list[SchemaEvent] = field(default_factory=list)
    quarantine: dict[str, Any] = field(default_factory=dict)
    seconds: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        d = self.__dict__.copy()
        d["schema_events"] = [e.as_dict() for e in self.schema_events]
        return d


def source_path(dataset: str, batch_id: str, raw_dir: Path) -> Path:
    sub, pattern, _ = SOURCE_LAYOUT[dataset]
    return raw_dir / sub / pattern.format(batch=batch_id)


def _with_metadata(table: pa.Table, batch_id: str, source_file: str, run_id: str) -> pa.Table:
    n = table.num_rows
    now = datetime.now(timezone.utc)
    table = table.append_column("_batch_id", pa.array([batch_id] * n, pa.string()))
    table = table.append_column("_source_file", pa.array([source_file] * n, pa.string()))
    table = table.append_column("_ingested_at", pa.array([now] * n, pa.timestamp("us", tz="UTC")))
    table = table.append_column("_run_id", pa.array([run_id] * n, pa.string()))
    table = table.append_column("_row_number", pa.array(range(1, n + 1), pa.int64()))
    return table


def bronze_dir(dataset: str, settings: Settings | None = None) -> Path:
    return (settings or get_settings()).bronze_dir / dataset


def land(
    dataset: str,
    result: ReadResult,
    batch_id: str,
    source_file: str,
    file_hash: str,
    run_id: str,
    settings: Settings | None = None,
) -> tuple[Path, list[SchemaEvent]]:
    """Write a ReadResult to bronze after schema-drift checks. Returns (path, events)."""
    settings = settings or get_settings()
    events = detect_drift(dataset, result.table.column_names, batch_id, source_file)
    for e in events:
        getattr(log, e.severity.lower() if e.severity != "ERROR" else "error")(
            "schema drift detected", dataset=dataset, change=e.change_type, column=e.column, batch_id=batch_id
        )
    raise_on_breaking(events)
    table = _with_metadata(result.table, batch_id, source_file, run_id)
    out_dir = bronze_dir(dataset, settings) / f"batch={batch_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{dataset}-{file_hash[:12]}.parquet"
    pq.write_table(table, path, compression="snappy")
    return path, events


def ingest_file(
    dataset: str, batch_id: str, run_id: str, force: bool = False, settings: Settings | None = None
) -> IngestResult:
    settings = settings or get_settings()
    t0 = datetime.now(timezone.utc)
    path = source_path(dataset, batch_id, settings.raw_dir)
    rel = path.relative_to(settings.raw_dir).as_posix() if path.exists() else path.name
    res = IngestResult(dataset=dataset, batch_id=batch_id, source_file=rel)
    if not path.exists():
        raise SourceError(f"{dataset}: expected source file missing for batch {batch_id}: {path}")

    ledger = IngestionLedger(settings.bronze_dir / "_ledger.json")
    file_hash = file_sha256(path)
    res.file_hash = file_hash
    if ledger.already_ingested(dataset, file_hash) and not force:
        prev = ledger.get(dataset, file_hash)
        res.skipped = True
        res.bronze_path = prev["bronze_path"]
        res.rows = prev["rows"]
        log.info(
            "file already ingested, skipping (duplicate delivery)",
            dataset=dataset,
            file=rel,
            hash=file_hash[:12],
        )
        return res

    reader = SOURCE_LAYOUT[dataset][2]()
    read = reader.read(path)
    out, events = land(dataset, read, batch_id, rel, file_hash, run_id, settings)
    res.rows = read.rows
    res.rejected = len(read.rejected)
    res.bronze_path = str(out)
    res.schema_events = events
    if read.rejected:
        res.quarantine = write_quarantine(
            dataset, batch_id, "parse", run_id, rel, read.rejected, settings.quarantine_dir
        )
    ledger.record(
        LedgerEntry(
            dataset=dataset,
            source_file=rel,
            file_hash=file_hash,
            batch_id=batch_id,
            rows=read.rows,
            rejected=res.rejected,
            bronze_path=str(out),
            run_id=run_id,
        )
    )
    res.seconds = (datetime.now(timezone.utc) - t0).total_seconds()
    log.info(
        "landed in bronze",
        dataset=dataset,
        file=rel,
        rows=res.rows,
        parse_rejects=res.rejected,
        seconds=round(res.seconds, 2),
    )
    return res


def ingest_reference(
    dataset: str, batch_id: str, run_id: str, settings: Settings | None = None
) -> IngestResult:
    """Pull a reference resource from the API and land it in bronze."""
    settings = settings or get_settings()
    t0 = datetime.now(timezone.utc)
    client = ReferenceApiClient(settings.reference_api_url)
    as_of = batch_id if REGISTRY[dataset].dataset == "exchange_rates" else None
    read = client.fetch(dataset, as_of=as_of)
    src = f"api:{dataset}" + (f"?as_of={as_of}" if as_of else "")
    # API payloads have no file to hash; hash the content so identical responses dedupe
    import hashlib

    content_hash = hashlib.sha256(read.table.to_pandas().to_csv(index=False).encode()).hexdigest()
    ledger = IngestionLedger(settings.bronze_dir / "_ledger.json")
    res = IngestResult(dataset=dataset, batch_id=batch_id, source_file=src, file_hash=content_hash)
    if ledger.already_ingested(dataset, content_hash):
        prev = ledger.get(dataset, content_hash)
        res.skipped, res.bronze_path, res.rows = True, prev["bronze_path"], prev["rows"]
        return res
    out, events = land(dataset, read, batch_id, src, content_hash, run_id, settings)
    res.rows, res.bronze_path, res.schema_events = read.rows, str(out), events
    ledger.record(
        LedgerEntry(
            dataset=dataset,
            source_file=src,
            file_hash=content_hash,
            batch_id=batch_id,
            rows=read.rows,
            rejected=0,
            bronze_path=str(out),
            run_id=run_id,
        )
    )
    res.seconds = (datetime.now(timezone.utc) - t0).total_seconds()
    log.info(
        "landed reference data in bronze",
        dataset=dataset,
        rows=res.rows,
        transport=read.stats.get("transport"),
    )
    return res


def ingest_batch(
    batch_id: str,
    run_id: str,
    datasets: list[str] | None = None,
    force: bool = False,
    settings: Settings | None = None,
) -> dict[str, IngestResult]:
    settings = settings or get_settings()
    datasets = datasets or list(SOURCE_LAYOUT) + list(API_DATASETS)
    results: dict[str, IngestResult] = {}
    for ds in datasets:
        if ds in API_DATASETS:
            results[ds] = ingest_reference(ds, batch_id, run_id, settings)
        else:
            results[ds] = ingest_file(ds, batch_id, run_id, force=force, settings=settings)
    return results


def bronze_files(
    dataset: str, batch_ids: list[str] | None = None, settings: Settings | None = None
) -> list[Path]:
    """All bronze parquet files for a dataset, optionally restricted to batches."""
    base = bronze_dir(dataset, settings)
    if not base.exists():
        return []
    files = []
    for part in sorted(base.glob("batch=*")):
        bid = part.name.split("=", 1)[1]
        if batch_ids and bid not in batch_ids:
            continue
        files.extend(sorted(part.glob("*.parquet")))
    return files
