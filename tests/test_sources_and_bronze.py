"""Source readers, schema registry, ledger, bronze landing and parse-level quarantine."""

from __future__ import annotations

import json

import httpx
import pyarrow.parquet as pq
import pytest

from dataforge.ingestion.bronze import bronze_files, ingest_file
from dataforge.ingestion.ledger import IngestionLedger
from dataforge.ingestion.schemas import SchemaDriftError, detect_drift, raise_on_breaking
from dataforge.ingestion.sources.api_source import ReferenceApiClient
from dataforge.ingestion.sources.base import SourceError
from dataforge.ingestion.sources.csv_source import CsvSource
from dataforge.ingestion.sources.json_source import JsonDocumentSource, NdjsonSource
from dataforge.ingestion.sources.log_source import LogSource, parse_line
from dataforge.quality.quarantine import read_quarantine


# ------------------------------------------------------------------ readers
def test_csv_reader_keeps_everything_as_string(tmp_path):
    p = tmp_path / "x.csv"
    p.write_text("id,qty,price\n1,3,9.99\n2,,\n", encoding="utf-8")
    r = CsvSource().read(p)
    assert r.rows == 2
    assert all(str(t) == "string" for t in r.table.schema.types)
    assert r.table.column("qty").to_pylist() == ["3", None]


def test_csv_reader_missing_file():
    with pytest.raises(SourceError):
        CsvSource().read(__import__("pathlib").Path("does/not/exist.csv"))


def test_ndjson_reader_rejects_malformed_lines(tmp_path):
    p = tmp_path / "ev.ndjson"
    p.write_text(
        '{"event_id":"1","qty":2}\nnot json\n{"event_id":"2"\n{"event_id":"3","nested":{"a":1}}\n',
        encoding="utf-8",
    )
    r = NdjsonSource().read(p)
    assert r.rows == 2 and len(r.rejected) == 2
    assert r.rejected[0]["line"] == 2
    assert json.loads(r.table.column("nested").to_pylist()[1]) == {"a": 1}


def test_json_document_reader(tmp_path):
    p = tmp_path / "cat.json"
    p.write_text(
        json.dumps({"schema_version": 2, "products": [{"product_id": "P1", "attrs": {"c": "x"}}, "junk"]}),
        encoding="utf-8",
    )
    r = JsonDocumentSource("products").read(p)
    assert r.rows == 1 and len(r.rejected) == 1 and r.stats["schema_version"] == 2
    with pytest.raises(SourceError):
        JsonDocumentSource("missing").read(p)


def test_log_parser():
    line = '2025-03-04T11:22:03Z INFO [shipping-tracker] event=in_transit shipment_id=SHP-0000001 order_id=ORD-0000001 carrier="NorthStar Logistics" region=EU-WEST'
    rec = parse_line(line)
    assert (
        rec["event"] == "in_transit"
        and rec["carrier"] == "NorthStar Logistics"
        and rec["event_ts"] == "2025-03-04T11:22:03Z"
    )
    assert parse_line("garbage line") is None


def test_log_reader_rejects_missing_required_keys(tmp_path):
    p = tmp_path / "s.log"
    p.write_text(
        "# header comment\n"
        "2025-03-04T11:22:03Z INFO [shipping-tracker] event=delivered shipment_id=SHP-0000001 order_id=ORD-0000001\n"
        "2025-03-04T11:22:03Z ERROR [shipping-tracker] tracker restarted\n"
        "truncated\n",
        encoding="utf-8",
    )
    r = LogSource().read(p)
    assert r.rows == 1 and len(r.rejected) == 2


# --------------------------------------------------------------- API client
def test_api_client_file_transport(lake):
    c = ReferenceApiClient(lake.reference_api_url)
    r = c.fetch("regions")
    assert r.rows == 12 and "region_code" in r.table.column_names
    fx = c.fetch("exchange_rates", as_of="2025-12-31")
    assert fx.rows > 0
    with pytest.raises(SourceError):
        c.fetch("exchange_rates")  # as_of required


def test_api_client_http_retries_then_succeeds(monkeypatch):
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503, json={"detail": "outage"})
        page = int(request.url.params.get("page", 1))
        return httpx.Response(
            200, json={"data": [{"region_code": f"R{page}"}], "total_pages": 2, "page": page}
        )

    real_client = httpx.Client

    def fake_client(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return real_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", fake_client)
    c = ReferenceApiClient("http://reference-api:8081", backoff=0.001)
    r = c.fetch("regions")
    assert r.rows == 2 and calls["n"] == 4  # 2 failures + 2 pages
    assert r.stats["transport"] == "http" and r.stats["pages"] == 2


def test_api_client_gives_up_after_max_retries(monkeypatch):
    def handler(request):
        return httpx.Response(503)

    real_client = httpx.Client
    monkeypatch.setattr(
        httpx, "Client", lambda *a, **k: real_client(*a, transport=httpx.MockTransport(handler), **k)
    )
    c = ReferenceApiClient("http://reference-api:8081", max_retries=2, backoff=0.001)
    with pytest.raises(SourceError):
        c.fetch("regions")


# -------------------------------------------------------------- schema drift
def test_schema_drift_classification():
    ev = detect_drift(
        "orders",
        [
            "order_id",
            "customer_id",
            "order_date",
            "status",
            "currency",
            "order_total",
            "updated_at",
            "promo_campaign",
        ],
    )
    kinds = {(e.change_type, e.column, e.severity) for e in ev}
    assert ("new_column", "promo_campaign", "INFO") in kinds
    assert ("missing_optional", "channel", "WARN") in kinds
    raise_on_breaking(ev)  # nothing breaking
    ev = detect_drift("orders", ["order_id"])
    with pytest.raises(SchemaDriftError):
        raise_on_breaking(ev)


# ------------------------------------------------------------------ bronze
@pytest.fixture
def fresh_lake(lake, tmp_path):
    """An untouched lake sharing the generated raw files (bronze/quarantine start empty)."""
    import shutil

    from dataforge.config import Settings

    shutil.copytree(lake.raw_dir, tmp_path / "raw")
    return Settings(
        dataforge_data_dir=tmp_path, reference_api_url=f"file://{(tmp_path / 'raw' / 'api').as_posix()}"
    )


def test_bronze_landing_metadata_and_ledger(fresh_lake):
    lake = fresh_lake
    res = ingest_file("customers", "2025-11-30", "run_a", settings=lake)
    assert res.rows > 0 and not res.skipped
    tbl = pq.read_table(res.bronze_path)
    for c in ("_batch_id", "_source_file", "_ingested_at", "_run_id", "_row_number"):
        assert c in tbl.column_names
    assert tbl.column("_row_number").to_pylist()[:3] == [1, 2, 3]
    # identical delivery is skipped
    again = ingest_file("customers", "2025-11-30", "run_b", settings=lake)
    assert again.skipped and again.bronze_path == res.bronze_path
    forced = ingest_file("customers", "2025-11-30", "run_c", force=True, settings=lake)
    assert not forced.skipped and forced.bronze_path == res.bronze_path  # same content hash -> same object
    assert len(bronze_files("customers", ["2025-11-30"], lake)) == 1
    ledger = IngestionLedger(lake.bronze_dir / "_ledger.json")
    assert ledger.already_ingested("customers", res.file_hash)


def test_bronze_parse_rejects_are_quarantined(fresh_lake):
    lake = fresh_lake
    res = ingest_file("inventory_events", "2025-11-30", "run_q", settings=lake)
    manifest = json.loads((lake.raw_dir / "_manifest" / "2025-11-30.json").read_text())
    expected = manifest["datasets"]["inventory_events"]["defects"]["malformed_json_line"]
    assert res.rejected == expected
    recs = read_quarantine("inventory_events", lake.quarantine_dir, "2025-11-30")
    assert len(recs) >= expected
    r = recs[0]
    assert (
        r["stage"] == "parse"
        and r["dataset"] == "inventory_events"
        and r["errors"]
        and r["detected_at"]
        and r["source"]
    )


def test_missing_source_fails_clearly(lake):
    with pytest.raises(SourceError):
        ingest_file("orders", "1999-01-01", "run_x", settings=lake)
