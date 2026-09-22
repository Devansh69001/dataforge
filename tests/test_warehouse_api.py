"""PostgreSQL loader idempotency, warehouse quality checks, dbt marts and the API."""

from __future__ import annotations

import pytest

from dataforge.db import fetch_all, fetch_one
from dataforge.quality.warehouse_checks import run_warehouse_checks, summarize
from dataforge.warehouse.loader import TABLES, load_table

pytestmark = [pytest.mark.db, pytest.mark.spark]


def _count(table, db):
    return fetch_one(f"SELECT count(*) AS n FROM warehouse.{table}", settings=db)["n"]


def test_ddl_creates_model(db):
    tables = {
        r["table_name"]
        for r in fetch_all(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='warehouse'", settings=db
        )
    }
    assert set(TABLES) <= tables
    mon = {
        r["table_name"]
        for r in fetch_all(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='monitoring'", settings=db
        )
    }
    assert {
        "pipeline_runs",
        "task_runs",
        "quality_results",
        "schema_events",
        "watermarks",
        "ingestion_batches",
    } <= mon


def test_loader_is_idempotent(warehouse, db):
    before = {t: _count(t, db) for t in TABLES}
    assert before["fact_orders"] > 0 and before["dim_customer"] > 0
    again = load_table("fact_orders", None, db)
    assert again.rows_inserted == 0 and again.rows_updated == before["fact_orders"]
    after = {t: _count(t, db) for t in TABLES}
    assert after == before
    dup = fetch_one(
        "SELECT count(*) - count(DISTINCT order_id) AS d FROM warehouse.fact_orders", settings=db
    )["d"]
    assert dup == 0


def test_partitions_created_per_month(warehouse, db):
    parts = fetch_all(
        "SELECT inhrelid::regclass::text AS p FROM pg_inherits WHERE inhparent='warehouse.fact_orders'::regclass ORDER BY 1",
        settings=db,
    )
    names = [r["p"] for r in parts]
    assert len(names) >= 12 and all("fact_orders_20" in n for n in names)


def test_incremental_partition_load(warehouse, db):
    res = load_table("fact_orders", ["2025-11"], db)
    assert res.rows_read > 0 and res.rows_inserted == 0 and res.rows_updated == res.rows_read


def test_warehouse_quality_checks_pass(warehouse, db):
    results = run_warehouse_checks("2025-11-30", db)
    failed_errors = [r for r in results if not r["passed"] and r["severity"] == "error"]
    assert not failed_errors, failed_errors
    assert summarize(results) in ("pass", "warn")
    ids = {r["rule_id"] for r in results}
    assert {
        "wh.fact_orders.order_id_unique",
        "wh.fact_order_items.product_ri",
        "wh.fact_order_items.quantity_positive",
        "wh.fact_orders.covers_batch",
    } <= ids


def test_marts_answer_business_questions(marts, db):
    monthly = fetch_all(
        "SELECT year_month, revenue_usd FROM analytics_marts.monthly_revenue_summary ORDER BY 1", settings=db
    )
    assert len(monthly) >= 12 and all(r["revenue_usd"] >= 0 for r in monthly)
    cats = fetch_all(
        "SELECT category_name, sum(revenue_usd) r FROM analytics_marts.category_revenue GROUP BY 1 ORDER BY 2 DESC",
        settings=db,
    )
    assert cats and cats[0]["r"] > 0
    declining = fetch_one(
        "SELECT count(*) AS n FROM analytics_marts.product_sales_metrics WHERE is_declining", settings=db
    )["n"]
    assert declining >= 0
    ltv = fetch_one(
        "SELECT max(net_lifetime_value_usd) AS m FROM analytics_marts.customer_lifetime_value", settings=db
    )["m"]
    assert ltv > 0
    sup = fetch_all(
        "SELECT supplier_id, defect_rate FROM analytics_marts.supplier_performance WHERE defect_rate IS NOT NULL ORDER BY 2 DESC LIMIT 1",
        settings=db,
    )
    assert sup and 0 <= sup[0]["defect_rate"] <= 1
    ship = fetch_all(
        "SELECT region_code, avg(avg_delivery_days) d FROM analytics_marts.shipping_performance GROUP BY 1",
        settings=db,
    )
    assert ship and all(r["d"] is None or r["d"] >= 0 for r in ship)
    turn = fetch_one(
        "SELECT count(*) AS n FROM analytics_marts.inventory_turnover WHERE turnover_ratio_90d IS NOT NULL",
        settings=db,
    )["n"]
    assert turn > 0


def test_api_endpoints(marts, db):
    from fastapi.testclient import TestClient

    from api.main import app

    c = TestClient(app)
    r = c.get("/health")
    assert r.status_code == 200 and r.json()["database"] == "ok"
    r = c.get("/metrics/sales?granularity=monthly&limit=3")
    assert r.status_code == 200 and len(r.json()["rows"]) == 3 and r.json()["totals"]["revenue_usd"] > 0
    r = c.get("/metrics/sales?granularity=daily&from=2025-11-01&to=2025-11-07")
    assert r.status_code == 200 and 1 <= len(r.json()["rows"]) <= 7
    for path in (
        "/metrics/sales/categories",
        "/metrics/sales/products?declining=true",
        "/metrics/inventory",
        "/metrics/customers",
        "/metrics/shipping",
        "/metrics/suppliers",
        "/pipeline/status",
        "/pipeline/runs",
        "/pipeline/quality",
    ):
        r = c.get(path)
        assert r.status_code == 200, (path, r.text[:200])
    assert c.get("/metrics/sales?granularity=weekly").status_code == 422
