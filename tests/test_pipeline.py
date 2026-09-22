"""Pipeline spec / DAG consistency, monitoring tracker, and the end-to-end incremental + rerun flow."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from dataforge.pipeline.spec import PIPELINE, TASK_BY_NAME, topological_order, validate_spec
from dataforge.pipeline.tasks import INGEST_GROUPS, SILVER_GROUPS, TASK_FUNCTIONS

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_spec_is_a_dag_and_every_task_is_implemented():
    validate_spec()
    order = topological_order()
    assert order[0] == "check_sources" and order[-1] == "publish"
    assert set(TASK_FUNCTIONS) == set(TASK_BY_NAME)
    assert set(INGEST_GROUPS) <= set(TASK_BY_NAME) and set(SILVER_GROUPS) <= set(TASK_BY_NAME)
    # silver order respects referential dependencies
    assert order.index("silver_customers") < order.index("silver_orders") < order.index("silver_order_items")
    assert (
        order.index("load_facts") < order.index("dbt_run") < order.index("dbt_test") < order.index("publish")
    )


def test_airflow_dag_loads():
    if importlib.util.find_spec("airflow") is None:
        pytest.skip("apache-airflow not installed on this host (runs in the Docker image)")
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(REPO_ROOT / "dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    dag = bag.get_dag("dataforge_pipeline")
    assert dag is not None
    assert {t.task_id.split(".")[-1] for t in dag.tasks} == {t.name for t in PIPELINE}


def test_dag_file_is_syntactically_valid():
    src = (REPO_ROOT / "dags" / "dataforge_pipeline.py").read_text(encoding="utf-8")
    compile(src, "dataforge_pipeline.py", "exec")
    assert 'dag_id="dataforge_pipeline"' in src and "retries" in src and "on_failure_callback" in src


def test_tracker_records_tasks_and_survives_reload(lake):
    from dataforge.monitoring.tracker import RunTracker

    t = RunTracker("run_unit_1", "initial", "2025-11-30", lake)
    t.db_ok = False  # file-only
    t.start()
    with t.task("demo") as rec:
        rec["rows_in"] = 5
    with pytest.raises(ValueError), t.task("boom"):
        raise ValueError("nope")
    t.add_totals(rows_ingested=5)
    t.finish("failed", "boom failed")
    doc = json.loads((lake.monitoring_dir / "runs" / "run_unit_1.json").read_text())
    assert doc["status"] == "failed" and doc["totals"]["rows_ingested"] == 5
    st = {x["task_name"]: x["status"] for x in doc["tasks"]}
    assert st == {"demo": "success", "boom": "failed"}
    assert RunTracker.load("run_unit_1", lake).doc["tasks"][1]["error"].startswith("ValueError")


@pytest.mark.e2e
@pytest.mark.spark
@pytest.mark.db
def test_end_to_end_initial_incremental_rerun(lake, db, spark, tmp_path, monkeypatch):
    """initial -> incremental -> rerun on an isolated copy of the small lake and its own database;
    the rerun must not change the warehouse."""
    import shutil

    import psycopg

    from dataforge.config import get_settings, reset_settings_cache
    from dataforge.db import fetch_one
    from dataforge.pipeline.runner import run_pipeline

    shutil.copytree(lake.raw_dir, tmp_path / "raw")
    e2e_db = f"{lake.postgres_db}_e2e"
    admin = lake.postgres_dsn.replace(f"dbname={lake.postgres_db}", "dbname=postgres")
    with psycopg.connect(admin, autocommit=True) as conn:
        if not conn.execute("SELECT 1 FROM pg_database WHERE datname=%s", (e2e_db,)).fetchone():
            conn.execute(f'CREATE DATABASE "{e2e_db}"')
    monkeypatch.setenv("DATAFORGE_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("REFERENCE_API_URL", f"file://{(tmp_path / 'raw' / 'api').as_posix()}")
    monkeypatch.setenv("POSTGRES_DB", e2e_db)
    reset_settings_cache()
    lake = get_settings()
    with psycopg.connect(lake.postgres_dsn, autocommit=True) as conn:
        conn.execute(
            "DROP SCHEMA IF EXISTS warehouse, analytics, analytics_staging, analytics_intermediate, analytics_marts, monitoring CASCADE"
        )

    r1 = run_pipeline("2025-11-30", run_id="e2e_1", triggered_by="pytest", stop_spark_on_exit=False)
    assert r1["status"] == "success", r1
    assert r1["mode"] == "initial" and r1["quality_status"] in ("pass", "warn")
    n1 = fetch_one(
        "SELECT count(*) AS n, count(DISTINCT order_id) AS u FROM warehouse.fact_orders", settings=lake
    )
    assert n1["n"] == n1["u"] > 0

    r2 = run_pipeline("2025-12-31", run_id="e2e_2", triggered_by="pytest", stop_spark_on_exit=False)
    assert r2["status"] == "success" and r2["mode"] == "incremental", r2
    n2 = fetch_one(
        "SELECT count(*) AS n, count(DISTINCT order_id) AS u FROM warehouse.fact_orders", settings=lake
    )
    assert n2["n"] == n2["u"] > n1["n"]
    rev2 = fetch_one(
        "SELECT sum(revenue_usd) AS r FROM analytics_marts.monthly_revenue_summary", settings=lake
    )["r"]

    r3 = run_pipeline("2025-12-31", run_id="e2e_3", triggered_by="pytest", stop_spark_on_exit=False)
    assert r3["status"] == "success" and r3["mode"] == "rerun", r3
    n3 = fetch_one(
        "SELECT count(*) AS n, count(DISTINCT order_id) AS u FROM warehouse.fact_orders", settings=lake
    )
    assert n3 == n2  # idempotent: no duplicates, no growth
    rev3 = fetch_one(
        "SELECT sum(revenue_usd) AS r FROM analytics_marts.monthly_revenue_summary", settings=lake
    )["r"]
    assert abs(rev3 - rev2) < 0.01
    wm = fetch_one("SELECT last_batch_id FROM monitoring.watermarks WHERE dataset='orders'", settings=lake)[
        "last_batch_id"
    ]
    assert wm == "2025-12-31"
    drift = fetch_one(
        "SELECT count(*) AS n FROM monitoring.schema_events WHERE change_type='new_column'", settings=lake
    )["n"]
    assert drift >= 1  # eco_rating / promo_campaign in batch 2
    monkeypatch.undo()
    reset_settings_cache()
