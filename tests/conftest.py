"""Shared fixtures.

* `lake`   - a throw-away data directory with a small synthetic dataset (scale 0.02).
* `spark`  - one local SparkSession for the whole session (marked tests only).
* `db`     - a dedicated PostgreSQL database `<POSTGRES_DB>_test`; tests are skipped when
             the server is unreachable so the pure-Python suite still runs anywhere.
"""

from __future__ import annotations

import os
import shutil
from datetime import date
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
TEST_SCALE = 0.02
BATCHES = ["2025-11-30", "2025-12-31"]


def pytest_configure(config):
    # every test process gets its own lake directory and test database name
    os.environ.setdefault("DATAFORGE_LOG_LEVEL", "WARNING")


@pytest.fixture(scope="session")
def lake(tmp_path_factory):
    from dataforge.config import get_settings, reset_settings_cache

    base = tmp_path_factory.mktemp("lake")
    os.environ["DATAFORGE_DATA_DIR"] = str(base)
    os.environ["REFERENCE_API_URL"] = f"file://{(base / 'raw' / 'api').as_posix()}"
    reset_settings_cache()
    base_db = get_settings().postgres_db  # from .env / environment
    if not base_db.endswith("_test"):
        os.environ["POSTGRES_DB"] = f"{base_db}_test"
        reset_settings_cache()
    settings = get_settings()

    from dataforge.generators.base import GenConfig
    from dataforge.generators.run import generate_all

    cfg = GenConfig(seed=4242, scale=TEST_SCALE, batches=[date.fromisoformat(b) for b in BATCHES])
    generate_all(cfg, settings.raw_dir)
    yield settings
    shutil.rmtree(base, ignore_errors=True)


@pytest.fixture(scope="session")
def spark(lake):
    pytest.importorskip("pyspark")
    from dataforge.spark.session import get_spark, stop_spark

    s = get_spark("dataforge-tests", lake)
    yield s
    stop_spark()


@pytest.fixture(scope="session")
def db(lake):
    """Create the test database (if the server is reachable) and apply the DDL."""
    import psycopg

    from dataforge.db import apply_ddl

    admin_dsn = lake.postgres_dsn.replace(f"dbname={lake.postgres_db}", "dbname=postgres")
    try:
        with psycopg.connect(admin_dsn, autocommit=True, connect_timeout=3) as conn:
            exists = conn.execute(
                "SELECT 1 FROM pg_database WHERE datname=%s", (lake.postgres_db,)
            ).fetchone()
            if not exists:
                conn.execute(f'CREATE DATABASE "{lake.postgres_db}"')
    except psycopg.OperationalError as e:
        pytest.skip(f"PostgreSQL not reachable: {e}")
    with psycopg.connect(lake.postgres_dsn, autocommit=True) as conn:
        conn.execute(
            "DROP SCHEMA IF EXISTS warehouse, analytics, analytics_staging, analytics_intermediate, analytics_marts, monitoring CASCADE"
        )
    apply_ddl(lake)
    return lake


@pytest.fixture(scope="session")
def bronze(lake):
    from dataforge.ingestion.bronze import ingest_batch

    return ingest_batch(BATCHES[0], "run_test_bronze", settings=lake)


@pytest.fixture(scope="session")
def silver(spark, bronze, lake):
    from dataforge.spark.silver.runner import run_silver

    return run_silver(spark, [BATCHES[0]], "run_test_silver", settings=lake)


@pytest.fixture(scope="session")
def gold(spark, silver, lake):
    from dataforge.spark.gold.runner import run_gold

    return run_gold(spark, None, lake)


@pytest.fixture(scope="session")
def warehouse(db, gold):
    from dataforge.warehouse.loader import load_all

    return load_all(None, db)


@pytest.fixture(scope="session")
def marts(warehouse, db):
    """dbt run on the test database (skipped when dbt is not installed)."""
    import shutil as _sh
    import subprocess

    exe = _sh.which("dbt")
    if not exe:
        pytest.skip("dbt not installed")
    env = {
        **os.environ,
        "POSTGRES_HOST": db.postgres_host,
        "POSTGRES_PORT": str(db.postgres_port),
        "POSTGRES_USER": db.postgres_user,
        "POSTGRES_PASSWORD": db.postgres_password,
        "POSTGRES_DB": db.postgres_db,
        "DBT_TARGET": "ci",
    }
    dbt_dir = REPO_ROOT / "dbt"
    proc = subprocess.run(
        [
            exe,
            "run",
            "--profiles-dir",
            str(dbt_dir),
            "--project-dir",
            str(dbt_dir),
            "--no-use-colors",
            "--full-refresh",
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stdout[-2000:]
    return db
