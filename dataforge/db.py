"""PostgreSQL connection helpers (psycopg 3)."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from .config import Settings, get_settings
from .logging_utils import get_logger

log = get_logger("db")

DDL_DIR = Path(__file__).parent / "warehouse" / "ddl"


class DatabaseUnavailable(Exception):
    pass


def connect(settings: Settings | None = None, autocommit: bool = False) -> psycopg.Connection:
    settings = settings or get_settings()
    try:
        return psycopg.connect(
            settings.postgres_dsn, autocommit=autocommit, row_factory=dict_row, connect_timeout=5
        )
    except psycopg.OperationalError as e:
        raise DatabaseUnavailable(f"cannot connect to {settings.redacted_dsn()}: {e}") from e


@contextmanager
def transaction(settings: Settings | None = None) -> Iterator[psycopg.Connection]:
    conn = connect(settings)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ping(settings: Settings | None = None) -> bool:
    try:
        with connect(settings, autocommit=True) as conn:
            conn.execute("SELECT 1")
        return True
    except DatabaseUnavailable:
        return False


def apply_ddl(settings: Settings | None = None) -> list[str]:
    """Apply every DDL file in order. All statements are idempotent (IF NOT EXISTS)."""
    applied = []
    with transaction(settings) as conn:
        for f in sorted(DDL_DIR.glob("*.sql")):
            conn.execute(f.read_text(encoding="utf-8"))
            applied.append(f.name)
    log.info("warehouse DDL applied", files=applied)
    return applied


def fetch_all(sql: str, params: Any = None, settings: Settings | None = None) -> list[dict[str, Any]]:
    with connect(settings, autocommit=True) as conn:
        return conn.execute(sql, params).fetchall()


def fetch_one(sql: str, params: Any = None, settings: Settings | None = None) -> dict[str, Any] | None:
    with connect(settings, autocommit=True) as conn:
        return conn.execute(sql, params).fetchone()


def execute(sql: str, params: Any = None, settings: Settings | None = None) -> None:
    with transaction(settings) as conn:
        conn.execute(sql, params)
