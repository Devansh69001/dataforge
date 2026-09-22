"""Gold parquet -> PostgreSQL, idempotently.

For every table:
    1. read the gold parquet (only the requested partitions for incremental runs)
    2. make sure the monthly partitions exist (partitioned facts)
    3. COPY the rows into a TEMP table shaped like the target (fast bulk path)
    4. INSERT ... SELECT FROM temp ON CONFLICT (business key) DO UPDATE SET ...

Step 4 is what makes re-running a batch safe: the same order processed twice resolves to
the same key and simply overwrites itself. Key matches are counted before the merge so
monitoring can report inserts vs updates.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import psycopg
import pyarrow as pa
import pyarrow.dataset as ds

from ..config import Settings, get_settings
from ..db import transaction
from ..logging_utils import get_logger

log = get_logger("warehouse.loader")


@dataclass(frozen=True)
class TableSpec:
    name: str
    keys: tuple[str, ...]
    date_partition: str | None = None  # warehouse partition column (DATE) for partitioned facts
    gold_partition: str | None = None  # gold parquet partition column (yyyy-MM)


TABLES: dict[str, TableSpec] = {
    "dim_date": TableSpec("dim_date", ("date_key",)),
    "dim_region": TableSpec("dim_region", ("region_key",)),
    "dim_category": TableSpec("dim_category", ("category_key",)),
    "dim_supplier": TableSpec("dim_supplier", ("supplier_key",)),
    "dim_warehouse": TableSpec("dim_warehouse", ("warehouse_key",)),
    "dim_customer": TableSpec("dim_customer", ("customer_key",)),
    "dim_product": TableSpec("dim_product", ("product_key",)),
    "fact_orders": TableSpec("fact_orders", ("order_key", "order_date"), "order_date", "order_month"),
    "fact_order_items": TableSpec(
        "fact_order_items", ("order_item_key", "order_date"), "order_date", "order_month"
    ),
    "fact_inventory": TableSpec(
        "fact_inventory", ("inventory_event_key", "event_date"), "event_date", "event_month"
    ),
    "fact_shipping": TableSpec("fact_shipping", ("shipment_key", "order_date"), "order_date", "order_month"),
    "agg_product_daily_sales": TableSpec(
        "agg_product_daily_sales", ("product_key", "order_date"), "order_date", "order_month"
    ),
    "agg_inventory_position": TableSpec("agg_inventory_position", ("position_key",)),
}
DIMENSIONS = [t for t in TABLES if t.startswith("dim_")]
FACTS = [t for t in TABLES if not t.startswith("dim_")]


@dataclass
class LoadResult:
    table: str
    rows_read: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    partitions: list[str] = field(default_factory=list)
    seconds: float = 0.0
    skipped_columns: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def _gold_table(table: str, partitions: list[str] | None, settings: Settings) -> pa.Table | None:
    root: Path = settings.gold_dir / table
    if not root.exists():
        return None
    files: list[Path] = []
    for p in sorted(root.rglob("*.parquet")):
        if partitions is not None:
            part_dir = p.parent.name
            if "=" in part_dir and part_dir.split("=", 1)[1] not in partitions:
                continue
        files.append(p)
    if not files:
        return None
    return ds.dataset([str(f) for f in files], format="parquet").to_table()


def _target_columns(conn: psycopg.Connection, table: str) -> list[str]:
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema='warehouse' AND table_name=%s ORDER BY ordinal_position",
        (table,),
    ).fetchall()
    return [r["column_name"] for r in rows]


def _ensure_partitions(conn: psycopg.Connection, spec: TableSpec, tbl: pa.Table) -> list[str]:
    if not spec.date_partition:
        return []
    col = tbl.column(spec.date_partition).to_pylist()
    months = sorted({date(d.year, d.month, 1) for d in col if d is not None})
    created = []
    for m in months:
        r = conn.execute(
            "SELECT warehouse.ensure_month_partition(%s::regclass, %s)", (f"warehouse.{spec.name}", m)
        ).fetchone()
        created.append(r["ensure_month_partition"])
    return created


def _csv_chunks(tbl: pa.Table, columns: list[str]):
    """Stream the table as CSV bytes (Arrow's C++ writer) for COPY ... FORMAT csv."""
    import pyarrow.csv as pacsv

    opts = pacsv.WriteOptions(include_header=False, batch_size=20_000)
    sub = tbl.select(columns)
    # tz-aware -> naive UTC (epoch preserving); the COPY session runs with TIME ZONE 'UTC'
    for i, f in enumerate(sub.schema):
        if pa.types.is_timestamp(f.type) and f.type.tz is not None:
            sub = sub.set_column(i, f.name, sub.column(i).cast(pa.int64()).cast(pa.timestamp(f.type.unit)))
    for batch in sub.to_batches(max_chunksize=50_000):
        buf = pa.BufferOutputStream()
        pacsv.write_csv(pa.Table.from_batches([batch]), buf, write_options=opts)
        yield buf.getvalue().to_pybytes()


def load_table(
    table: str, partitions: list[str] | None = None, settings: Settings | None = None
) -> LoadResult:
    settings = settings or get_settings()
    spec = TABLES[table]
    t0 = time.time()
    res = LoadResult(table=table)
    tbl = _gold_table(table, partitions, settings)
    if tbl is None or tbl.num_rows == 0:
        log.warning("nothing to load", table=table, partitions=partitions)
        return res
    res.rows_read = tbl.num_rows

    with transaction(settings) as conn:
        target_cols = _target_columns(conn, table)
        if not target_cols:
            raise RuntimeError(f"warehouse.{table} does not exist; run apply_ddl() first")
        cols = [c for c in tbl.column_names if c in target_cols]
        res.skipped_columns = [c for c in tbl.column_names if c not in target_cols]
        if res.skipped_columns:
            log.warning(
                "gold columns not present in warehouse table (ignored)",
                table=table,
                columns=res.skipped_columns,
            )
        res.partitions = _ensure_partitions(conn, spec, tbl)

        conn.execute("SET TIME ZONE 'UTC'")
        tmp = f"tmp_{table}"
        conn.execute(f"CREATE TEMP TABLE {tmp} (LIKE warehouse.{table} INCLUDING DEFAULTS) ON COMMIT DROP")
        col_list = ", ".join(cols)
        # empty string == NULL: silver already turned blanks into NULLs, so nothing is lost
        with (
            conn.cursor() as cur,
            cur.copy(f"COPY {tmp} ({col_list}) FROM STDIN WITH (FORMAT csv, NULL '')") as copy,
        ):
            for chunk in _csv_chunks(tbl, cols):
                copy.write(chunk)

        # idempotent merge: same key -> update in place
        set_cols = [f"{c} = EXCLUDED.{c}" for c in cols if c not in spec.keys]
        if "_loaded_at" in target_cols:
            set_cols.append("_loaded_at = now()")
        conflict = ", ".join(spec.keys)
        action = f"DO UPDATE SET {', '.join(set_cols)}" if set_cols else "DO NOTHING"
        # count key matches before merging so monitoring can report inserts vs updates
        # (system columns such as xmax are not available on partitioned tables)
        join = " AND ".join(f"t.{k} = s.{k}" for k in spec.keys)
        matched = conn.execute(
            f"SELECT count(*) AS n FROM warehouse.{table} t JOIN {tmp} s ON {join}"
        ).fetchone()["n"]
        staged = conn.execute(f"SELECT count(*) AS n FROM {tmp}").fetchone()["n"]
        conn.execute(
            f"INSERT INTO warehouse.{table} ({col_list}) SELECT {col_list} FROM {tmp} ON CONFLICT ({conflict}) {action}"
        )
        res.rows_updated = int(matched)
        res.rows_inserted = int(staged) - int(matched)
    res.seconds = round(time.time() - t0, 2)
    log.info(
        "loaded",
        table=table,
        read=res.rows_read,
        inserted=res.rows_inserted,
        updated=res.rows_updated,
        seconds=res.seconds,
    )
    return res


def load_all(
    partitions_by_table: dict[str, list[str] | None] | None = None, settings: Settings | None = None
) -> dict[str, LoadResult]:
    """Load dimensions then facts. `partitions_by_table` restricts facts to touched months."""
    settings = settings or get_settings()
    out: dict[str, LoadResult] = {}
    for t in DIMENSIONS + FACTS:
        parts = (partitions_by_table or {}).get(t)
        out[t] = load_table(t, parts, settings)
    return out
