#!/usr/bin/env python
"""pandas vs PySpark on the silver order-lines workload, at several data scales.

The workload mirrors what silver + gold do for order lines:
    1. typed casts of the bronze strings
    2. rule checks (quantity > 0, unit_price > 0) and referential checks (orders, products)
    3. joins to orders (date, status) and products (unit cost)
    4. derived measures (line total, gross margin)
    5. product x day aggregation
    6. window: cumulative revenue per product ordered by day
    7. dedup: keep latest by (order_item_id, _ingested_at)

Larger scales are produced by replicating the real bronze rows with fresh ids, so both
engines see identical inputs. Timings include reading parquet from disk. Results go to
benchmarks/results/pandas_vs_spark.json and a markdown table is printed.

    python benchmarks/benchmark_pandas_vs_spark.py --scales 1,4,10
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dataforge.config import get_settings  # noqa: E402
from dataforge.ingestion.bronze import bronze_files  # noqa: E402

RESULTS = Path(__file__).resolve().parent / "results" / "pandas_vs_spark.json"


def _read_bronze(dataset: str, cols: list[str]) -> pa.Table:
    files = bronze_files(dataset)
    if not files:
        raise SystemExit(f"no bronze data for {dataset}; run the pipeline first")
    return pa.concat_tables([pq.read_table(f, columns=cols) for f in files])


def prepare(scale: int, work: Path) -> dict[str, Path]:
    """Materialise scaled parquet inputs once so both engines read the same files."""
    work.mkdir(parents=True, exist_ok=True)
    items = _read_bronze("order_items", ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_pct", "_ingested_at"]).to_pandas()
    orders = _read_bronze("orders", ["order_id", "order_date", "status"]).to_pandas().drop_duplicates("order_id")
    products = _read_bronze("products", ["product_id", "unit_cost"]).to_pandas().drop_duplicates("product_id")
    if scale > 1:
        parts = []
        for k in range(scale):
            p = items.copy()
            if k:
                p["order_item_id"] = p["order_item_id"] + f"-{k}"
            parts.append(p)
        items = pd.concat(parts, ignore_index=True)
    paths = {"items": work / f"items_x{scale}.parquet", "orders": work / "orders.parquet", "products": work / "products.parquet"}
    pq.write_table(pa.Table.from_pandas(items, preserve_index=False), paths["items"])
    pq.write_table(pa.Table.from_pandas(orders, preserve_index=False), paths["orders"])
    pq.write_table(pa.Table.from_pandas(products, preserve_index=False), paths["products"])
    return paths


# ------------------------------------------------------------------ pandas
def run_pandas(paths: dict[str, Path]) -> dict:
    t0 = time.perf_counter()
    items = pd.read_parquet(paths["items"])
    orders = pd.read_parquet(paths["orders"])
    products = pd.read_parquet(paths["products"])
    items["quantity_t"] = pd.to_numeric(items["quantity"], errors="coerce")
    items["unit_price_t"] = pd.to_numeric(items["unit_price"], errors="coerce")
    items["discount_t"] = pd.to_numeric(items["discount_pct"], errors="coerce").fillna(0.0)
    orders["order_date_t"] = pd.to_datetime(orders["order_date"], errors="coerce", utc=True)
    products["unit_cost_t"] = pd.to_numeric(products["unit_cost"], errors="coerce")
    # dedup keep latest
    items = items.sort_values(["order_item_id", "_ingested_at"]).drop_duplicates("order_item_id", keep="last")
    # rules + referential checks
    df = items.merge(orders[["order_id", "order_date_t", "status"]], on="order_id", how="left").merge(products[["product_id", "unit_cost_t"]], on="product_id", how="left")
    ok = (df.quantity_t > 0) & (df.unit_price_t > 0) & df.order_date_t.notna() & df.unit_cost_t.notna()
    rejected = int((~ok).sum())
    df = df[ok]
    df["line_total"] = (df.quantity_t * df.unit_price_t * (1 - df.discount_t / 100)).round(2)
    df["gross_margin"] = df.line_total - df.quantity_t * df.unit_cost_t
    df["day"] = df.order_date_t.dt.floor("D")
    agg = df.groupby(["product_id", "day"], as_index=False).agg(revenue=("line_total", "sum"), margin=("gross_margin", "sum"), units=("quantity_t", "sum"))
    agg = agg.sort_values(["product_id", "day"])
    agg["cum_revenue"] = agg.groupby("product_id")["revenue"].cumsum()
    n_out = len(agg)
    return {"engine": "pandas", "seconds": round(time.perf_counter() - t0, 2), "rows_in": len(items), "rejected": rejected, "rows_out": n_out}


# ------------------------------------------------------------------- spark
def run_spark(paths: dict[str, Path], spark) -> dict:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    t0 = time.perf_counter()
    items = spark.read.parquet(paths["items"].as_posix())
    orders = spark.read.parquet(paths["orders"].as_posix())
    products = spark.read.parquet(paths["products"].as_posix())
    items = (
        items.withColumn("quantity_t", F.col("quantity").cast("double"))
        .withColumn("unit_price_t", F.col("unit_price").cast("double"))
        .withColumn("discount_t", F.coalesce(F.col("discount_pct").cast("double"), F.lit(0.0)))
    )
    w = Window.partitionBy("order_item_id").orderBy(F.col("_ingested_at").desc())
    items = items.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
    orders = orders.withColumn("order_date_t", F.to_timestamp("order_date", "yyyy-MM-dd'T'HH:mm:ss'Z'")).select("order_id", "order_date_t", "status")
    products = products.withColumn("unit_cost_t", F.col("unit_cost").cast("double")).select("product_id", "unit_cost_t")
    df = items.join(orders, "order_id", "left").join(F.broadcast(products), "product_id", "left")
    ok = (F.col("quantity_t") > 0) & (F.col("unit_price_t") > 0) & F.col("order_date_t").isNotNull() & F.col("unit_cost_t").isNotNull()
    df = df.withColumn("ok", ok).cache()
    rejected = df.filter(~F.col("ok")).count()
    good = df.filter("ok").withColumn("line_total", F.round(F.col("quantity_t") * F.col("unit_price_t") * (1 - F.col("discount_t") / 100), 2))
    good = good.withColumn("gross_margin", F.col("line_total") - F.col("quantity_t") * F.col("unit_cost_t")).withColumn("day", F.date_trunc("day", "order_date_t"))
    agg = good.groupBy("product_id", "day").agg(F.sum("line_total").alias("revenue"), F.sum("gross_margin").alias("margin"), F.sum("quantity_t").alias("units"))
    w2 = Window.partitionBy("product_id").orderBy("day").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    agg = agg.withColumn("cum_revenue", F.sum("revenue").over(w2))
    n_out = agg.count()
    n_in = df.count()
    df.unpersist()
    return {"engine": "pyspark", "seconds": round(time.perf_counter() - t0, 2), "rows_in": n_in, "rejected": int(rejected), "rows_out": n_out}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scales", default="1,4,10", help="replication factors of the order_items bronze data")
    ap.add_argument("--repeat", type=int, default=2, help="timings per engine/scale; the best is reported")
    a = ap.parse_args()
    scales = [int(s) for s in a.scales.split(",")]
    settings = get_settings()
    work = Path(settings.effective_spark_tmp_dir) / "benchmark"

    from dataforge.spark.session import get_spark, stop_spark

    spark = get_spark("dataforge-benchmark", settings)
    t = time.perf_counter()
    spark.range(1).count()
    spark_startup = round(time.perf_counter() - t, 2)

    rows = []
    for scale in scales:
        paths = prepare(scale, work)
        best = {}
        for engine, fn in (("pandas", lambda: run_pandas(paths)), ("pyspark", lambda: run_spark(paths, spark))):
            runs = [fn() for _ in range(a.repeat)]
            best[engine] = min(runs, key=lambda r: r["seconds"])
        p, s = best["pandas"], best["pyspark"]
        assert p["rejected"] == s["rejected"] and p["rows_out"] == s["rows_out"], (p, s)
        rows.append({"scale": scale, "rows_in": p["rows_in"], "rejected": p["rejected"], "rows_out": p["rows_out"],
                     "pandas_seconds": p["seconds"], "pyspark_seconds": s["seconds"],
                     "pandas_rows_per_s": int(p["rows_in"] / p["seconds"]), "pyspark_rows_per_s": int(s["rows_in"] / s["seconds"])})
        print(f"scale x{scale}: rows={p['rows_in']:,} pandas={p['seconds']}s pyspark={s['seconds']}s", flush=True)
    stop_spark()

    doc = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "host": {"platform": platform.platform(), "python": platform.python_version(), "cpu_count": __import__("os").cpu_count(),
                 "spark_master": settings.spark_master, "spark_writer": settings.effective_spark_writer, "spark_startup_seconds": spark_startup},
        "workload": "silver order-lines: cast, dedup(window), rules, 2 joins, derived measures, product x day aggregation, cumulative window",
        "results": rows,
        "note": "single machine; Spark local mode. pandas is faster while the data fits in memory; Spark's cost is JVM/scheduling overhead, its benefit is horizontal scale beyond one machine's RAM.",
    }
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    RESULTS.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    print("\n| scale | rows | pandas (s) | PySpark (s) | pandas rows/s | PySpark rows/s |\n|---|---|---|---|---|---|")
    for r in rows:
        print(f"| x{r['scale']} | {r['rows_in']:,} | {r['pandas_seconds']} | {r['pyspark_seconds']} | {r['pandas_rows_per_s']:,} | {r['pyspark_rows_per_s']:,} |")
    print(f"\n(Spark session start-up: {spark_startup}s, not included in the rows above)\nwritten: {RESULTS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
