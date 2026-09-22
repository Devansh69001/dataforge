"""Silver reference data: regions, warehouses, categories, suppliers, exchange rates.

These are small, trusted feeds; they are typed and deduplicated but carry no rule
suite of their own (they ARE the reference the other suites check against).
"""

from __future__ import annotations

import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ...config import Settings, get_settings
from .common import (
    SilverResult,
    boolean,
    date,
    drop_exact_duplicates,
    integer,
    load_bronze,
    merge_snapshot,
    norm,
    num,
    upper,
)

META_KEEP = ["_batch_id", "_source_file", "_ingested_at", "_row_number"]


def _run(
    spark: SparkSession,
    dataset: str,
    batch_ids: list[str],
    select: list[F.Column],
    keys: list[str],
    version_col: str | None,
    settings: Settings,
) -> SilverResult:
    t0 = time.time()
    res = SilverResult(dataset=dataset, batch_ids=batch_ids)
    df = load_bronze(spark, dataset, batch_ids, settings)
    if df is None:
        return res
    df, dups = drop_exact_duplicates(df, dataset)
    res.exact_duplicates_removed = dups
    out = df.select(*select, *[F.col(c) for c in META_KEEP]).withColumn(
        "quality_warnings", F.array().cast("array<string>")
    )
    res.input_rows = out.count() + dups
    res.valid_rows = res.input_rows - dups
    res.written_rows = merge_snapshot(spark, out, dataset, keys, version_col, settings)
    res.seconds = round(time.time() - t0, 2)
    return res


def silver_regions(
    spark: SparkSession, batch_ids: list[str], run_id: str, settings: Settings | None = None
) -> SilverResult:
    settings = settings or get_settings()
    sel = [
        upper("region_code").alias("region_code"),
        norm("region_name").alias("region_name"),
        upper("country_code").alias("country_code"),
        norm("country_name").alias("country_name"),
        upper("currency_code").alias("currency_code"),
        num("base_delivery_days").alias("base_delivery_days"),
        norm("timezone").alias("timezone"),
    ]
    return _run(spark, "regions", batch_ids, sel, ["region_code"], None, settings)


def silver_warehouses(
    spark: SparkSession, batch_ids: list[str], run_id: str, settings: Settings | None = None
) -> SilverResult:
    settings = settings or get_settings()
    sel = [
        upper("warehouse_id").alias("warehouse_id"),
        norm("warehouse_name").alias("warehouse_name"),
        norm("city").alias("city"),
        upper("region_code").alias("region_code"),
        integer("capacity_units").alias("capacity_units"),
        date("opened_date").alias("opened_date"),
    ]
    return _run(spark, "warehouses", batch_ids, sel, ["warehouse_id"], None, settings)


def silver_categories(
    spark: SparkSession, batch_ids: list[str], run_id: str, settings: Settings | None = None
) -> SilverResult:
    settings = settings or get_settings()
    sel = [
        upper("category_id").alias("category_id"),
        norm("category_name").alias("category_name"),
        upper("parent_category_id").alias("parent_category_id"),
        integer("level").alias("level"),
    ]
    return _run(spark, "categories", batch_ids, sel, ["category_id"], None, settings)


def silver_suppliers(
    spark: SparkSession, batch_ids: list[str], run_id: str, settings: Settings | None = None
) -> SilverResult:
    settings = settings or get_settings()
    sel = [
        upper("supplier_id").alias("supplier_id"),
        norm("supplier_name").alias("supplier_name"),
        upper("country_code").alias("country_code"),
        upper("region_code").alias("region_code"),
        integer("lead_time_days").alias("lead_time_days"),
        upper("quality_tier").alias("quality_tier"),
        boolean("active").alias("active"),
    ]
    return _run(spark, "suppliers", batch_ids, sel, ["supplier_id"], None, settings)


def silver_exchange_rates(
    spark: SparkSession, batch_ids: list[str], run_id: str, settings: Settings | None = None
) -> SilverResult:
    settings = settings or get_settings()
    sel = [
        date("rate_date").alias("rate_date"),
        upper("currency_code").alias("currency_code"),
        num("usd_per_unit").alias("usd_per_unit"),
    ]
    return _run(spark, "exchange_rates", batch_ids, sel, ["rate_date", "currency_code"], None, settings)


def load_refs(
    spark: SparkSession, names: list[str], settings: Settings | None = None
) -> dict[str, DataFrame]:
    from .common import read_silver

    out = {}
    for n in names:
        df = read_silver(spark, n, settings=settings)
        if df is None:
            raise RuntimeError(f"silver reference '{n}' is not available yet; run its silver job first")
        out[n] = df
    return out
