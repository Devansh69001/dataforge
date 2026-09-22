"""Silver inventory events, partitioned by event_month."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ...config import Settings, get_settings
from ...quality.suites import INVENTORY_EVENTS
from .common import SilverResult, integer, lower, month_of, norm, num, run_silver_job, ts, upper
from .reference import load_refs


def transform(df: DataFrame, refs: dict[str, DataFrame]) -> DataFrame:
    return (
        df.withColumn("event_type_n", lower("event_type"))
        .withColumn("quantity_delta_t", integer("quantity_delta"))
        .withColumn("defective_qty_t", integer("defective_qty"))
        .withColumn("counted_quantity_t", integer("counted_quantity"))
        .withColumn("unit_cost_t", num("unit_cost"))
        .withColumn("event_ts_t", ts("event_ts"))
        .withColumn("product_id", upper("product_id"))
        .withColumn("warehouse_id", upper("warehouse_id"))
    )


def final_select(df: DataFrame) -> DataFrame:
    return df.select(
        norm("event_id").alias("event_id"),
        F.col("event_type_n").alias("event_type"),
        F.col("product_id"),
        F.col("warehouse_id"),
        F.col("quantity_delta_t").cast("int").alias("quantity_delta"),
        F.col("event_ts_t").alias("event_ts"),
        F.to_date("event_ts_t").alias("event_date"),
        month_of("event_ts_t").alias("event_month"),
        norm("reference_id").alias("reference_id"),
        upper("supplier_id").alias("supplier_id"),
        F.col("unit_cost_t").alias("unit_cost"),
        F.col("defective_qty_t").cast("int").alias("defective_qty"),
        F.col("counted_quantity_t").cast("int").alias("counted_quantity"),
        lower("reason").alias("reason"),
        norm("source_system").alias("source_system"),
        "quality_warnings",
        "_batch_id",
        "_source_file",
        "_ingested_at",
        "_row_number",
    )


def silver_inventory_events(
    spark: SparkSession, batch_ids: list[str], run_id: str, settings: Settings | None = None
) -> SilverResult:
    settings = settings or get_settings()
    refs = load_refs(spark, ["products", "warehouses"], settings)
    return run_silver_job(
        spark,
        "inventory_events",
        batch_ids,
        run_id,
        transform,
        refs,
        INVENTORY_EVENTS,
        final_select,
        keys=["event_id"],
        version_col=None,
        partition_col="event_month",
        settings=settings,
    )
