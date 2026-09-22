"""Silver shipping events (parsed tracker logs), partitioned by event_month."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ...config import Settings, get_settings
from ...quality.suites import SHIPPING_EVENTS
from .common import SilverResult, lower, month_of, norm, run_silver_job, ts, upper
from .reference import load_refs


def transform(df: DataFrame, refs: dict[str, DataFrame]) -> DataFrame:
    return (
        df.withColumn("event_n", lower("event"))
        .withColumn("order_id_n", norm("order_id"))
        .withColumn("event_ts_t", ts("event_ts"))
        .withColumn("warehouse_id", upper("warehouse_id"))
        .withColumn("carrier", norm("carrier"))
    )


def final_select(df: DataFrame) -> DataFrame:
    return df.select(
        norm("shipment_id").alias("shipment_id"),
        F.col("order_id_n").alias("order_id"),
        F.col("event_n").alias("event"),
        F.col("event_ts_t").alias("event_ts"),
        month_of("event_ts_t").alias("event_month"),
        F.col("carrier"),
        F.col("warehouse_id"),
        upper("region").alias("region_code"),
        norm("location").alias("location"),
        "quality_warnings",
        "_batch_id",
        "_source_file",
        "_ingested_at",
        "_row_number",
    )


def silver_shipping_events(
    spark: SparkSession, batch_ids: list[str], run_id: str, settings: Settings | None = None
) -> SilverResult:
    settings = settings or get_settings()
    refs = load_refs(spark, ["orders", "warehouses"], settings)
    return run_silver_job(
        spark,
        "shipping_events",
        batch_ids,
        run_id,
        transform,
        refs,
        SHIPPING_EVENTS,
        final_select,
        keys=["shipment_id", "event", "event_ts"],
        version_col=None,
        partition_col="event_month",
        settings=settings,
    )
