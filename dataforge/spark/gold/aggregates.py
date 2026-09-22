"""Gold aggregates computed in Spark.

These are the row-heavy roll-ups that are cheaper to do close to the facts than in
the warehouse; dbt marts build the business summaries on top of them.
"""

from __future__ import annotations

import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from ...config import Settings, get_settings
from .common import GoldResult, read_gold, skey, write_gold


def agg_product_daily_sales(
    spark: SparkSession, months: list[str] | None = None, settings: Settings | None = None
) -> GoldResult:
    """product x day: units, revenue, margin, orders (large-scale aggregation over fact_order_items)."""
    settings = settings or get_settings()
    t0 = time.time()
    items = read_gold(spark, "fact_order_items", months, settings)
    if items is None:
        raise RuntimeError("fact_order_items must be built before agg_product_daily_sales")
    df = (
        items.filter(F.col("is_revenue"))
        .groupBy("product_key", "product_id", "category_key", "supplier_key", "order_date", "order_month")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("line_total_usd"), 2).alias("revenue_usd"),
            F.round(F.sum("gross_margin_usd"), 2).alias("gross_margin_usd"),
            F.countDistinct("customer_key").alias("unique_customers"),
        )
        .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
    )
    mode = "overwrite_partitions" if months else "overwrite"
    return write_gold(df, "agg_product_daily_sales", "order_month", mode, settings, t0)


def agg_inventory_position(spark: SparkSession, settings: Settings | None = None) -> GoldResult:
    """Current stock position per product x warehouse (latest running balance + recency)."""
    settings = settings or get_settings()
    t0 = time.time()
    inv = read_gold(spark, "fact_inventory", None, settings)
    if inv is None:
        raise RuntimeError("fact_inventory must be built before agg_inventory_position")
    w = Window.partitionBy("product_key", "warehouse_key").orderBy(
        F.col("event_ts").desc(), F.col("event_id").desc()
    )
    latest = (
        inv.withColumn("__rn", F.row_number().over(w))
        .filter(F.col("__rn") == 1)
        .select(
            "product_key",
            "warehouse_key",
            F.col("on_hand_after").alias("on_hand_units"),
            F.col("event_ts").alias("last_event_ts"),
        )
    )
    recency = inv.groupBy("product_key", "product_id", "warehouse_key", "warehouse_id").agg(
        F.max(F.when(F.col("event_type") == "receipt", F.col("event_ts"))).alias("last_receipt_ts"),
        F.max(F.when(F.col("event_type") == "shipment", F.col("event_ts"))).alias("last_shipment_ts"),
        F.round(F.avg(F.when(F.col("event_type") == "receipt", F.col("unit_cost"))), 4).alias(
            "avg_unit_cost_usd"
        ),
        F.sum(F.when(F.col("event_type") == "receipt", F.col("quantity_delta")).otherwise(0)).alias(
            "units_received"
        ),
        F.sum(F.when(F.col("event_type") == "shipment", -F.col("quantity_delta")).otherwise(0)).alias(
            "units_shipped"
        ),
        F.sum(F.coalesce("defective_qty", F.lit(0))).alias("units_defective"),
    )
    df = recency.join(latest, ["product_key", "warehouse_key"], "left").select(
        skey("product_id", "warehouse_id").alias("position_key"),
        "product_key",
        "product_id",
        "warehouse_key",
        "warehouse_id",
        F.coalesce("on_hand_units", F.lit(0)).cast("int").alias("on_hand_units"),
        "avg_unit_cost_usd",
        F.round(F.coalesce("on_hand_units", F.lit(0)) * F.coalesce("avg_unit_cost_usd", F.lit(0.0)), 2).alias(
            "inventory_value_usd"
        ),
        "units_received",
        "units_shipped",
        "units_defective",
        "last_receipt_ts",
        "last_shipment_ts",
        "last_event_ts",
    )
    return write_gold(df, "agg_inventory_position", None, "overwrite", settings, t0)
