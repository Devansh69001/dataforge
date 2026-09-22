"""Silver orders, order_items and payments.

orders       partitioned by order_month; late-arriving status updates replace the older
             version of the same order_id (keep_latest on updated_at).
order_items  partitioned by the parent order's month (needs a join to silver orders, which
             doubles as the referential-integrity check).
payments     partitioned by paid_month.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ...config import Settings, get_settings
from ...quality.suites import ORDER_ITEMS, ORDERS, PAYMENTS
from .common import SilverResult, country_code, integer, lower, month_of, norm, num, run_silver_job, ts, upper
from .reference import load_refs


# ------------------------------------------------------------------- orders
def transform_orders(df: DataFrame, refs: dict[str, DataFrame]) -> DataFrame:
    cust = refs["customers"].select(
        F.col("customer_id").alias("__cust_id"), F.col("region_code").alias("__cust_region")
    )
    df = df.join(F.broadcast(cust), df["customer_id"] == cust["__cust_id"], "left").drop("__cust_id")
    return (
        df.withColumn("status_n", lower("status"))
        .withColumn("channel_n", lower("channel"))
        .withColumn("currency_n", upper("currency"))
        .withColumn("region_code_n", upper("region_code"))
        .withColumn("region_code_final", F.coalesce(F.col("region_code_n"), F.col("__cust_region")))
        .withColumn("order_date_t", ts("order_date"))
        .withColumn("updated_at_t", ts("updated_at"))
        .withColumn("order_total_t", num("order_total"))
        .drop("__cust_region")
    )


def final_orders(df: DataFrame) -> DataFrame:
    return df.select(
        norm("order_id").alias("order_id"),
        norm("customer_id").alias("customer_id"),
        F.col("order_date_t").alias("order_ts"),
        F.to_date("order_date_t").alias("order_date"),
        month_of("order_date_t").alias("order_month"),
        F.col("status_n").alias("status"),
        F.col("channel_n").alias("channel"),
        F.col("currency_n").alias("currency"),
        F.col("region_code_final").alias("region_code"),
        country_code("shipping_country").alias("shipping_country_code"),
        upper("coupon_code").alias("coupon_code"),
        F.col("order_total_t").alias("order_total"),
        F.col("updated_at_t").alias("updated_at"),
        "quality_warnings",
        "_batch_id",
        "_source_file",
        "_ingested_at",
        "_row_number",
    )


def silver_orders(
    spark: SparkSession, batch_ids: list[str], run_id: str, settings: Settings | None = None
) -> SilverResult:
    settings = settings or get_settings()
    refs = load_refs(spark, ["customers"], settings)
    return run_silver_job(
        spark,
        "orders",
        batch_ids,
        run_id,
        transform_orders,
        refs,
        ORDERS,
        final_orders,
        keys=["order_id"],
        version_col="updated_at",
        partition_col="order_month",
        settings=settings,
    )


# -------------------------------------------------------------- order items
def transform_items(df: DataFrame, refs: dict[str, DataFrame]) -> DataFrame:
    orders = refs["orders"].select(
        F.col("order_id").alias("__oid"), F.col("order_month").alias("order_month_ref")
    )
    df = df.join(orders, df["order_id"] == orders["__oid"], "left").drop("__oid")
    df = (
        df.withColumn("quantity_t", integer("quantity"))
        .withColumn("unit_price_t", num("unit_price"))
        .withColumn("discount_pct_t", F.coalesce(num("discount_pct"), F.lit(0.0)))
        .withColumn("line_total_t", num("line_total"))
    )
    return df.withColumn(
        "line_total_calc",
        F.round(F.col("quantity_t") * F.col("unit_price_t") * (1 - F.col("discount_pct_t") / 100), 2),
    )


def final_items(df: DataFrame) -> DataFrame:
    consistent = F.abs(F.col("line_total_t") - F.col("line_total_calc")) <= 0.05
    return df.select(
        norm("order_item_id").alias("order_item_id"),
        norm("order_id").alias("order_id"),
        norm("product_id").alias("product_id"),
        F.col("quantity_t").cast("int").alias("quantity"),
        F.col("unit_price_t").alias("unit_price"),
        F.col("discount_pct_t").alias("discount_pct"),
        F.when(consistent, F.col("line_total_t")).otherwise(F.col("line_total_calc")).alias("line_total"),
        F.col("line_total_t").alias("line_total_source"),
        upper("currency").alias("currency"),
        F.col("order_month_ref").alias("order_month"),
        "quality_warnings",
        "_batch_id",
        "_source_file",
        "_ingested_at",
        "_row_number",
    )


def silver_order_items(
    spark: SparkSession, batch_ids: list[str], run_id: str, settings: Settings | None = None
) -> SilverResult:
    settings = settings or get_settings()
    refs = load_refs(spark, ["orders", "products"], settings)
    return run_silver_job(
        spark,
        "order_items",
        batch_ids,
        run_id,
        transform_items,
        refs,
        ORDER_ITEMS,
        final_items,
        keys=["order_item_id"],
        version_col=None,
        partition_col="order_month",
        settings=settings,
    )


# ----------------------------------------------------------------- payments
def transform_payments(df: DataFrame, refs: dict[str, DataFrame]) -> DataFrame:
    orders = refs["orders"].select(
        F.col("order_id").alias("__oid"), F.col("order_total").alias("order_total_ref")
    )
    df = df.join(orders, df["order_id"] == orders["__oid"], "left").drop("__oid")
    return (
        df.withColumn("payment_method_n", lower("payment_method"))
        .withColumn("status_n", lower("status"))
        .withColumn("amount_t", num("amount"))
        .withColumn("paid_at_t", ts("paid_at"))
    )


def final_payments(df: DataFrame) -> DataFrame:
    return df.select(
        norm("payment_id").alias("payment_id"),
        norm("order_id").alias("order_id"),
        F.col("payment_method_n").alias("payment_method"),
        F.col("amount_t").alias("amount"),
        upper("currency").alias("currency"),
        F.col("status_n").alias("status"),
        F.col("paid_at_t").alias("paid_at"),
        month_of("paid_at_t").alias("paid_month"),
        "quality_warnings",
        "_batch_id",
        "_source_file",
        "_ingested_at",
        "_row_number",
    )


def silver_payments(
    spark: SparkSession, batch_ids: list[str], run_id: str, settings: Settings | None = None
) -> SilverResult:
    settings = settings or get_settings()
    refs = load_refs(spark, ["orders"], settings)
    return run_silver_job(
        spark,
        "payments",
        batch_ids,
        run_id,
        transform_payments,
        refs,
        PAYMENTS,
        final_payments,
        keys=["payment_id"],
        version_col=None,
        partition_col="paid_month",
        settings=settings,
    )
