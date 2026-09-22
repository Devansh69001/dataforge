"""Gold facts.

Every fact is partitioned by month and rebuilt only for the months touched by the
current batch (`months=None` rebuilds everything). Where a metric needs history that
spans months (customer order sequence, running on-hand stock, shipment lifecycle) the
window is computed over the full silver dataset and the OUTPUT is restricted to the
touched months, so an incremental run yields exactly the same rows as a full rebuild.

Spark features exercised here on purpose:
    * joins across five datasets (orders, items, products, payments, fx)
    * window functions: row_number / lag (customer sequence), cumulative sum (on-hand),
      last-value forward fill (fx rates)
    * large-scale aggregation (payments per order, items per order, events per shipment)
    * partition-aware reads/writes (only touched months)
"""

from __future__ import annotations

import time

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ...config import Settings, get_settings
from .common import GoldResult, date_key, dense_fx_rates, silver, skey, write_gold


def _mode(months: list[str] | None) -> str:
    return "overwrite_partitions" if months else "overwrite"


def _restrict(df: DataFrame, col: str, months: list[str] | None) -> DataFrame:
    return df.filter(F.col(col).isin(months)) if months else df


# --------------------------------------------------------------- fact_orders
def fact_orders(
    spark: SparkSession, months: list[str] | None = None, settings: Settings | None = None
) -> GoldResult:
    settings = settings or get_settings()
    t0 = time.time()
    orders = silver(spark, "orders", settings=settings)  # full history for the customer window
    items = silver(spark, "order_items", months, settings)
    payments = silver(spark, "payments", settings=settings)
    fx = (
        dense_fx_rates(spark, settings)
        .withColumnRenamed("rate_date", "order_date")
        .withColumnRenamed("currency_code", "currency")
    )

    item_agg = items.groupBy("order_id").agg(
        F.count("*").alias("item_count"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("line_total"), 2).alias("items_total_local"),
        F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2).alias("gross_local"),
    )
    pay_agg = payments.groupBy("order_id").agg(
        F.max(F.when(F.col("status") == "captured", F.col("paid_at"))).alias("paid_at"),
        F.sum(F.when(F.col("status") == "captured", F.col("amount")).otherwise(0.0)).alias(
            "captured_amount_local"
        ),
        F.sum(F.when(F.col("status") == "refunded", -F.col("amount")).otherwise(0.0)).alias(
            "refunded_amount_local"
        ),
        F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).alias("failed_payment_attempts"),
    )
    w = Window.partitionBy("customer_id").orderBy("order_ts", "order_id")
    o = (
        orders.withColumn("customer_order_seq", F.row_number().over(w))
        .withColumn("prev_order_ts", F.lag("order_ts").over(w))
        .withColumn("days_since_prev_order", F.datediff("order_ts", "prev_order_ts"))
    )
    o = _restrict(o, "order_month", months)
    df = (
        o.join(item_agg, "order_id", "left")
        .join(pay_agg, "order_id", "left")
        .join(F.broadcast(fx), ["order_date", "currency"], "left")
        .select(
            skey("order_id").alias("order_key"),
            "order_id",
            skey("customer_id").alias("customer_key"),
            "customer_id",
            skey("region_code").alias("region_key"),
            "region_code",
            date_key("order_date").alias("order_date_key"),
            "order_ts",
            "order_date",
            "order_month",
            "status",
            "channel",
            "currency",
            "coupon_code",
            F.col("order_total").alias("order_total_local"),
            F.col("usd_per_unit").alias("fx_usd_per_unit"),
            F.round(F.col("order_total") * F.col("usd_per_unit"), 2).alias("order_total_usd"),
            F.round(F.col("gross_local") * F.col("usd_per_unit"), 2).alias("gross_amount_usd"),
            F.round(
                (
                    F.coalesce(F.col("gross_local"), F.lit(0.0))
                    - F.coalesce(F.col("items_total_local"), F.lit(0.0))
                )
                * F.col("usd_per_unit"),
                2,
            ).alias("discount_usd"),
            F.round(F.coalesce(F.col("items_total_local"), F.lit(0.0)) * F.col("usd_per_unit"), 2).alias(
                "lines_total_usd"
            ),
            # header total vs the lines that survived validation; false => some lines were quarantined
            (
                F.abs(F.coalesce(F.col("items_total_local"), F.lit(0.0)) - F.col("order_total"))
                <= F.greatest(F.lit(1.0), F.col("order_total") * 0.01)
            ).alias("lines_reconciled"),
            F.coalesce("item_count", F.lit(0)).cast("int").alias("item_count"),
            F.coalesce("total_quantity", F.lit(0)).cast("int").alias("total_quantity"),
            "paid_at",
            F.round(F.coalesce("captured_amount_local", F.lit(0.0)) * F.col("usd_per_unit"), 2).alias(
                "captured_amount_usd"
            ),
            F.round(F.coalesce("refunded_amount_local", F.lit(0.0)) * F.col("usd_per_unit"), 2).alias(
                "refunded_amount_usd"
            ),
            F.coalesce("failed_payment_attempts", F.lit(0)).cast("int").alias("failed_payment_attempts"),
            (F.col("status") == "cancelled").alias("is_cancelled"),
            (F.col("status") == "returned").alias("is_returned"),
            F.col("status").isin("paid", "shipped", "delivered").alias("is_revenue"),
            "customer_order_seq",
            "days_since_prev_order",
            (F.col("customer_order_seq") == 1).alias("is_first_order"),
            "updated_at",
            "_batch_id",
        )
    )
    return write_gold(df, "fact_orders", "order_month", _mode(months), settings, t0)


# ---------------------------------------------------------- fact_order_items
def fact_order_items(
    spark: SparkSession, months: list[str] | None = None, settings: Settings | None = None
) -> GoldResult:
    settings = settings or get_settings()
    t0 = time.time()
    items = silver(spark, "order_items", months, settings)
    orders = silver(spark, "orders", months, settings).select(
        "order_id", "customer_id", "region_code", "order_date", "order_ts", "status", "currency"
    )
    products = silver(spark, "products", settings=settings).select(
        "product_id", "category_id", "supplier_id", F.col("unit_cost").alias("unit_cost_usd")
    )
    fx = (
        dense_fx_rates(spark, settings)
        .withColumnRenamed("rate_date", "order_date")
        .withColumnRenamed("currency_code", "currency")
    )
    df = (
        items.drop("currency")
        .join(orders, "order_id", "inner")
        .join(F.broadcast(products), "product_id", "left")
        .join(F.broadcast(fx), ["order_date", "currency"], "left")
        .select(
            skey("order_item_id").alias("order_item_key"),
            "order_item_id",
            skey("order_id").alias("order_key"),
            "order_id",
            skey("product_id").alias("product_key"),
            "product_id",
            skey("category_id").alias("category_key"),
            skey("supplier_id").alias("supplier_key"),
            skey("customer_id").alias("customer_key"),
            skey("region_code").alias("region_key"),
            date_key("order_date").alias("order_date_key"),
            "order_date",
            "order_month",
            "order_ts",
            "status",
            "currency",
            "quantity",
            F.col("unit_price").alias("unit_price_local"),
            "discount_pct",
            F.col("line_total").alias("line_total_local"),
            F.col("usd_per_unit").alias("fx_usd_per_unit"),
            F.round(F.col("unit_price") * F.col("usd_per_unit"), 4).alias("unit_price_usd"),
            F.round(F.col("line_total") * F.col("usd_per_unit"), 2).alias("line_total_usd"),
            "unit_cost_usd",
            F.round(F.col("quantity") * F.col("unit_cost_usd"), 2).alias("line_cost_usd"),
            F.round(
                F.col("line_total") * F.col("usd_per_unit") - F.col("quantity") * F.col("unit_cost_usd"), 2
            ).alias("gross_margin_usd"),
            F.col("status").isin("paid", "shipped", "delivered").alias("is_revenue"),
            (F.col("status") == "returned").alias("is_returned"),
            F.array_contains("quality_warnings", "order_items.line_total_consistent").alias(
                "line_total_recomputed"
            ),
            "_batch_id",
        )
    )
    return write_gold(df, "fact_order_items", "order_month", _mode(months), settings, t0)


# ------------------------------------------------------------ fact_inventory
def fact_inventory(
    spark: SparkSession, months: list[str] | None = None, settings: Settings | None = None
) -> GoldResult:
    settings = settings or get_settings()
    t0 = time.time()
    ev = silver(spark, "inventory_events", settings=settings)  # full history for the running balance
    w = (
        Window.partitionBy("product_id", "warehouse_id")
        .orderBy("event_ts", "event_id")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    ev = ev.withColumn("on_hand_after", F.sum("quantity_delta").over(w))
    ev = _restrict(ev, "event_month", months)
    df = ev.select(
        skey("event_id").alias("inventory_event_key"),
        "event_id",
        skey("product_id").alias("product_key"),
        "product_id",
        skey("warehouse_id").alias("warehouse_key"),
        "warehouse_id",
        skey("supplier_id").alias("supplier_key"),
        "supplier_id",
        date_key("event_date").alias("event_date_key"),
        "event_ts",
        "event_date",
        "event_month",
        "event_type",
        "quantity_delta",
        "on_hand_after",
        "unit_cost",
        F.round(F.col("quantity_delta") * F.col("unit_cost"), 2).alias("receipt_value_usd"),
        "defective_qty",
        "counted_quantity",
        "reference_id",
        "reason",
        "_batch_id",
    )
    return write_gold(df, "fact_inventory", "event_month", _mode(months), settings, t0)


# ------------------------------------------------------------- fact_shipping
def fact_shipping(
    spark: SparkSession, months: list[str] | None = None, settings: Settings | None = None
) -> GoldResult:
    """One row per shipment: the tracker event stream is pivoted into lifecycle timestamps."""
    settings = settings or get_settings()
    t0 = time.time()
    ev = silver(spark, "shipping_events", settings=settings)
    orders = silver(spark, "orders", settings=settings).select(
        "order_id", "customer_id", "order_month", "order_date"
    )

    def first_ts(event: str) -> F.Column:
        return F.min(F.when(F.col("event") == event, F.col("event_ts")))

    ship = ev.groupBy("shipment_id", "order_id").agg(
        F.first("carrier", ignorenulls=True).alias("carrier"),
        F.first("warehouse_id", ignorenulls=True).alias("warehouse_id"),
        F.first("region_code", ignorenulls=True).alias("region_code"),
        first_ts("label_created").alias("label_created_ts"),
        first_ts("picked_up").alias("picked_up_ts"),
        first_ts("in_transit").alias("in_transit_ts"),
        first_ts("out_for_delivery").alias("out_for_delivery_ts"),
        first_ts("delivered").alias("delivered_ts"),
        first_ts("delivery_exception").alias("exception_ts"),
        first_ts("returned").alias("returned_ts"),
        F.count("*").alias("event_count"),
    )
    df = ship.join(orders, "order_id", "inner")
    delivery_hours = (F.unix_timestamp("delivered_ts") - F.unix_timestamp("label_created_ts")) / 3600.0
    df = (
        df.withColumn("delivery_hours", F.round(delivery_hours, 2))
        .withColumn(
            "timeline_inconsistent",
            F.col("delivered_ts").isNotNull()
            & F.col("label_created_ts").isNotNull()
            & (F.col("delivered_ts") < F.col("label_created_ts")),
        )
        .withColumn(
            "delivery_days", F.when(F.col("delivery_hours") >= 0, F.round(F.col("delivery_hours") / 24.0, 2))
        )
    )
    df = _restrict(df, "order_month", months)
    df = df.select(
        skey("shipment_id").alias("shipment_key"),
        "shipment_id",
        skey("order_id").alias("order_key"),
        "order_id",
        skey("customer_id").alias("customer_key"),
        skey("warehouse_id").alias("warehouse_key"),
        "warehouse_id",
        skey("region_code").alias("region_key"),
        "region_code",
        "carrier",
        "order_date",
        "order_month",
        date_key("label_created_ts").alias("ship_date_key"),
        date_key("delivered_ts").alias("delivered_date_key"),
        "label_created_ts",
        "picked_up_ts",
        "in_transit_ts",
        "out_for_delivery_ts",
        "delivered_ts",
        "exception_ts",
        "returned_ts",
        "delivery_hours",
        "delivery_days",
        F.col("delivered_ts").isNotNull().alias("is_delivered"),
        F.col("exception_ts").isNotNull().alias("had_exception"),
        F.col("returned_ts").isNotNull().alias("is_returned"),
        "timeline_inconsistent",
        "event_count",
    )
    return write_gold(df, "fact_shipping", "order_month", _mode(months), settings, t0)


FACTS = [fact_orders, fact_order_items, fact_inventory, fact_shipping]


def build_facts(
    spark: SparkSession, months: list[str] | None = None, settings: Settings | None = None
) -> dict[str, GoldResult]:
    return {fn.__name__: fn(spark, months, settings) for fn in FACTS}
