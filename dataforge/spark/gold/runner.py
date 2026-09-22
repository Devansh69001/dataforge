"""Gold build orchestration: dimensions -> facts -> aggregates, scoped to touched months."""

from __future__ import annotations

from pyspark.sql import SparkSession

from ...config import Settings, get_settings
from ...logging_utils import get_logger
from ..silver.common import read_silver
from .aggregates import agg_inventory_position, agg_product_daily_sales
from .common import GoldResult
from .dimensions import build_dimensions
from .facts import fact_inventory, fact_order_items, fact_orders, fact_shipping

log = get_logger("spark.gold")

GOLD_TABLES = [
    "dim_date",
    "dim_region",
    "dim_category",
    "dim_supplier",
    "dim_warehouse",
    "dim_customer",
    "dim_product",
    "fact_orders",
    "fact_order_items",
    "fact_inventory",
    "fact_shipping",
    "agg_product_daily_sales",
    "agg_inventory_position",
]


def _union(*lists: list[str] | None) -> list[str] | None:
    out: set[str] = set()
    for lst in lists:
        if lst:
            out.update(lst)
    return sorted(out) or None


def shipping_order_months(
    spark: SparkSession, event_months: list[str] | None, settings: Settings
) -> list[str] | None:
    """Order months affected by shipping events in the given event months."""
    if not event_months:
        return None
    ev = read_silver(spark, "shipping_events", event_months, settings)
    orders = read_silver(spark, "orders", settings=settings)
    if ev is None or orders is None:
        return None
    rows = (
        ev.select("order_id")
        .distinct()
        .join(orders.select("order_id", "order_month"), "order_id")
        .select("order_month")
        .distinct()
        .collect()
    )
    return sorted(r[0] for r in rows if r[0])


def run_gold(
    spark: SparkSession, touched: dict[str, list[str]] | None = None, settings: Settings | None = None
) -> dict[str, GoldResult]:
    """`touched` maps silver dataset -> partitions written in this run (None = full rebuild)."""
    settings = settings or get_settings()
    t = touched or {}
    full = touched is None
    results: dict[str, GoldResult] = {}
    results.update(build_dimensions(spark, settings))

    order_months = None if full else _union(t.get("orders"), t.get("order_items"), t.get("payments"))
    inv_months = None if full else _union(t.get("inventory_events"))
    ship_months = (
        None
        if full
        else _union(t.get("orders"), shipping_order_months(spark, t.get("shipping_events"), settings))
    )
    log.info(
        "gold scope",
        full_rebuild=full,
        order_months=order_months,
        inventory_months=inv_months,
        shipping_months=ship_months,
    )

    results["fact_orders"] = fact_orders(spark, order_months, settings)
    results["fact_order_items"] = fact_order_items(spark, order_months, settings)
    results["fact_inventory"] = fact_inventory(spark, inv_months, settings)
    results["fact_shipping"] = fact_shipping(spark, ship_months, settings)
    results["agg_product_daily_sales"] = agg_product_daily_sales(spark, order_months, settings)
    results["agg_inventory_position"] = agg_inventory_position(spark, settings)
    return results
