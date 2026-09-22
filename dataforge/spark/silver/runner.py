"""Silver job registry in dependency order."""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import SparkSession

from ...config import Settings
from .common import SilverResult
from .customers import silver_customers
from .inventory import silver_inventory_events
from .orders import silver_order_items, silver_orders, silver_payments
from .products import silver_products
from .reference import (
    silver_categories,
    silver_exchange_rates,
    silver_regions,
    silver_suppliers,
    silver_warehouses,
)
from .shipping import silver_shipping_events

SilverJob = Callable[[SparkSession, list[str], str, Settings | None], SilverResult]

# (dataset, job, upstream silver datasets it validates against)
SILVER_JOBS: list[tuple[str, SilverJob, list[str]]] = [
    ("regions", silver_regions, []),
    ("warehouses", silver_warehouses, []),
    ("categories", silver_categories, []),
    ("suppliers", silver_suppliers, []),
    ("exchange_rates", silver_exchange_rates, []),
    ("customers", silver_customers, ["regions"]),
    ("products", silver_products, ["categories", "suppliers"]),
    ("orders", silver_orders, ["customers"]),
    ("order_items", silver_order_items, ["orders", "products"]),
    ("payments", silver_payments, ["orders"]),
    ("inventory_events", silver_inventory_events, ["products", "warehouses"]),
    ("shipping_events", silver_shipping_events, ["orders", "warehouses"]),
]

JOB_BY_DATASET: dict[str, SilverJob] = {name: job for name, job, _ in SILVER_JOBS}


def run_silver(
    spark: SparkSession,
    batch_ids: list[str],
    run_id: str,
    datasets: list[str] | None = None,
    settings: Settings | None = None,
) -> dict[str, SilverResult]:
    out: dict[str, SilverResult] = {}
    for name, job, _ in SILVER_JOBS:
        if datasets and name not in datasets:
            continue
        out[name] = job(spark, batch_ids, run_id, settings)
    return out
