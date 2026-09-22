"""Post-load quality suite executed against PostgreSQL.

These checks guard the published tables (what analysts and the API read), complementing
the row-level silver rules. Each check is one SQL statement returning `total` and `failed`;
the failure rate is compared with a threshold. Severity `error` fails the pipeline run,
`warn` is recorded and surfaced on the dashboard.

Thresholds (documented in docs/data_quality.md):
    uniqueness of business keys        0 duplicates allowed
    referential integrity              0 orphans allowed (facts -> dims)
    value ranges (qty > 0, price > 0)  0 violations allowed
    null rate of optional keys         < 2 %
    header/lines reconciliation        >= 90 % of revenue orders
    freshness                          newest order within 2 days of the batch cutoff;
                                       tables loaded within the last 24 hours
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..config import Settings, get_settings
from ..db import transaction


@dataclass(frozen=True)
class WarehouseCheck:
    id: str
    table: str
    severity: str  # error | warn
    description: str
    sql: str  # must return columns: total, failed
    max_failure_rate: float = 0.0
    params: dict[str, Any] = field(default_factory=dict)


def _uniq(id_: str, table: str, key: str) -> WarehouseCheck:
    return WarehouseCheck(
        id_,
        table,
        "error",
        f"{key} must be unique in {table}",
        f"SELECT count(*) AS total, count(*) - count(DISTINCT {key}) AS failed FROM warehouse.{table}",
    )


def _ri(id_: str, table: str, col: str, ref_table: str, ref_col: str) -> WarehouseCheck:
    return WarehouseCheck(
        id_,
        table,
        "error",
        f"{table}.{col} must reference an existing {ref_table}.{ref_col}",
        f"SELECT count(*) AS total, count(*) FILTER (WHERE f.{col} IS NOT NULL AND d.{ref_col} IS NULL) AS failed "
        f"FROM warehouse.{table} f LEFT JOIN warehouse.{ref_table} d ON d.{ref_col} = f.{col}",
    )


def _range(
    id_: str, table: str, predicate: str, description: str, severity: str = "error", max_rate: float = 0.0
) -> WarehouseCheck:
    return WarehouseCheck(
        id_,
        table,
        severity,
        description,
        f"SELECT count(*) AS total, count(*) FILTER (WHERE NOT ({predicate})) AS failed FROM warehouse.{table}",
        max_rate,
    )


def _null_rate(id_: str, table: str, col: str, max_rate: float, severity: str = "warn") -> WarehouseCheck:
    return WarehouseCheck(
        id_,
        table,
        severity,
        f"{table}.{col} null rate must stay below {max_rate:.0%}",
        f"SELECT count(*) AS total, count(*) FILTER (WHERE {col} IS NULL) AS failed FROM warehouse.{table}",
        max_rate,
    )


CHECKS: list[WarehouseCheck] = [
    # uniqueness
    _uniq("wh.fact_orders.order_id_unique", "fact_orders", "order_id"),
    _uniq("wh.fact_order_items.order_item_id_unique", "fact_order_items", "order_item_id"),
    _uniq("wh.fact_inventory.event_id_unique", "fact_inventory", "event_id"),
    _uniq("wh.fact_shipping.shipment_id_unique", "fact_shipping", "shipment_id"),
    _uniq("wh.dim_customer.customer_id_unique", "dim_customer", "customer_id"),
    _uniq("wh.dim_product.product_id_unique", "dim_product", "product_id"),
    # referential integrity
    _ri("wh.fact_orders.customer_ri", "fact_orders", "customer_key", "dim_customer", "customer_key"),
    _ri("wh.fact_orders.region_ri", "fact_orders", "region_key", "dim_region", "region_key"),
    _ri("wh.fact_order_items.product_ri", "fact_order_items", "product_key", "dim_product", "product_key"),
    _ri("wh.fact_order_items.order_ri", "fact_order_items", "order_key", "fact_orders", "order_key"),
    _ri("wh.fact_inventory.product_ri", "fact_inventory", "product_key", "dim_product", "product_key"),
    _ri(
        "wh.fact_inventory.warehouse_ri", "fact_inventory", "warehouse_key", "dim_warehouse", "warehouse_key"
    ),
    _ri("wh.fact_shipping.order_ri", "fact_shipping", "order_key", "fact_orders", "order_key"),
    _ri("wh.dim_product.category_ri", "dim_product", "category_key", "dim_category", "category_key"),
    _ri("wh.dim_product.supplier_ri", "dim_product", "supplier_key", "dim_supplier", "supplier_key"),
    # value ranges
    _range(
        "wh.fact_order_items.quantity_positive", "fact_order_items", "quantity > 0", "quantity must be > 0"
    ),
    _range(
        "wh.fact_order_items.unit_price_positive",
        "fact_order_items",
        "unit_price_usd > 0",
        "unit_price_usd must be > 0",
    ),
    _range(
        "wh.fact_orders.total_non_negative",
        "fact_orders",
        "order_total_usd >= 0",
        "order_total_usd must be >= 0",
    ),
    _range(
        "wh.fact_orders.fx_present",
        "fact_orders",
        "fx_usd_per_unit IS NOT NULL AND fx_usd_per_unit > 0",
        "every order must have an FX rate",
    ),
    _range(
        "wh.fact_shipping.delivery_days_non_negative",
        "fact_shipping",
        "delivery_days IS NULL OR delivery_days >= 0",
        "delivery_days must be >= 0",
    ),
    _range(
        "wh.dim_product.price_positive", "dim_product", "unit_price_usd > 0", "unit_price_usd must be > 0"
    ),
    _range(
        "wh.fact_orders.status_allowed",
        "fact_orders",
        "status IN ('placed','paid','shipped','delivered','cancelled','returned')",
        "status must be a known state",
    ),
    # null rates
    _null_rate("wh.fact_orders.region_null_rate", "fact_orders", "region_key", 0.02),
    _null_rate("wh.dim_customer.email_null_rate", "dim_customer", "email", 0.05),
    # reconciliation
    _range(
        "wh.fact_orders.lines_reconciled_rate",
        "fact_orders",
        "NOT is_revenue OR lines_reconciled",
        "revenue orders must reconcile with their lines (<= 10% unreconciled)",
        "warn",
        0.10,
    ),
]


def freshness_checks(batch_id: str | None) -> list[WarehouseCheck]:
    out = [
        WarehouseCheck(
            "wh.fact_orders.loaded_recently",
            "fact_orders",
            "warn",
            "fact_orders must have been loaded within the last 24 hours",
            "SELECT 1 AS total, CASE WHEN max(_loaded_at) >= now() - interval '24 hours' THEN 0 ELSE 1 END AS failed FROM warehouse.fact_orders",
        ),
    ]
    if batch_id:
        out.append(
            WarehouseCheck(
                "wh.fact_orders.covers_batch",
                "fact_orders",
                "error",
                f"newest order must be within 2 days of the batch cutoff {batch_id}",
                "SELECT 1 AS total, CASE WHEN max(order_date) >= %(batch)s::date - 2 THEN 0 ELSE 1 END AS failed FROM warehouse.fact_orders",
                params={"batch": batch_id},
            )
        )
    return out


def run_warehouse_checks(
    batch_id: str | None = None, settings: Settings | None = None
) -> list[dict[str, Any]]:
    settings = settings or get_settings()
    results = []
    with transaction(settings) as conn:
        for c in CHECKS + freshness_checks(batch_id):
            row = conn.execute(c.sql, c.params or None).fetchone()
            total, failed = int(row["total"] or 0), int(row["failed"] or 0)
            rate = failed / total if total else 0.0
            results.append(
                {
                    "rule_id": c.id,
                    "severity": c.severity,
                    "table": c.table,
                    "description": c.description,
                    "total_rows": total,
                    "failed_rows": failed,
                    "failure_rate": round(rate, 6),
                    "threshold": c.max_failure_rate,
                    "passed": rate <= c.max_failure_rate,
                }
            )
    return results


def summarize(results: list[dict[str, Any]]) -> str:
    if any(not r["passed"] and r["severity"] == "error" for r in results):
        return "fail"
    if any(not r["passed"] for r in results):
        return "warn"
    return "pass"
