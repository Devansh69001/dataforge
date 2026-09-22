"""Source schema registry + drift detection.

The registry is the contract between producers and the platform. For each dataset it
lists the columns bronze expects. When a delivery deviates:

    new column          -> INFO  : kept in bronze (bronze is schema-on-read), surfaced in
                                   monitoring.schema_events, ignored by silver until the
                                   registry is updated. Nothing breaks, nothing is lost.
    missing optional    -> WARN  : silver fills with NULL.
    missing required    -> ERROR : ingestion of that file fails fast (SchemaDriftError);
                                   nothing downstream sees a half-usable dataset.

The registry itself is versioned in git, so "accepting" a new column is a reviewed change.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class DatasetSchema:
    dataset: str
    source: str  # human-readable source system
    format: str
    required: tuple[str, ...]
    optional: tuple[str, ...] = ()
    business_key: tuple[str, ...] = ()
    version_column: str | None = None  # column used to pick the latest record on dedup
    description: str = ""

    @property
    def known(self) -> set[str]:
        return set(self.required) | set(self.optional)


REGISTRY: dict[str, DatasetSchema] = {
    "customers": DatasetSchema(
        dataset="customers",
        source="CRM export",
        format="csv",
        required=(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "signup_date",
            "country",
            "region_code",
            "updated_at",
        ),
        optional=("phone", "birth_date", "customer_segment", "city", "street_address", "marketing_opt_in"),
        business_key=("customer_id",),
        version_column="updated_at",
        description="Customer master data; one row per customer version.",
    ),
    "products": DatasetSchema(
        dataset="products",
        source="PIM product catalog feed",
        format="json",
        required=(
            "product_id",
            "sku",
            "name",
            "category_id",
            "supplier_id",
            "unit_price",
            "unit_cost",
            "currency",
            "is_active",
            "updated_at",
        ),
        optional=("brand", "weight_kg", "dimensions_cm", "attributes", "created_at"),
        business_key=("product_id",),
        version_column="updated_at",
        description="Product catalog; nested dimensions/attributes kept as JSON text in bronze.",
    ),
    "categories": DatasetSchema(
        dataset="categories",
        source="PIM product catalog feed",
        format="json",
        required=("category_id", "category_name", "level"),
        optional=("parent_category_id",),
        business_key=("category_id",),
    ),
    "suppliers": DatasetSchema(
        dataset="suppliers",
        source="PIM product catalog feed",
        format="json",
        required=("supplier_id", "supplier_name", "country_code"),
        optional=("region_code", "lead_time_days", "quality_tier", "active"),
        business_key=("supplier_id",),
    ),
    "orders": DatasetSchema(
        dataset="orders",
        source="Order management system export",
        format="csv",
        required=("order_id", "customer_id", "order_date", "status", "currency", "order_total", "updated_at"),
        optional=("channel", "region_code", "shipping_country", "coupon_code"),
        business_key=("order_id",),
        version_column="updated_at",
        description="Order headers; re-delivered when status changes (late-arriving updates).",
    ),
    "order_items": DatasetSchema(
        dataset="order_items",
        source="Order management system export",
        format="csv",
        required=("order_item_id", "order_id", "product_id", "quantity", "unit_price", "line_total"),
        optional=("discount_pct", "currency"),
        business_key=("order_item_id",),
    ),
    "payments": DatasetSchema(
        dataset="payments",
        source="Payment service export",
        format="csv",
        required=("payment_id", "order_id", "payment_method", "amount", "currency", "status", "paid_at"),
        business_key=("payment_id",),
    ),
    "inventory_events": DatasetSchema(
        dataset="inventory_events",
        source="Warehouse management system event stream",
        format="ndjson",
        required=("event_id", "event_type", "product_id", "warehouse_id", "quantity_delta", "event_ts"),
        optional=(
            "reference_id",
            "supplier_id",
            "unit_cost",
            "defective_qty",
            "counted_quantity",
            "reason",
            "source_system",
        ),
        business_key=("event_id",),
    ),
    "shipping_events": DatasetSchema(
        dataset="shipping_events",
        source="Shipping tracker application logs",
        format="log",
        required=("event_ts", "event", "shipment_id", "order_id"),
        optional=("level", "component", "carrier", "warehouse_id", "region", "location"),
        business_key=("shipment_id", "event", "event_ts"),
    ),
    "regions": DatasetSchema(
        dataset="regions",
        source="Reference API",
        format="api",
        required=("region_code", "region_name", "country_code", "country_name", "currency_code"),
        optional=("base_delivery_days", "timezone"),
        business_key=("region_code",),
    ),
    "warehouses": DatasetSchema(
        dataset="warehouses",
        source="Reference API",
        format="api",
        required=("warehouse_id", "warehouse_name", "region_code"),
        optional=("city", "capacity_units", "opened_date"),
        business_key=("warehouse_id",),
    ),
    "exchange_rates": DatasetSchema(
        dataset="exchange_rates",
        source="Reference API",
        format="api",
        required=("rate_date", "currency_code", "usd_per_unit"),
        business_key=("rate_date", "currency_code"),
    ),
}


class SchemaDriftError(Exception):
    pass


@dataclass
class SchemaEvent:
    dataset: str
    change_type: str  # new_column | missing_required | missing_optional
    column: str
    severity: str  # INFO | WARN | ERROR
    batch_id: str | None = None
    source_file: str | None = None
    detected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def detect_drift(
    dataset: str, observed: list[str], batch_id: str | None = None, source_file: str | None = None
) -> list[SchemaEvent]:
    schema = REGISTRY[dataset]
    obs = set(observed)
    events: list[SchemaEvent] = []
    for c in schema.required:
        if c not in obs:
            events.append(SchemaEvent(dataset, "missing_required", c, "ERROR", batch_id, source_file))
    for c in schema.optional:
        if c not in obs:
            events.append(SchemaEvent(dataset, "missing_optional", c, "WARN", batch_id, source_file))
    for c in sorted(obs - schema.known):
        events.append(SchemaEvent(dataset, "new_column", c, "INFO", batch_id, source_file))
    return events


def raise_on_breaking(events: list[SchemaEvent]) -> None:
    breaking = [e for e in events if e.severity == "ERROR"]
    if breaking:
        cols = ", ".join(e.column for e in breaking)
        raise SchemaDriftError(f"{breaking[0].dataset}: required column(s) missing from delivery: {cols}")
