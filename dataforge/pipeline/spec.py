"""The pipeline graph as data.

Both the CLI runner and the Airflow DAG are generated from this spec, so the task
names, ordering and dependencies are tested once and cannot drift apart.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class TaskSpec:
    name: str
    description: str
    upstream: tuple[str, ...] = ()
    group: str | None = None
    retries: int = 1
    tags: tuple[str, ...] = field(default_factory=tuple)


PIPELINE: list[TaskSpec] = [
    TaskSpec(
        "check_sources",
        "Verify every raw source for the batch is present and the reference API is reachable",
        retries=3,
    ),
    TaskSpec(
        "init_warehouse", "Apply idempotent warehouse DDL (schemas, dims, facts, monitoring)", retries=2
    ),
    TaskSpec(
        "ingest_customers",
        "Land customers CSV in bronze",
        ("check_sources", "init_warehouse"),
        group="ingest",
    ),
    TaskSpec(
        "ingest_catalog",
        "Land product catalog JSON (products, categories, suppliers) in bronze",
        ("check_sources", "init_warehouse"),
        group="ingest",
    ),
    TaskSpec(
        "ingest_orders",
        "Land orders, order items and payments CSVs in bronze",
        ("check_sources", "init_warehouse"),
        group="ingest",
    ),
    TaskSpec(
        "ingest_inventory",
        "Land inventory NDJSON events in bronze (malformed lines -> quarantine)",
        ("check_sources", "init_warehouse"),
        group="ingest",
    ),
    TaskSpec(
        "ingest_shipping",
        "Parse shipping tracker logs into bronze (malformed lines -> quarantine)",
        ("check_sources", "init_warehouse"),
        group="ingest",
    ),
    TaskSpec(
        "ingest_reference",
        "Pull regions, warehouses and exchange rates from the reference API",
        ("check_sources", "init_warehouse"),
        group="ingest",
    ),
    TaskSpec(
        "detect_schema_drift",
        "Aggregate schema events from all ingestions; fail on missing required columns",
        (
            "ingest_customers",
            "ingest_catalog",
            "ingest_orders",
            "ingest_inventory",
            "ingest_shipping",
            "ingest_reference",
        ),
    ),
    TaskSpec(
        "silver_reference", "Spark: type and dedupe reference data", ("detect_schema_drift",), group="silver"
    ),
    TaskSpec(
        "silver_customers",
        "Spark: validate, normalise and dedupe customers",
        ("silver_reference",),
        group="silver",
    ),
    TaskSpec(
        "silver_products",
        "Spark: flatten and validate the product catalog",
        ("silver_reference",),
        group="silver",
    ),
    TaskSpec(
        "silver_orders",
        "Spark: validate orders against customers; late-arriving updates",
        ("silver_customers",),
        group="silver",
    ),
    TaskSpec(
        "silver_order_items",
        "Spark: validate order lines against orders and products",
        ("silver_orders", "silver_products"),
        group="silver",
    ),
    TaskSpec(
        "silver_payments", "Spark: validate payments against orders", ("silver_orders",), group="silver"
    ),
    TaskSpec(
        "silver_inventory",
        "Spark: validate inventory events against products and warehouses",
        ("silver_products",),
        group="silver",
    ),
    TaskSpec(
        "silver_shipping",
        "Spark: validate shipping events against orders",
        ("silver_orders",),
        group="silver",
    ),
    TaskSpec(
        "gold_dimensions",
        "Spark: rebuild dimensions",
        (
            "silver_customers",
            "silver_products",
            "silver_inventory",
            "silver_shipping",
            "silver_order_items",
            "silver_payments",
        ),
        group="gold",
    ),
    TaskSpec(
        "gold_facts",
        "Spark: build facts and aggregates for the touched months",
        ("gold_dimensions",),
        group="gold",
    ),
    TaskSpec("load_dimensions", "Upsert dimensions into PostgreSQL", ("gold_facts",), group="load"),
    TaskSpec(
        "load_facts",
        "Upsert facts and aggregates into PostgreSQL (touched partitions only)",
        ("load_dimensions",),
        group="load",
    ),
    TaskSpec("dbt_run", "dbt run: staging -> intermediate -> marts", ("load_facts",), group="dbt"),
    TaskSpec("dbt_test", "dbt test: schema and data tests on the marts", ("dbt_run",), group="dbt"),
    TaskSpec(
        "warehouse_quality_checks",
        "Uniqueness, referential integrity, ranges, null rates and freshness on the warehouse",
        ("load_facts",),
    ),
    TaskSpec(
        "publish",
        "Advance watermarks and mark the run as published",
        ("dbt_test", "warehouse_quality_checks"),
    ),
]

TASK_BY_NAME = {t.name: t for t in PIPELINE}


def topological_order() -> list[str]:
    done: list[str] = []
    remaining = list(PIPELINE)
    while remaining:
        progressed = False
        for t in list(remaining):
            if all(u in done for u in t.upstream):
                done.append(t.name)
                remaining.remove(t)
                progressed = True
        if not progressed:
            raise ValueError("cycle in pipeline spec")
    return done


def validate_spec() -> None:
    names = [t.name for t in PIPELINE]
    if len(names) != len(set(names)):
        raise ValueError("duplicate task names")
    for t in PIPELINE:
        for u in t.upstream:
            if u not in TASK_BY_NAME:
                raise ValueError(f"{t.name}: unknown upstream {u}")
    topological_order()
