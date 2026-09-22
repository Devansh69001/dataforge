# Data quality strategy

The rule-by-rule catalogue is generated from the code into
[`data_quality_rules.md`](data_quality_rules.md) (`python scripts/export_quality_rules.py`).
This document explains the design behind it.

## Where quality is enforced

| Stage | What is checked | On failure |
|---|---|---|
| **Ingestion (parse)** | Can the line be parsed at all? (JSON syntax, log envelope, required log keys, CSV structure) | The line goes to quarantine with `stage="parse"`. The rest of the file still lands. |
| **Ingestion (schema)** | Does the delivery match the source schema registry? | Missing **required** column -> the file is rejected (`SchemaDriftError`). Missing optional / new column -> logged to `monitoring.schema_events`, ingestion continues. |
| **Silver (row rules)** | 76 declarative rules: types, nulls, ranges, enums, referential integrity, date validity, cross-column consistency | `reject` rules quarantine the row (`stage="validate"`); `warn` rules keep it and flag `quality_warnings`. |
| **Silver (dataset)** | Reject rate vs the dataset's circuit breaker | `QualityThresholdError` aborts the run; nothing downstream is published. |
| **Warehouse (post-load)** | 27 checks: uniqueness, referential integrity, value ranges, null rates, reconciliation, freshness | `error` severity fails the run; `warn` is recorded. |
| **dbt** | 68 tests: unique, not_null, relationships, accepted values, accepted range, plus two custom business assertions | A failing test fails `dbt_test`, so the run never reaches `publish`. |

## Why quarantine instead of deletion

Dropping bad rows destroys the evidence of a producer problem and silently changes the
numbers. Every rejected row is written as one JSON line:

```json
{
  "record": {"order_item_id": "OI-00012345", "order_id": "ORD-0001234", "quantity": "-3", "_source_file": "orders/order_items_2025-11-30.csv", "_row_number": 84213},
  "dataset": "order_items",
  "source": "orders/order_items_2025-11-30.csv",
  "batch_id": "2025-11-30",
  "stage": "validate",
  "rule_ids": ["order_items.quantity_positive"],
  "errors": ["quantity must be greater than zero"],
  "detected_at": "2026-09-22T09:14:02.118+00:00",
  "run_id": "final_initial"
}
```

That is enough to (a) find the offending line in the source file, (b) tell the producer
exactly which rule it broke, and (c) replay the record after a fix. The counts per rule
land in `monitoring.quality_results` and drive the dashboard's "quality rule hits" panel.

## Severity: reject vs warn

The question is always *"can the business fact survive this defect?"*

* **reject** - the row cannot be interpreted or would corrupt a measure: unparseable dates,
  non-numeric prices, negative quantities, unknown foreign keys, unknown enum values,
  missing primary keys, a receipt with a negative quantity.
* **warn** - the row is still a valid business fact, one attribute is unreliable:
  a malformed email (nulled), a product whose `dimensions_cm` could not be parsed
  (dimensions nulled), a line total that disagrees with `qty x price` (recomputed),
  a missing `region_code` (derived from the customer), a payment amount that differs
  from the order total.

A bad contact field must never erase a customer's order history; a negative quantity must
never reach a revenue number. That distinction is the whole rule design.

## Cascade: children follow their parents

Referential integrity is enforced against the **silver** parent, not the raw file. If an
order is quarantined (say, an orphan customer), its lines, payments and shipping events
fail `*.order_known` and are quarantined too. Facts therefore never contain half an order.
This is also why child datasets have a wider circuit breaker (10 % vs 5 %): their reject
rate includes inherited rejections.

## Thresholds and how they were chosen

| Threshold | Value | Reasoning |
|---|---|---|
| `customers`, `products`, `orders` max reject rate | 5 % | These are primary feeds; a higher rate means the export itself is broken, and publishing would distort every downstream number. Observed on the demo data: 0.2 % / 1.8 % / 1.6 %. |
| `order_items`, `payments`, `shipping_events` max reject rate | 10 % | Inherit parent rejections (cascade). Observed: 6.4 % / 2.9 % / 3.2 %. |
| `inventory_events` max reject rate | 10 % | Same cascade effect via products/warehouses. Observed: 4.1 %. |
| Warehouse uniqueness / RI / ranges | 0 failures | These are invariants of the model; one violation is a bug, not noise. |
| `fact_orders.region_key` null rate | < 2 % | Region is derived from the customer when missing; a higher rate means the derivation stopped working. |
| `dim_customer.email` null rate | < 5 % | Emails are genuinely missing sometimes; a spike means the CRM export changed. |
| Header/lines reconciliation | >= 90 % of revenue orders | An order's header total is authoritative; a gap means some lines were quarantined. The demo defect profile yields ~8.5 % unreconciled, so 10 % is the alarm point. |
| Freshness: newest order | within 2 days of the batch cutoff | Catches a stale or partially delivered batch. |
| Freshness: load recency | within 24 hours | Catches a pipeline that stopped running. |

Thresholds are deliberately *above* the observed defect rate and *below* "the source is
broken" - they are alarms, not targets.

## Uniqueness and duplicate handling

Three different duplicate problems, three different mechanisms:

1. **Byte-identical file re-delivered** - the ingestion ledger (sha256 per file) skips it.
2. **Identical row repeated inside a file** (at-least-once producers) - `drop_exact_duplicates`
   removes it before validation; the count is reported as `exact_duplicates_removed`.
3. **Same business key, different content** (a late status update, a corrected record) -
   `keep_latest` orders by the dataset's version column (`updated_at`), then by ingestion
   time, and keeps one row per key.

After silver, `order_id`, `order_item_id`, `event_id`, `customer_id`, `product_id` are unique
by construction; the warehouse checks assert it again after loading (0 duplicates allowed,
i.e. > 99.99 % uniqueness in the spec's terms - the measured value is exactly 100 %).

## Freshness

`monitoring.watermarks` records the last successfully published batch and the maximum
business timestamp per dataset. The warehouse checks compare the newest `order_date` with
the batch cutoff, and `_loaded_at` with wall-clock time. The API exposes both through
`/pipeline/status`.
