-- Facts. The large event-style facts are declaratively RANGE-partitioned by month on
-- their business date:
--   * incremental loads touch only the partitions of the months in the batch
--   * time-bounded analytics (monthly revenue, last-90-days velocity) prune partitions
--   * retention can drop a month with DROP TABLE instead of DELETE
-- Monthly partitions are created on demand by the loader (warehouse.ensure_month_partitions).
-- Upserts on a partitioned table require the partition key inside the unique constraint,
-- hence the composite primary keys.

CREATE TABLE IF NOT EXISTS warehouse.fact_orders (
    order_key               BIGINT NOT NULL,
    order_id                TEXT NOT NULL,
    customer_key            BIGINT NOT NULL,
    customer_id             TEXT NOT NULL,
    region_key              BIGINT,
    region_code             TEXT,
    order_date_key          INTEGER NOT NULL,
    order_ts                TIMESTAMPTZ NOT NULL,
    order_date              DATE NOT NULL,
    order_month             TEXT NOT NULL,
    status                  TEXT NOT NULL,
    channel                 TEXT,
    currency                TEXT NOT NULL,
    coupon_code             TEXT,
    order_total_local       NUMERIC(14,2),
    fx_usd_per_unit         NUMERIC(14,6),
    order_total_usd         NUMERIC(14,2),
    gross_amount_usd        NUMERIC(14,2),
    discount_usd            NUMERIC(14,2),
    lines_total_usd         NUMERIC(14,2),
    lines_reconciled        BOOLEAN,
    item_count              INTEGER,
    total_quantity          INTEGER,
    paid_at                 TIMESTAMPTZ,
    captured_amount_usd     NUMERIC(14,2),
    refunded_amount_usd     NUMERIC(14,2),
    failed_payment_attempts INTEGER,
    is_cancelled            BOOLEAN,
    is_returned             BOOLEAN,
    is_revenue              BOOLEAN,
    customer_order_seq      INTEGER,
    days_since_prev_order   INTEGER,
    is_first_order          BOOLEAN,
    updated_at              TIMESTAMPTZ,
    _batch_id               TEXT,
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_key, order_date)
) PARTITION BY RANGE (order_date);

CREATE TABLE IF NOT EXISTS warehouse.fact_order_items (
    order_item_key          BIGINT NOT NULL,
    order_item_id           TEXT NOT NULL,
    order_key               BIGINT NOT NULL,
    order_id                TEXT NOT NULL,
    product_key             BIGINT NOT NULL,
    product_id              TEXT NOT NULL,
    category_key            BIGINT,
    supplier_key            BIGINT,
    customer_key            BIGINT NOT NULL,
    region_key              BIGINT,
    order_date_key          INTEGER NOT NULL,
    order_date              DATE NOT NULL,
    order_month             TEXT NOT NULL,
    order_ts                TIMESTAMPTZ,
    status                  TEXT,
    currency                TEXT,
    quantity                INTEGER NOT NULL,
    unit_price_local        NUMERIC(14,2),
    discount_pct            NUMERIC(6,2),
    line_total_local        NUMERIC(14,2),
    fx_usd_per_unit         NUMERIC(14,6),
    unit_price_usd          NUMERIC(14,4),
    line_total_usd          NUMERIC(14,2),
    unit_cost_usd           NUMERIC(14,2),
    line_cost_usd           NUMERIC(14,2),
    gross_margin_usd        NUMERIC(14,2),
    is_revenue              BOOLEAN,
    is_returned             BOOLEAN,
    line_total_recomputed   BOOLEAN,
    _batch_id               TEXT,
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_item_key, order_date)
) PARTITION BY RANGE (order_date);

CREATE TABLE IF NOT EXISTS warehouse.fact_inventory (
    inventory_event_key BIGINT NOT NULL,
    event_id            TEXT NOT NULL,
    product_key         BIGINT NOT NULL,
    product_id          TEXT NOT NULL,
    warehouse_key       BIGINT NOT NULL,
    warehouse_id        TEXT NOT NULL,
    supplier_key        BIGINT,
    supplier_id         TEXT,
    event_date_key      INTEGER NOT NULL,
    event_ts            TIMESTAMPTZ NOT NULL,
    event_date          DATE NOT NULL,
    event_month         TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    quantity_delta      INTEGER NOT NULL,
    on_hand_after       BIGINT,
    unit_cost           NUMERIC(12,2),
    receipt_value_usd   NUMERIC(14,2),
    defective_qty       INTEGER,
    counted_quantity    INTEGER,
    reference_id        TEXT,
    reason              TEXT,
    _batch_id           TEXT,
    _loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (inventory_event_key, event_date)
) PARTITION BY RANGE (event_date);

CREATE TABLE IF NOT EXISTS warehouse.fact_shipping (
    shipment_key        BIGINT NOT NULL,
    shipment_id         TEXT NOT NULL,
    order_key           BIGINT NOT NULL,
    order_id            TEXT NOT NULL,
    customer_key        BIGINT,
    warehouse_key       BIGINT,
    warehouse_id        TEXT,
    region_key          BIGINT,
    region_code         TEXT,
    carrier             TEXT,
    order_date          DATE NOT NULL,
    order_month         TEXT NOT NULL,
    ship_date_key       INTEGER,
    delivered_date_key  INTEGER,
    label_created_ts    TIMESTAMPTZ,
    picked_up_ts        TIMESTAMPTZ,
    in_transit_ts       TIMESTAMPTZ,
    out_for_delivery_ts TIMESTAMPTZ,
    delivered_ts        TIMESTAMPTZ,
    exception_ts        TIMESTAMPTZ,
    returned_ts         TIMESTAMPTZ,
    delivery_hours      NUMERIC(10,2),
    delivery_days       NUMERIC(8,2),
    is_delivered        BOOLEAN,
    had_exception       BOOLEAN,
    is_returned         BOOLEAN,
    timeline_inconsistent BOOLEAN,
    event_count         INTEGER,
    _loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (shipment_key, order_date)
) PARTITION BY RANGE (order_date);

CREATE TABLE IF NOT EXISTS warehouse.agg_product_daily_sales (
    product_key         BIGINT NOT NULL,
    product_id          TEXT NOT NULL,
    category_key        BIGINT,
    supplier_key        BIGINT,
    order_date          DATE NOT NULL,
    order_month         TEXT NOT NULL,
    date_key            INTEGER NOT NULL,
    orders              INTEGER,
    units_sold          INTEGER,
    revenue_usd         NUMERIC(14,2),
    gross_margin_usd    NUMERIC(14,2),
    unique_customers    INTEGER,
    _loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (product_key, order_date)
) PARTITION BY RANGE (order_date);

CREATE TABLE IF NOT EXISTS warehouse.agg_inventory_position (
    position_key        BIGINT PRIMARY KEY,
    product_key         BIGINT NOT NULL,
    product_id          TEXT NOT NULL,
    warehouse_key       BIGINT NOT NULL,
    warehouse_id        TEXT NOT NULL,
    on_hand_units       INTEGER,
    avg_unit_cost_usd   NUMERIC(14,4),
    inventory_value_usd NUMERIC(14,2),
    units_received      BIGINT,
    units_shipped       BIGINT,
    units_defective     BIGINT,
    last_receipt_ts     TIMESTAMPTZ,
    last_shipment_ts    TIMESTAMPTZ,
    last_event_ts       TIMESTAMPTZ,
    _loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Secondary indexes on the partitioned facts are propagated to every partition.
CREATE INDEX IF NOT EXISTS ix_fact_orders_customer ON warehouse.fact_orders (customer_key);
CREATE INDEX IF NOT EXISTS ix_fact_orders_region ON warehouse.fact_orders (region_key);
CREATE INDEX IF NOT EXISTS ix_fact_items_product ON warehouse.fact_order_items (product_key);
CREATE INDEX IF NOT EXISTS ix_fact_items_order ON warehouse.fact_order_items (order_key);
CREATE INDEX IF NOT EXISTS ix_fact_inventory_product_wh ON warehouse.fact_inventory (product_key, warehouse_key);
CREATE INDEX IF NOT EXISTS ix_fact_shipping_region ON warehouse.fact_shipping (region_key);
CREATE INDEX IF NOT EXISTS ix_agg_pds_product ON warehouse.agg_product_daily_sales (product_key);

-- Creates the monthly partition covering `month_start` for a partitioned fact if missing.
CREATE OR REPLACE FUNCTION warehouse.ensure_month_partition(parent regclass, month_start date)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    part_name text;
    start_d date := date_trunc('month', month_start)::date;
    end_d date := (date_trunc('month', month_start) + interval '1 month')::date;
BEGIN
    part_name := replace(parent::text, 'warehouse.', '') || '_' || to_char(start_d, 'YYYYMM');
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'warehouse' AND c.relname = part_name
    ) THEN
        EXECUTE format('CREATE TABLE warehouse.%I PARTITION OF %s FOR VALUES FROM (%L) TO (%L)', part_name, parent, start_d, end_d);
    END IF;
    RETURN part_name;
END $$;
