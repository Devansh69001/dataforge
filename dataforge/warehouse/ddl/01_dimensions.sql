-- Dimensions (SCD type 1). *_key columns are deterministic xxhash64 surrogate keys
-- computed in Spark; the natural business id is kept and unique.

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key        INTEGER PRIMARY KEY,
    full_date       DATE NOT NULL UNIQUE,
    year            SMALLINT NOT NULL,
    quarter         SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    month_name      TEXT NOT NULL,
    year_month      TEXT NOT NULL,
    week_of_year    SMALLINT NOT NULL,
    day_of_month    SMALLINT NOT NULL,
    day_of_week     SMALLINT NOT NULL,
    day_name        TEXT NOT NULL,
    is_weekend      BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_region (
    region_key          BIGINT PRIMARY KEY,
    region_code         TEXT NOT NULL UNIQUE,
    region_name         TEXT NOT NULL,
    country_code        TEXT NOT NULL,
    country_name        TEXT NOT NULL,
    currency_code       TEXT NOT NULL,
    base_delivery_days  NUMERIC(6,2)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_category (
    category_key            BIGINT PRIMARY KEY,
    category_id             TEXT NOT NULL UNIQUE,
    category_name           TEXT NOT NULL,
    parent_category_id      TEXT,
    parent_category_name    TEXT,
    level                   SMALLINT
);

CREATE TABLE IF NOT EXISTS warehouse.dim_supplier (
    supplier_key    BIGINT PRIMARY KEY,
    supplier_id     TEXT NOT NULL UNIQUE,
    supplier_name   TEXT NOT NULL,
    country_code    TEXT,
    region_code     TEXT,
    lead_time_days  INTEGER,
    quality_tier    TEXT,
    is_active       BOOLEAN
);

CREATE TABLE IF NOT EXISTS warehouse.dim_warehouse (
    warehouse_key   BIGINT PRIMARY KEY,
    warehouse_id    TEXT NOT NULL UNIQUE,
    warehouse_name  TEXT NOT NULL,
    city            TEXT,
    region_code     TEXT,
    region_key      BIGINT,
    capacity_units  INTEGER,
    opened_date     DATE
);

CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_key            BIGINT PRIMARY KEY,
    customer_id             TEXT NOT NULL UNIQUE,
    full_name               TEXT,
    email                   TEXT,
    customer_segment        TEXT,
    country_code            TEXT,
    region_code             TEXT,
    region_key              BIGINT,
    city                    TEXT,
    signup_date             DATE,
    birth_date              DATE,
    marketing_opt_in        BOOLEAN,
    updated_at              TIMESTAMPTZ,
    quality_warning_count   INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_key             BIGINT PRIMARY KEY,
    product_id              TEXT NOT NULL UNIQUE,
    sku                     TEXT,
    product_name            TEXT,
    brand                   TEXT,
    category_key            BIGINT,
    category_id             TEXT,
    category_name           TEXT,
    parent_category_name    TEXT,
    supplier_key            BIGINT,
    supplier_id             TEXT,
    supplier_name           TEXT,
    unit_price_usd          NUMERIC(12,2),
    unit_cost_usd           NUMERIC(12,2),
    unit_margin_usd         NUMERIC(12,2),
    weight_kg               NUMERIC(10,3),
    length_cm               NUMERIC(8,2),
    width_cm                NUMERIC(8,2),
    height_cm               NUMERIC(8,2),
    color                   TEXT,
    size                    TEXT,
    is_active               BOOLEAN,
    created_at              TIMESTAMPTZ,
    updated_at              TIMESTAMPTZ,
    has_negative_margin     BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS ix_dim_product_category ON warehouse.dim_product (category_key);
CREATE INDEX IF NOT EXISTS ix_dim_product_supplier ON warehouse.dim_product (supplier_key);
CREATE INDEX IF NOT EXISTS ix_dim_customer_region ON warehouse.dim_customer (region_key);
