"""Gold dimensions (SCD type 1, full rebuild each run - they are small)."""

from __future__ import annotations

import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ...config import Settings, get_settings
from .common import GoldResult, date_key, silver, skey, write_gold


def dim_date(
    spark: SparkSession, settings: Settings | None = None, start: str = "2023-01-01", end: str = "2026-12-31"
) -> GoldResult:
    settings = settings or get_settings()
    t0 = time.time()
    cal = spark.sql(
        f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS full_date"
    )
    df = cal.select(
        date_key("full_date").alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.date_format("full_date", "yyyy-MM").alias("year_month"),
        F.weekofyear("full_date").alias("week_of_year"),
        F.dayofmonth("full_date").alias("day_of_month"),
        F.dayofweek("full_date").alias("day_of_week"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
    )
    return write_gold(df, "dim_date", None, "overwrite", settings, t0)


def dim_region(spark: SparkSession, settings: Settings | None = None) -> GoldResult:
    settings = settings or get_settings()
    t0 = time.time()
    r = silver(spark, "regions", settings=settings)
    df = r.select(
        skey("region_code").alias("region_key"),
        "region_code",
        "region_name",
        "country_code",
        "country_name",
        "currency_code",
        "base_delivery_days",
    )
    return write_gold(df, "dim_region", None, "overwrite", settings, t0)


def dim_category(spark: SparkSession, settings: Settings | None = None) -> GoldResult:
    settings = settings or get_settings()
    t0 = time.time()
    c = silver(spark, "categories", settings=settings)
    parent = c.select(
        F.col("category_id").alias("parent_category_id"), F.col("category_name").alias("parent_category_name")
    )
    df = c.join(parent, "parent_category_id", "left").select(
        skey("category_id").alias("category_key"),
        "category_id",
        "category_name",
        "parent_category_id",
        F.coalesce("parent_category_name", "category_name").alias("parent_category_name"),
        "level",
    )
    return write_gold(df, "dim_category", None, "overwrite", settings, t0)


def dim_supplier(spark: SparkSession, settings: Settings | None = None) -> GoldResult:
    settings = settings or get_settings()
    t0 = time.time()
    s = silver(spark, "suppliers", settings=settings)
    df = s.select(
        skey("supplier_id").alias("supplier_key"),
        "supplier_id",
        "supplier_name",
        "country_code",
        "region_code",
        "lead_time_days",
        "quality_tier",
        F.col("active").alias("is_active"),
    )
    return write_gold(df, "dim_supplier", None, "overwrite", settings, t0)


def dim_warehouse(spark: SparkSession, settings: Settings | None = None) -> GoldResult:
    settings = settings or get_settings()
    t0 = time.time()
    w = silver(spark, "warehouses", settings=settings)
    df = w.select(
        skey("warehouse_id").alias("warehouse_key"),
        "warehouse_id",
        "warehouse_name",
        "city",
        "region_code",
        skey("region_code").alias("region_key"),
        "capacity_units",
        "opened_date",
    )
    return write_gold(df, "dim_warehouse", None, "overwrite", settings, t0)


def dim_customer(spark: SparkSession, settings: Settings | None = None) -> GoldResult:
    settings = settings or get_settings()
    t0 = time.time()
    c = silver(spark, "customers", settings=settings)
    df = c.select(
        skey("customer_id").alias("customer_key"),
        "customer_id",
        F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
        "email",
        "customer_segment",
        "country_code",
        "region_code",
        skey("region_code").alias("region_key"),
        "city",
        "signup_date",
        "birth_date",
        "marketing_opt_in",
        "updated_at",
        F.size("quality_warnings").alias("quality_warning_count"),
    )
    return write_gold(df, "dim_customer", None, "overwrite", settings, t0)


def dim_product(spark: SparkSession, settings: Settings | None = None) -> GoldResult:
    settings = settings or get_settings()
    t0 = time.time()
    p = silver(spark, "products", settings=settings)
    c = silver(spark, "categories", settings=settings)
    parent = c.select(
        F.col("category_id").alias("parent_category_id"), F.col("category_name").alias("parent_category_name")
    )
    cats = c.join(parent, "parent_category_id", "left").select(
        "category_id",
        "category_name",
        F.coalesce("parent_category_name", "category_name").alias("parent_category_name"),
    )
    s = silver(spark, "suppliers", settings=settings).select("supplier_id", "supplier_name")
    df = (
        p.join(F.broadcast(cats), "category_id", "left")
        .join(F.broadcast(s), "supplier_id", "left")
        .select(
            skey("product_id").alias("product_key"),
            "product_id",
            "sku",
            "product_name",
            "brand",
            skey("category_id").alias("category_key"),
            "category_id",
            "category_name",
            "parent_category_name",
            skey("supplier_id").alias("supplier_key"),
            "supplier_id",
            "supplier_name",
            F.col("unit_price").alias("unit_price_usd"),
            F.col("unit_cost").alias("unit_cost_usd"),
            F.round(F.col("unit_price") - F.col("unit_cost"), 2).alias("unit_margin_usd"),
            "weight_kg",
            "length_cm",
            "width_cm",
            "height_cm",
            "color",
            "size",
            "is_active",
            "created_at",
            "updated_at",
            F.array_contains("quality_warnings", "products.margin_non_negative").alias("has_negative_margin"),
        )
    )
    return write_gold(df, "dim_product", None, "overwrite", settings, t0)


DIMENSIONS = [dim_date, dim_region, dim_category, dim_supplier, dim_warehouse, dim_customer, dim_product]


def build_dimensions(spark: SparkSession, settings: Settings | None = None) -> dict[str, GoldResult]:
    return {fn.__name__: fn(spark, settings) for fn in DIMENSIONS}
