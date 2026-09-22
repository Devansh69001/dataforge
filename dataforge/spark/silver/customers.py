"""Silver customers: normalised, typed, validated, one row per customer (latest version)."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ...config import Settings, get_settings
from ...quality.suites import CUSTOMERS
from .common import SilverResult, boolean, country_code, date, lower, norm, run_silver_job, title, ts, upper
from .reference import load_refs


def transform(df: DataFrame, refs: dict[str, DataFrame]) -> DataFrame:
    return (
        df.withColumn("email_n", lower("email"))
        .withColumn("customer_segment_n", lower("customer_segment"))
        .withColumn("country_code_n", country_code("country"))
        .withColumn("region_code_n", upper("region_code"))
        .withColumn("signup_date_t", date("signup_date"))
        .withColumn("birth_date_t", date("birth_date"))
        .withColumn("updated_at_t", ts("updated_at"))
        .withColumn("region_code", F.col("region_code_n"))
    )


def final_select(df: DataFrame) -> DataFrame:
    return df.select(
        norm("customer_id").alias("customer_id"),
        title("first_name").alias("first_name"),
        title("last_name").alias("last_name"),
        F.when(F.array_contains("quality_warnings", "customers.email_format"), None)
        .otherwise(F.col("email_n"))
        .alias("email"),
        norm("phone").alias("phone"),
        F.col("birth_date_t").alias("birth_date"),
        F.when(F.array_contains("quality_warnings", "customers.signup_date_not_future"), None)
        .otherwise(F.col("signup_date_t"))
        .alias("signup_date"),
        F.col("customer_segment_n").alias("customer_segment"),
        F.col("country_code_n").alias("country_code"),
        F.col("region_code_n").alias("region_code"),
        norm("city").alias("city"),
        norm("street_address").alias("street_address"),
        boolean("marketing_opt_in").alias("marketing_opt_in"),
        F.col("updated_at_t").alias("updated_at"),
        "quality_warnings",
        "_batch_id",
        "_source_file",
        "_ingested_at",
        "_row_number",
    )


def silver_customers(
    spark: SparkSession, batch_ids: list[str], run_id: str, settings: Settings | None = None
) -> SilverResult:
    settings = settings or get_settings()
    refs = load_refs(spark, ["regions"], settings)
    return run_silver_job(
        spark,
        "customers",
        batch_ids,
        run_id,
        transform,
        refs,
        CUSTOMERS,
        final_select,
        keys=["customer_id"],
        version_col="updated_at",
        partition_col=None,
        settings=settings,
    )
