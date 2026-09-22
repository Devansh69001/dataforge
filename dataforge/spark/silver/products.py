"""Silver products: nested JSON fields flattened, brand casing fixed, catalog validated."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ...config import Settings, get_settings
from ...quality.suites import PRODUCTS
from .common import SilverResult, boolean, norm, num, run_silver_job, title, ts, upper
from .reference import load_refs

DIM_SCHEMA = T.StructType(
    [
        T.StructField("length", T.DoubleType()),
        T.StructField("width", T.DoubleType()),
        T.StructField("height", T.DoubleType()),
    ]
)
ATTR_SCHEMA = T.MapType(T.StringType(), T.StringType())


def transform(df: DataFrame, refs: dict[str, DataFrame]) -> DataFrame:
    # from_json returns NULL for the malformed "10x20x5" string -> rule products.dimensions_parseable warns
    dims = F.from_json(norm("dimensions_cm"), DIM_SCHEMA)
    attrs = F.from_json(norm("attributes"), ATTR_SCHEMA)
    return (
        df.withColumn("unit_price_t", num("unit_price"))
        .withColumn("unit_cost_t", num("unit_cost"))
        .withColumn("currency_n", upper("currency"))
        .withColumn("is_active_t", boolean("is_active"))
        .withColumn("updated_at_t", ts("updated_at"))
        .withColumn("created_at_t", ts("created_at"))
        .withColumn("length_cm_t", dims["length"])
        .withColumn("width_cm_t", dims["width"])
        .withColumn("height_cm_t", dims["height"])
        .withColumn("attrs_t", attrs)
    )


def final_select(df: DataFrame) -> DataFrame:
    return df.select(
        norm("product_id").alias("product_id"),
        norm("sku").alias("sku"),
        norm("name").alias("product_name"),
        title("brand").alias("brand"),
        upper("category_id").alias("category_id"),
        upper("supplier_id").alias("supplier_id"),
        F.col("unit_price_t").alias("unit_price"),
        F.col("unit_cost_t").alias("unit_cost"),
        F.col("currency_n").alias("currency"),
        num("weight_kg").alias("weight_kg"),
        F.col("length_cm_t").alias("length_cm"),
        F.col("width_cm_t").alias("width_cm"),
        F.col("height_cm_t").alias("height_cm"),
        F.col("attrs_t")["color"].alias("color"),
        F.col("attrs_t")["size"].alias("size"),
        norm("attributes").alias("attributes_json"),  # nothing lost: unknown keys stay queryable
        F.col("is_active_t").alias("is_active"),
        F.col("created_at_t").alias("created_at"),
        F.col("updated_at_t").alias("updated_at"),
        "quality_warnings",
        "_batch_id",
        "_source_file",
        "_ingested_at",
        "_row_number",
    )


def silver_products(
    spark: SparkSession, batch_ids: list[str], run_id: str, settings: Settings | None = None
) -> SilverResult:
    settings = settings or get_settings()
    refs = load_refs(spark, ["categories", "suppliers"], settings)
    return run_silver_job(
        spark,
        "products",
        batch_ids,
        run_id,
        transform,
        refs,
        PRODUCTS,
        final_select,
        keys=["product_id"],
        version_col="updated_at",
        partition_col=None,
        settings=settings,
    )
