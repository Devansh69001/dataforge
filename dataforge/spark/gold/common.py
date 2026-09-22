"""Shared helpers for gold jobs.

Surrogate keys are deterministic 64-bit hashes of the business key (xxhash64). That keeps
the loader simple and idempotent - a fact row computed twice resolves to the same key
without a lookup against the warehouse - at the cost of not supporting SCD2 history,
which is documented as a limitation.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ...config import Settings, get_settings
from ...logging_utils import get_logger
from ..io import read_parquet, write_parquet
from ..silver.common import read_silver

log = get_logger("spark.gold")


@dataclass
class GoldResult:
    table: str
    rows: int = 0
    partitions: list[str] = field(default_factory=list)
    seconds: float = 0.0
    details: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def skey(*cols: str) -> F.Column:
    """Deterministic surrogate key from one or more business-key columns."""
    return F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols])


def date_key(col: str) -> F.Column:
    return F.date_format(F.col(col), "yyyyMMdd").cast("int")


def gold_root(table: str, settings: Settings | None = None) -> Path:
    return (settings or get_settings()).gold_dir / table


def read_gold(
    spark: SparkSession,
    table: str,
    partition_values: list[str] | None = None,
    settings: Settings | None = None,
) -> DataFrame | None:
    return read_parquet(spark, gold_root(table, settings), partition_values, settings)


def silver(
    spark: SparkSession,
    dataset: str,
    partition_values: list[str] | None = None,
    settings: Settings | None = None,
) -> DataFrame:
    df = read_silver(spark, dataset, partition_values, settings)
    if df is None:
        raise RuntimeError(f"silver dataset '{dataset}' is empty; run the silver layer first")
    return df


def write_gold(
    df: DataFrame, table: str, partition_by: str | None, mode: str, settings: Settings, t0: float
) -> GoldResult:
    df = df.cache()
    n = df.count()
    parts = [r[0] for r in df.select(partition_by).distinct().collect()] if partition_by else []
    write_parquet(df, gold_root(table, settings), partition_by, mode, settings)
    df.unpersist()
    res = GoldResult(
        table=table,
        rows=n,
        partitions=sorted(p for p in parts if p is not None),
        seconds=round(time.time() - t0, 2),
    )
    log.info(
        "gold table written",
        table=table,
        rows=n,
        partitions=len(res.partitions),
        mode=mode,
        seconds=res.seconds,
    )
    return res


def dense_fx_rates(spark: SparkSession, settings: Settings) -> DataFrame:
    """Forward-filled daily USD rate per currency (window function over a generated calendar).

    The reference API has outage days; gold must still convert every order, so the last
    known rate carries forward. USD always converts at 1.0."""
    fx = silver(spark, "exchange_rates", settings=settings).select(
        "rate_date", "currency_code", "usd_per_unit"
    )
    bounds = fx.agg(F.min("rate_date").alias("lo"), F.max("rate_date").alias("hi")).collect()[0]
    cal = spark.sql(
        f"SELECT explode(sequence(to_date('{bounds['lo']}'), to_date('{bounds['hi']}'), interval 1 day)) AS rate_date"
    )
    currencies = fx.select("currency_code").distinct()
    grid = cal.crossJoin(currencies)
    w = (
        Window.partitionBy("currency_code")
        .orderBy("rate_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    dense = grid.join(fx, ["rate_date", "currency_code"], "left").withColumn(
        "usd_per_unit", F.last("usd_per_unit", ignorenulls=True).over(w)
    )
    return dense.withColumn(
        "usd_per_unit", F.when(F.col("currency_code") == "USD", F.lit(1.0)).otherwise(F.col("usd_per_unit"))
    )
