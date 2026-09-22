"""Shared building blocks for silver jobs.

A silver job follows the same shape for every dataset:

    bronze (strings)  -> drop exact duplicate deliveries
                      -> normalise (trim / case / alias maps)          *_n columns
                      -> typed casts (null on failure, never abort)    *_t columns
                      -> apply the dataset's RuleSuite                 valid | rejected
                      -> rejected  -> quarantine (stage=validate)
                      -> valid     -> select final silver schema
                      -> merge into silver (dedup by business key, keep the latest version)

Merging strategies
    snapshot     small master/reference data: union(existing, new) -> keep latest -> overwrite all
    partitioned  event data partitioned by month: only the months present in the new batch
                 are read back, merged and rewritten (partition-aware, cheap increments)
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ...config import Settings, get_settings
from ...generators.reference import COUNTRY_ALIASES
from ...ingestion.bronze import bronze_files
from ...ingestion.schemas import REGISTRY
from ...logging_utils import get_logger
from ...quality.quarantine import write_quarantine
from ...quality.rules import RuleSuite, ValidationResult, apply_rules, enforce_threshold
from ..io import list_partitions, read_files, read_parquet, to_arrow, write_parquet

log = get_logger("spark.silver")

META_COLS = ["_batch_id", "_source_file", "_ingested_at", "_run_id", "_row_number"]
ISO_TS = "yyyy-MM-dd'T'HH:mm:ss'Z'"


@dataclass
class SilverResult:
    dataset: str
    batch_ids: list[str]
    input_rows: int = 0
    exact_duplicates_removed: int = 0
    valid_rows: int = 0
    rejected_rows: int = 0
    reject_rate: float = 0.0
    rule_stats: list[dict[str, Any]] = field(default_factory=list)
    quarantine: dict[str, Any] = field(default_factory=dict)
    written_rows: int = 0
    partitions_written: list[str] = field(default_factory=list)
    seconds: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


# ------------------------------------------------------------------ bronze in
def load_bronze(
    spark: SparkSession, dataset: str, batch_ids: list[str] | None, settings: Settings | None = None
) -> DataFrame | None:
    files = bronze_files(dataset, batch_ids, settings)
    df = read_files(spark, files)
    if df is None:
        return None
    # make sure every registered column exists even if a delivery omitted an optional one
    schema = REGISTRY[dataset]
    for c in schema.known:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    return df


def drop_exact_duplicates(df: DataFrame, dataset: str) -> tuple[DataFrame, int]:
    """Identical rows re-delivered inside the same batch (or across batches) are noise
    from at-least-once producers; keep the first occurrence."""
    business_cols = [c for c in df.columns if c not in META_COLS]
    before = df.count()
    w = Window.partitionBy(*business_cols).orderBy(F.col("_ingested_at").asc(), F.col("_row_number").asc())
    out = df.withColumn("__dup_rn", F.row_number().over(w)).filter(F.col("__dup_rn") == 1).drop("__dup_rn")
    after = out.count()
    return out, before - after


# ---------------------------------------------------------------- normalise
def norm(col: str) -> F.Column:
    """trim and turn empty strings into NULL."""
    c = F.trim(F.col(col))
    return F.when(c == "", None).otherwise(c)


def lower(col: str) -> F.Column:
    return F.lower(norm(col))


def upper(col: str) -> F.Column:
    return F.upper(norm(col))


def title(col: str) -> F.Column:
    return F.initcap(norm(col))


def ts(col: str) -> F.Column:
    return F.to_timestamp(norm(col), ISO_TS)


def date(col: str) -> F.Column:
    return F.to_date(norm(col), "yyyy-MM-dd")


def num(col: str) -> F.Column:
    return norm(col).cast(T.DoubleType())


def integer(col: str) -> F.Column:
    # "3" -> 3, "3.0" -> null on purpose (integers must be integral in the source)
    return F.when(norm(col).rlike(r"^-?\d+$"), norm(col).cast(T.LongType()))


def boolean(col: str) -> F.Column:
    c = lower(col)
    return F.when(c.isin("true", "1", "yes", "t"), True).when(c.isin("false", "0", "no", "f"), False)


def month_of(ts_col: str) -> F.Column:
    return F.date_format(F.col(ts_col), "yyyy-MM")


def country_code(col: str) -> F.Column:
    """Map messy country spellings ('U.S.', 'united states', 'USA') to ISO-2 codes."""
    lookup: dict[str, str] = {}
    for code, aliases in COUNTRY_ALIASES.items():
        for a in {*aliases, code}:
            lookup.setdefault(a.strip().lower(), code)
    pairs: list = []
    for alias, code in lookup.items():
        pairs.extend([F.lit(alias), F.lit(code)])
    m = F.create_map(*pairs)
    return m[lower(col)]


# ------------------------------------------------------------------- dedup
def keep_latest(df: DataFrame, keys: list[str], version_col: str | None) -> DataFrame:
    """One row per business key: highest version, then most recently ingested."""
    order = []
    if version_col:
        order.append(F.col(version_col).desc_nulls_last())
    order += [F.col("_ingested_at").desc(), F.col("_row_number").desc()]
    w = Window.partitionBy(*keys).orderBy(*order)
    return df.withColumn("__rn", F.row_number().over(w)).filter(F.col("__rn") == 1).drop("__rn")


# ---------------------------------------------------------------- validate
def validate(
    df: DataFrame, suite: RuleSuite, refs: dict[str, DataFrame], raw_columns: list[str]
) -> ValidationResult:
    record_cols = [c for c in raw_columns if c in df.columns] + [
        c for c in ("_batch_id", "_source_file", "_row_number") if c in df.columns
    ]
    result = apply_rules(df, suite, refs, record_columns=record_cols)
    enforce_threshold(result, suite)
    return result


def quarantine_rejected(
    result: ValidationResult, dataset: str, batch_ids: list[str], run_id: str, settings: Settings
) -> dict[str, Any]:
    if result.rejected_count == 0:
        return {"count": 0, "per_rule": {}}
    rows = to_arrow(result.rejected).to_pylist()
    summary: dict[str, Any] = {"count": 0, "per_rule": {}, "paths": []}
    by_batch: dict[str, list[dict]] = {}
    for r in rows:
        rule_ids = r.pop("rule_ids") or []
        errors = r.pop("errors") or []
        bid = r.get("_batch_id") or (batch_ids[0] if batch_ids else "unknown")
        by_batch.setdefault(bid, []).append({"record": r, "rule_ids": rule_ids, "errors": errors})
    for bid, recs in by_batch.items():
        src = recs[0]["record"].get("_source_file", dataset)
        s = write_quarantine(dataset, bid, "validate", run_id, src, recs, settings.quarantine_dir)
        summary["count"] += s["count"]
        summary["paths"].append(s["path"])
        for k, v in s["per_rule"].items():
            summary["per_rule"][k] = summary["per_rule"].get(k, 0) + v
    return summary


# ------------------------------------------------------------------ silver out
def silver_root(dataset: str, settings: Settings | None = None) -> Path:
    return (settings or get_settings()).silver_dir / dataset


def read_silver(
    spark: SparkSession,
    dataset: str,
    partition_values: list[str] | None = None,
    settings: Settings | None = None,
) -> DataFrame | None:
    return read_parquet(spark, silver_root(dataset, settings), partition_values, settings)


def merge_snapshot(
    spark: SparkSession,
    new: DataFrame,
    dataset: str,
    keys: list[str],
    version_col: str | None,
    settings: Settings,
) -> int:
    existing = read_silver(spark, dataset, settings=settings)
    merged = new if existing is None else existing.unionByName(new, allowMissingColumns=True)
    merged = keep_latest(merged, keys, version_col).cache()
    n = merged.count()
    write_parquet(merged, silver_root(dataset, settings), None, "overwrite", settings)
    merged.unpersist()
    return n


def merge_partitioned(
    spark: SparkSession,
    new: DataFrame,
    dataset: str,
    keys: list[str],
    version_col: str | None,
    partition_col: str,
    settings: Settings,
) -> tuple[int, list[str]]:
    months = [r[0] for r in new.select(partition_col).distinct().collect() if r[0] is not None]
    existing_parts = set(list_partitions(silver_root(dataset, settings)))
    touched = sorted(months)
    existing = (
        read_silver(spark, dataset, [m for m in touched if m in existing_parts], settings=settings)
        if existing_parts
        else None
    )
    merged = new if existing is None else existing.unionByName(new, allowMissingColumns=True)
    merged = keep_latest(merged, keys, version_col).cache()
    n = merged.count()
    write_parquet(merged, silver_root(dataset, settings), partition_col, "overwrite_partitions", settings)
    merged.unpersist()
    return n, touched


# ---------------------------------------------------------------- template
def run_silver_job(
    spark: SparkSession,
    dataset: str,
    batch_ids: list[str],
    run_id: str,
    transform: Any,
    refs: dict[str, DataFrame],
    suite: RuleSuite,
    final_select: Any,
    keys: list[str],
    version_col: str | None,
    partition_col: str | None,
    settings: Settings | None = None,
) -> SilverResult:
    """Generic driver: `transform(df, refs)` adds *_n / *_t columns; `final_select(df)` projects
    the silver schema from the validated frame."""
    settings = settings or get_settings()
    t0 = time.time()
    res = SilverResult(dataset=dataset, batch_ids=batch_ids)
    df = load_bronze(spark, dataset, batch_ids, settings)
    if df is None:
        log.warning("no bronze data for dataset/batches", dataset=dataset, batch_ids=batch_ids)
        return res
    raw_columns = [c for c in df.columns if c not in META_COLS]
    df, dups = drop_exact_duplicates(df, dataset)
    res.exact_duplicates_removed = dups
    df = transform(df, refs)
    result = validate(df, suite, refs, raw_columns)
    res.input_rows = result.total + dups
    res.rejected_rows = result.rejected_count
    res.valid_rows = result.total - result.rejected_count
    res.reject_rate = round(result.reject_rate, 6)
    res.rule_stats = [s.as_dict() for s in result.stats]
    res.quarantine = quarantine_rejected(result, dataset, batch_ids, run_id, settings)

    final = final_select(result.valid)
    if partition_col:
        res.written_rows, res.partitions_written = merge_partitioned(
            spark, final, dataset, keys, version_col, partition_col, settings
        )
    else:
        res.written_rows = merge_snapshot(spark, final, dataset, keys, version_col, settings)
    result.valid.unpersist()
    res.seconds = round(time.time() - t0, 2)
    log.info(
        "silver job done",
        dataset=dataset,
        input=res.input_rows,
        exact_dups=dups,
        valid=res.valid_rows,
        rejected=res.rejected_rows,
        reject_rate=res.reject_rate,
        silver_rows=res.written_rows,
        seconds=res.seconds,
    )
    return res
