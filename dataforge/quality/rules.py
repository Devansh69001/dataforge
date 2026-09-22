"""Declarative data-quality rules evaluated on Spark DataFrames.

A `Rule` is a pure description (id, check type, parameters, severity, rationale). The
engine turns each rule into a boolean "failed" expression, evaluates all rules in ONE pass
over the DataFrame, and splits rows into:

  * valid rows   - no `reject` rule failed; `warn` failures are kept as `quality_warnings`
  * rejected rows - carry `rule_ids` + `errors` and go to quarantine

Check types
-----------
  not_null      column must be present
  regex         column (if present) must match `pattern`
  in_set        column (if present) must be one of `values`
  castable      the raw string could be converted: fails when raw is not null but `typed` is null
  compare       column <op> value              (op in >, >=, <, <=, ==, !=)   nulls pass
  compare_cols  left <op> right                nulls pass
  not_future    timestamp column must not be after `reference` column (default `_ingested_at`)
  fk            column value (if present) must exist in refs[ref].ref_column
  expr          arbitrary Spark SQL boolean expression that must be TRUE to pass

Severity
--------
  reject   row goes to quarantine
  warn     row is kept; the rule id is appended to quality_warnings and counted

Each suite can set `max_reject_rate`: when a batch rejects more than that fraction of
rows the job raises `QualityThresholdError` (a circuit breaker for upstream breakage:
one bad file should not silently empty the warehouse).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

OPS = {">": "__gt__", ">=": "__ge__", "<": "__lt__", "<=": "__le__", "==": "__eq__", "!=": "__ne__"}


@dataclass(frozen=True)
class Rule:
    id: str
    dataset: str
    check: str
    severity: str  # reject | warn
    description: str
    column: str | None = None
    params: dict[str, Any] = field(default_factory=dict)
    rationale: str = ""

    def __post_init__(self):
        if self.severity not in ("reject", "warn"):
            raise ValueError(f"{self.id}: severity must be reject|warn")


@dataclass
class RuleSuite:
    dataset: str
    rules: list[Rule]
    max_reject_rate: float = 0.05
    description: str = ""

    def by_severity(self, sev: str) -> list[Rule]:
        return [r for r in self.rules if r.severity == sev]


@dataclass
class RuleStat:
    rule_id: str
    severity: str
    failed: int
    total: int

    @property
    def failure_rate(self) -> float:
        return self.failed / self.total if self.total else 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "severity": self.severity,
            "failed": self.failed,
            "total": self.total,
            "failure_rate": round(self.failure_rate, 6),
        }


@dataclass
class ValidationResult:
    valid: DataFrame
    rejected: DataFrame
    stats: list[RuleStat]
    total: int
    rejected_count: int

    @property
    def reject_rate(self) -> float:
        return self.rejected_count / self.total if self.total else 0.0


class QualityThresholdError(Exception):
    pass


def _fail_expr(rule: Rule, df: DataFrame, refs: dict[str, DataFrame]) -> Column:
    """Boolean column that is TRUE when the row FAILS the rule."""
    c = F.col(rule.column) if rule.column else None
    p = rule.params
    if rule.check == "not_null":
        return c.isNull()
    if rule.check == "regex":
        return c.isNotNull() & ~c.rlike(p["pattern"])
    if rule.check == "in_set":
        return c.isNotNull() & ~c.isin(list(p["values"]))
    if rule.check == "castable":
        return c.isNotNull() & F.col(p["typed"]).isNull()
    if rule.check == "compare":
        cmp = getattr(c, OPS[p["op"]])(F.lit(p["value"]))
        return c.isNotNull() & ~cmp
    if rule.check == "compare_cols":
        left, right = F.col(p["left"]), F.col(p["right"])
        cmp = getattr(left, OPS[p["op"]])(right)
        return left.isNotNull() & right.isNotNull() & ~cmp
    if rule.check == "not_future":
        ref = F.col(p.get("reference", "_ingested_at"))
        return c.isNotNull() & (c > ref)
    if rule.check == "fk":
        # handled by join in apply_rules; marker column is pre-computed
        return c.isNotNull() & F.col(f"__fk_{rule.id.replace('.', '_')}").isNull()
    if rule.check == "expr":
        return ~F.coalesce(F.expr(p["expression"]), F.lit(True))
    raise ValueError(f"unknown check type {rule.check} in {rule.id}")


def _attach_fk_markers(df: DataFrame, rules: list[Rule], refs: dict[str, DataFrame]) -> DataFrame:
    for r in rules:
        if r.check != "fk":
            continue
        ref = refs.get(r.params["ref"])
        if ref is None:
            raise ValueError(f"{r.id}: reference dataset '{r.params['ref']}' not provided")
        marker = f"__fk_{r.id.replace('.', '_')}"
        keys = (
            ref.select(F.col(r.params["ref_column"]).alias(r.column)).distinct().withColumn(marker, F.lit(1))
        )
        if r.params.get("broadcast", True):
            keys = F.broadcast(keys)
        df = df.join(keys, on=r.column, how="left")
    return df


def apply_rules(
    df: DataFrame,
    suite: RuleSuite,
    refs: dict[str, DataFrame] | None = None,
    record_columns: list[str] | None = None,
) -> ValidationResult:
    """Evaluate a suite. `record_columns` are the columns copied into the quarantine record
    (defaults to every column not starting with '__')."""
    refs = refs or {}
    df = _attach_fk_markers(df, suite.rules, refs)

    fail_cols = []
    for i, r in enumerate(suite.rules):
        name = f"__fail_{i}"
        df = df.withColumn(name, F.coalesce(_fail_expr(r, df, refs), F.lit(False)))
        fail_cols.append(name)

    reject_idx = [i for i, r in enumerate(suite.rules) if r.severity == "reject"]
    warn_idx = [i for i, r in enumerate(suite.rules) if r.severity == "warn"]

    def ids(idx: list[int]) -> Column:
        if not idx:
            return F.array().cast("array<string>")
        return F.array_compact(
            F.array(*[F.when(F.col(f"__fail_{i}"), F.lit(suite.rules[i].id)) for i in idx])
        )

    def errors(idx: list[int]) -> Column:
        if not idx:
            return F.array().cast("array<string>")
        return F.array_compact(
            F.array(*[F.when(F.col(f"__fail_{i}"), F.lit(suite.rules[i].description)) for i in idx])
        )

    df = (
        df.withColumn("rule_ids", ids(reject_idx))
        .withColumn("errors", errors(reject_idx))
        .withColumn("quality_warnings", ids(warn_idx))
        .withColumn("__rejected", F.size(F.col("rule_ids")) > 0)
    )
    df = df.cache()
    total = df.count()

    agg = df.agg(
        *[F.sum(F.col(c).cast("int")).alias(c) for c in fail_cols],
        F.sum(F.col("__rejected").cast("int")).alias("__rej"),
    ).collect()[0]
    stats = [
        RuleStat(r.id, r.severity, int(agg[f"__fail_{i}"] or 0), total) for i, r in enumerate(suite.rules)
    ]
    rejected_count = int(agg["__rej"] or 0)

    record_columns = record_columns or [
        c
        for c in df.columns
        if not c.startswith("__") and c not in ("rule_ids", "errors", "quality_warnings")
    ]
    drop = [c for c in df.columns if c.startswith("__")]
    valid = df.filter(~F.col("__rejected")).drop(*drop, "rule_ids", "errors")
    rejected = df.filter(F.col("__rejected")).select(*record_columns, "rule_ids", "errors")
    return ValidationResult(
        valid=valid, rejected=rejected, stats=stats, total=total, rejected_count=rejected_count
    )


def enforce_threshold(result: ValidationResult, suite: RuleSuite) -> None:
    if result.total and result.reject_rate > suite.max_reject_rate:
        raise QualityThresholdError(
            f"{suite.dataset}: reject rate {result.reject_rate:.2%} exceeds max_reject_rate {suite.max_reject_rate:.2%} "
            f"({result.rejected_count}/{result.total}); refusing to publish a batch that looks broken upstream"
        )
