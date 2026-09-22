"""Rule engine semantics on a hand-built DataFrame (Spark)."""

from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest
from pyspark.sql import functions as F

from dataforge.quality.rules import QualityThresholdError, Rule, RuleSuite, apply_rules, enforce_threshold
from dataforge.quality.suites import SUITES

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def frame(spark):
    ing = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    rows = [
        # id,  qty,  qty_t, price_t, status, ts_t,                     cust
        ("A", "2", 2, 9.5, "paid", dt.datetime(2025, 5, 1), "C1"),
        ("B", "0", 0, 9.5, "paid", dt.datetime(2025, 5, 1), "C1"),  # qty <= 0
        ("C", "x", None, 9.5, "paid", dt.datetime(2025, 5, 1), "C2"),  # not castable
        (None, "1", 1, 9.5, "paid", dt.datetime(2025, 5, 1), "C1"),  # null id
        ("E", "1", 1, -1.0, "weird", dt.datetime(2027, 1, 1), "C9"),  # price, status, future, fk
        ("F", "1", 1, 9.5, "paid", dt.datetime(2025, 5, 1), "C1"),
    ]
    # pandas -> Arrow -> Spark: no Python workers involved (same path the pipeline uses)
    pdf = pd.DataFrame(rows, columns=["id", "qty", "qty_t", "price_t", "status", "ts_t", "cust"])
    pdf["qty_t"] = pdf["qty_t"].astype("Int64")
    df = spark.createDataFrame(pdf).withColumn("_ingested_at", F.lit(ing))
    refs = {"customers": spark.createDataFrame(pd.DataFrame({"customer_id": ["C1", "C2"]}))}
    return df, refs


def _suite(rules, max_rate=0.99):
    return RuleSuite("t", rules, max_reject_rate=max_rate)


def test_each_check_type(frame):
    df, refs = frame
    suite = _suite(
        [
            Rule("t.id_not_null", "t", "not_null", "reject", "id required", "id"),
            Rule("t.id_regex", "t", "regex", "reject", "id must be a letter", "id", {"pattern": "^[A-Z]$"}),
            Rule("t.qty_castable", "t", "castable", "reject", "qty numeric", "qty", {"typed": "qty_t"}),
            Rule("t.qty_positive", "t", "compare", "reject", "qty > 0", "qty_t", {"op": ">", "value": 0}),
            Rule(
                "t.price_positive", "t", "compare", "reject", "price > 0", "price_t", {"op": ">", "value": 0}
            ),
            Rule(
                "t.status_allowed",
                "t",
                "in_set",
                "reject",
                "status known",
                "status",
                {"values": ["paid", "placed"]},
            ),
            Rule("t.not_future", "t", "not_future", "reject", "ts not in future", "ts_t"),
            Rule(
                "t.customer_known",
                "t",
                "fk",
                "reject",
                "customer exists",
                "cust",
                {"ref": "customers", "ref_column": "customer_id"},
            ),
            Rule(
                "t.expr", "t", "expr", "warn", "qty*price small", None, {"expression": "qty_t * price_t < 15"}
            ),
        ]
    )
    res = apply_rules(df, suite, refs)
    stats = {s.rule_id: s.failed for s in res.stats}
    assert stats == {
        "t.id_not_null": 1,
        "t.id_regex": 0,
        "t.qty_castable": 1,
        "t.qty_positive": 1,
        "t.price_positive": 1,
        "t.status_allowed": 1,
        "t.not_future": 1,
        "t.customer_known": 1,
        "t.expr": 1,
    }
    assert res.total == 6 and res.rejected_count == 4
    valid_ids = sorted(r.id for r in res.valid.collect())
    assert valid_ids == ["A", "F"]
    warned = {r.id: r.quality_warnings for r in res.valid.collect()}
    assert warned["A"] == ["t.expr"] and warned["F"] == []
    rejected = {r.id: (r.rule_ids, r.errors) for r in res.rejected.collect()}
    assert set(rejected["E"][0]) == {
        "t.price_positive",
        "t.status_allowed",
        "t.not_future",
        "t.customer_known",
    }
    assert "price > 0" in rejected["E"][1]


def test_threshold_circuit_breaker(frame):
    df, refs = frame
    suite = _suite(
        [Rule("t.qty_positive", "t", "compare", "reject", "qty > 0", "qty_t", {"op": ">", "value": 0})],
        max_rate=0.05,
    )
    res = apply_rules(df, suite, refs)
    with pytest.raises(QualityThresholdError):
        enforce_threshold(res, suite)


def test_rule_catalogue_is_well_formed():
    ids = [r.id for s in SUITES.values() for r in s.rules]
    assert len(ids) == len(set(ids))
    for name, suite in SUITES.items():
        assert suite.dataset == name
        for r in suite.rules:
            assert r.id.startswith(f"{name}."), r.id
            assert r.description
            assert r.severity in ("reject", "warn")
    # the documented core rules exist
    for rid in (
        "orders.customer_known",
        "order_items.quantity_positive",
        "order_items.product_known",
        "products.unit_price_positive",
        "orders.order_date_not_future",
        "inventory_events.sign_matches_type",
    ):
        assert rid in ids
