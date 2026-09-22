"""Silver cleaning / dedup / quarantine and gold facts on the small synthetic lake (Spark)."""

from __future__ import annotations

import glob
import json

import pandas as pd
import pyarrow.dataset as ds
import pytest

from dataforge.quality.quarantine import read_quarantine
from dataforge.spark.silver.common import read_silver

pytestmark = pytest.mark.spark


def _pdf(root, cols=None):
    return ds.dataset(glob.glob(f"{root}/**/*.parquet", recursive=True)).to_table(columns=cols).to_pandas()


def _manifest(lake, batch):
    return json.loads((lake.raw_dir / "_manifest" / f"{batch}.json").read_text())


def test_silver_rejects_injected_defects(lake, silver):
    m = _manifest(lake, "2025-11-30")["datasets"]
    q = {ds_: {} for ds_ in silver}
    for name, res in silver.items():
        q[name] = res.quarantine.get("per_rule", {})
    # every injected reject-class defect is caught by its rule (cascade may add more)
    assert (
        q["order_items"]["order_items.quantity_positive"]
        >= m["order_items"]["defects"]["non_positive_quantity"]
    )
    assert q["order_items"]["order_items.product_known"] >= m["order_items"]["defects"]["invalid_product"]
    assert q["orders"]["orders.customer_known"] >= m["orders"]["defects"]["orphan_customer"]
    assert q["orders"]["orders.status_allowed"] >= m["orders"]["defects"]["unknown_status"]
    assert q["orders"]["orders.order_total_non_negative"] >= m["orders"]["defects"]["negative_total"]
    assert q["orders"]["orders.order_date_not_future"] >= m["orders"]["defects"]["future_order_date"]
    assert q["products"].get("products.unit_price_numeric", 0) >= m["products"]["defects"]["price_as_string"]
    assert (
        q["inventory_events"]["inventory_events.event_type_allowed"]
        >= m["inventory_events"]["defects"]["unknown_event_type"]
    )
    assert (
        q["customers"]["customers.customer_id_not_null"] == m["customers"]["defects"]["missing_customer_id"]
    )
    # exact duplicates are removed before validation
    assert silver["orders"].exact_duplicates_removed >= m["orders"]["defects"]["exact_duplicate"]
    # nothing tripped the circuit breaker
    assert all(res.reject_rate < 0.10 for res in silver.values())


def test_quarantine_records_are_investigable(lake, silver):
    recs = read_quarantine("orders", lake.quarantine_dir, "2025-11-30")
    assert recs
    r = recs[0]
    assert r["stage"] == "validate" and r["rule_ids"] and r["errors"] and r["batch_id"] == "2025-11-30"
    assert "_source_file" in r["record"] and "_row_number" in r["record"]


def test_silver_normalisation_and_uniqueness(lake, silver):
    cust = _pdf(lake.silver_dir / "customers")
    assert cust.customer_id.is_unique
    assert cust.email.dropna().str.match(r"^[a-z0-9._%+-]+@").all()  # lower-cased, trimmed
    assert (
        cust.country_code.dropna()
        .isin(["US", "CA", "DE", "GB", "ES", "SE", "IN", "SG", "AU", "BR", "AE"])
        .all()
    )  # aliases mapped
    assert not cust.first_name.str.startswith(" ").any()
    orders = _pdf(lake.silver_dir / "orders")
    assert orders.order_id.is_unique
    assert orders.status.isin(["placed", "paid", "shipped", "delivered", "cancelled", "returned"]).all()
    assert orders.currency.str.isupper().all()
    assert orders.region_code.notna().all()  # missing region derived from the customer
    assert set(orders.order_month.str.len()) == {7}
    items = _pdf(lake.silver_dir / "order_items")
    assert items.order_item_id.is_unique and (items.quantity > 0).all() and (items.unit_price > 0).all()
    # inconsistent line totals were recomputed
    assert ((items.line_total - items.line_total_source).abs() > 0.05).sum() > 0


def test_keep_latest_version(lake, silver):
    raw = pd.read_csv(lake.raw_dir / "customers" / "customers_2025-11-30.csv", dtype=str)
    dup_ids = raw[raw.duplicated("customer_id", keep=False) & raw.customer_id.notna()].customer_id.unique()
    moved = raw[raw.city.fillna("").str.endswith("(moved)")].customer_id.unique()
    assert len(moved) > 0 and set(moved) <= set(dup_ids)
    cust = _pdf(lake.silver_dir / "customers", ["customer_id", "city", "updated_at"])
    latest = cust[cust.customer_id.isin(moved)]
    assert latest.city.str.endswith("(moved)").all()


def test_incremental_silver_merge(spark, lake, silver):
    from dataforge.ingestion.bronze import ingest_batch
    from dataforge.spark.silver.runner import run_silver

    before = _pdf(lake.silver_dir / "orders", ["order_id", "status", "order_month"])
    ingest_batch("2025-12-31", "run_test_bronze2", settings=lake)
    res = run_silver(spark, ["2025-12-31"], "run_test_silver2", settings=lake)
    after = _pdf(lake.silver_dir / "orders", ["order_id", "status", "order_month"])
    assert after.order_id.is_unique
    assert len(after) >= len(before)
    assert "2025-12" in set(after.order_month)
    b2_ids = set(pd.read_csv(lake.raw_dir / "orders" / "orders_2025-12-31.csv", dtype=str).order_id.dropna())
    assert len(b2_ids & set(after.order_id)) > 0.9 * len(
        b2_ids
    )  # batch-2 orders are present (minus quarantined)
    # only touched months were rewritten
    assert set(res["orders"].partitions_written) <= set(after.order_month)
    # late-arriving update: an order present in both raw batches carries the newest status
    b1 = pd.read_csv(lake.raw_dir / "orders" / "orders_2025-11-30.csv", dtype=str).drop_duplicates("order_id")
    b2 = pd.read_csv(lake.raw_dir / "orders" / "orders_2025-12-31.csv", dtype=str).drop_duplicates("order_id")
    both = b1.merge(b2, on="order_id", suffixes=("_1", "_2"))
    changed = both[
        (both.status_1.str.strip().str.lower() != both.status_2.str.strip().str.lower())
        & (both.updated_at_2 > both.updated_at_1)
    ]
    changed = changed[
        changed.status_2.str.strip()
        .str.lower()
        .isin(["delivered", "shipped", "cancelled", "returned", "paid"])
    ]
    assert len(changed) > 0
    got = after.set_index("order_id").loc[changed.order_id.values[:20], "status"]
    exp = changed.set_index("order_id").status_2.str.strip().str.lower().values[:20]
    assert (got.values == exp).mean() >= 0.9  # a few may be quarantined for other injected defects


def test_gold_facts_are_consistent(lake, gold):
    fo = _pdf(lake.gold_dir / "fact_orders")
    assert fo.order_key.is_unique and fo.fx_usd_per_unit.notna().all() and fo.order_total_usd.notna().all()
    assert (fo.customer_order_seq >= 1).all() and fo.is_first_order.sum() == fo.customer_key.nunique()
    foi = _pdf(lake.gold_dir / "fact_order_items")
    assert foi.order_item_key.is_unique and foi.line_total_usd.notna().all()
    assert set(foi.order_key) <= set(fo.order_key)  # every line has its header
    fi = _pdf(lake.gold_dir / "fact_inventory")
    one = fi[fi.product_id == fi.product_id.iloc[0]].sort_values(["warehouse_id", "event_ts"])
    for _, g in one.groupby("warehouse_id"):
        assert (g.quantity_delta.cumsum().values == g.on_hand_after.values).all()  # running balance
    fs = _pdf(lake.gold_dir / "fact_shipping")
    assert fs.shipment_key.is_unique and (fs.delivery_days.dropna() >= 0).all()
    # delivery time needs both label_created and delivered events; a few labels are quarantined (malformed lines)
    assert (
        fs.is_delivered.sum() > 0
        and 0.9 * fs.is_delivered.sum() <= fs.delivery_days.notna().sum() <= fs.is_delivered.sum()
    )
    dimp = _pdf(lake.gold_dir / "dim_product")
    assert dimp.product_key.is_unique and dimp.category_name.notna().all()


def test_incremental_gold_equals_full_rebuild_for_touched_month(spark, lake, gold):
    from dataforge.spark.gold.facts import fact_orders

    full = (
        _pdf(lake.gold_dir / "fact_orders")
        .query("order_month == '2025-11'")
        .sort_values("order_id")
        .reset_index(drop=True)
    )
    fact_orders(spark, ["2025-11"], lake)
    inc = (
        _pdf(lake.gold_dir / "fact_orders")
        .query("order_month == '2025-11'")
        .sort_values("order_id")
        .reset_index(drop=True)
    )
    cols = [
        "order_id",
        "status",
        "order_total_usd",
        "customer_order_seq",
        "days_since_prev_order",
        "item_count",
    ]
    pd.testing.assert_frame_equal(full[cols], inc[cols])


def test_read_silver_partition_pruning(spark, lake, silver):
    df = read_silver(spark, "orders", ["2025-11"], settings=lake)
    months = {r[0] for r in df.select("order_month").distinct().collect()}
    assert months == {"2025-11"}
