"""Synthetic data generation: determinism, volumes, defect manifest."""

from __future__ import annotations

import hashlib
import json
from datetime import date

import pandas as pd

from dataforge.generators.base import GenConfig, Rng
from dataforge.generators.run import generate_all


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_generation_is_deterministic(tmp_path):
    cfg = GenConfig(seed=7, scale=0.01, batches=[date(2025, 11, 30), date(2025, 12, 31)])
    generate_all(cfg, tmp_path / "a")
    generate_all(cfg, tmp_path / "b")
    for rel in [
        "customers/customers_2025-11-30.csv",
        "orders/orders_2025-12-31.csv",
        "inventory/inventory_events_2025-11-30.ndjson",
        "shipping/shipping_events_2025-11-30.log",
        "products/product_catalog_2025-12-31.json",
    ]:
        assert _sha(tmp_path / "a" / rel) == _sha(tmp_path / "b" / rel), rel


def test_seed_changes_output(tmp_path):
    generate_all(GenConfig(seed=1, scale=0.01), tmp_path / "a")
    generate_all(GenConfig(seed=2, scale=0.01), tmp_path / "b")
    assert _sha(tmp_path / "a" / "orders/orders_2025-11-30.csv") != _sha(
        tmp_path / "b" / "orders/orders_2025-11-30.csv"
    )


def test_child_rng_streams_are_independent():
    root = Rng(123)
    a = root.child("orders").ints(0, 1000, 5).tolist()
    b = Rng(123).child("orders").ints(0, 1000, 5).tolist()
    c = Rng(123).child("customers").ints(0, 1000, 5).tolist()
    assert a == b and a != c


def test_volumes_and_manifest(lake):
    summary = json.loads((lake.raw_dir / "_manifest" / "summary.json").read_text())
    assert summary["synthetic"] is True
    m1 = json.loads((lake.raw_dir / "_manifest" / "2025-11-30.json").read_text())
    m2 = json.loads((lake.raw_dir / "_manifest" / "2025-12-31.json").read_text())
    orders = pd.read_csv(lake.raw_dir / "orders" / "orders_2025-11-30.csv", dtype=str)
    assert len(orders) == m1["datasets"]["orders"]["rows"]
    assert m1["datasets"]["orders"]["defects"]["orphan_customer"] > 0
    assert m1["datasets"]["inventory_events"]["defects"]["malformed_json_line"] > 0
    # schema evolution only in the second batch
    assert m1["schema_version"] == 1 and m2["schema_version"] == 2
    orders2 = pd.read_csv(lake.raw_dir / "orders" / "orders_2025-12-31.csv", dtype=str)
    assert "promo_campaign" in orders2.columns and "promo_campaign" not in orders.columns
    # late-arriving updates: some order ids appear in both batches
    assert m2["datasets"]["orders"]["defects"]["late_arriving_updates"] > 0
    assert len(set(orders.order_id) & set(orders2.order_id)) > 0


def test_synthetic_notice_present(lake):
    assert (lake.raw_dir / "SYNTHETIC_DATA_NOTICE.txt").exists()
    assert "SYNTHETIC" in (lake.raw_dir / "SYNTHETIC_DATA_NOTICE.txt").read_text()
