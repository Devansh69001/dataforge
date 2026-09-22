"""SOURCE 4 - inventory events (newline-delimited JSON from the warehouse management system).

Event-sourced model: on-hand stock is the running sum of `quantity_delta` per
(product, warehouse). Receipts carry supplier and defect counts, which feed the
supplier_performance mart.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .base import GenConfig, Rng, parse_iso
from .orders import OrderBundle


def gen_inventory_events(
    cfg: GenConfig,
    rng: Rng,
    products: pd.DataFrame,
    warehouses: pd.DataFrame,
    suppliers: pd.DataFrame,
    bundle: OrderBundle,
) -> pd.DataFrame:
    r = rng.child("inventory")
    wh_by_region = warehouses.set_index("region_code")["warehouse_id"].to_dict()
    all_wh = warehouses["warehouse_id"].values
    sup_defect = suppliers.set_index("supplier_id")["_latent_defect_rate"].to_dict()
    prod_supplier = products.set_index("product_id")["supplier_id"].to_dict()
    prod_cost = products.set_index("product_id")["unit_cost"].to_dict()

    orders = bundle.orders
    items = bundle.order_items.merge(
        orders[["order_id", "status", "region_code", "order_date", "updated_at"]], on="order_id"
    )
    shipped = items[items["status"].isin(["shipped", "delivered", "returned"])].copy()
    shipped["warehouse_id"] = shipped["region_code"].map(wh_by_region).fillna(all_wh[0])
    ship_ts = parse_iso(shipped["order_date"]) + pd.to_timedelta(r.ints(6, 60, len(shipped)), unit="h")

    frames = []
    # ---- shipments (stock out)
    frames.append(
        pd.DataFrame(
            {
                "event_type": "shipment",
                "product_id": shipped["product_id"].values,
                "warehouse_id": shipped["warehouse_id"].values,
                "quantity_delta": -shipped["quantity"].values,
                "event_ts": ship_ts.values,
                "reference_id": shipped["order_id"].values,
                "supplier_id": None,
                "unit_cost": None,
                "defective_qty": None,
                "counted_quantity": None,
                "reason": None,
            }
        )
    )
    # ---- returns (stock back in)
    ret = shipped[shipped["status"] == "returned"]
    frames.append(
        pd.DataFrame(
            {
                "event_type": "return",
                "product_id": ret["product_id"].values,
                "warehouse_id": ret["warehouse_id"].values,
                "quantity_delta": ret["quantity"].values,
                "event_ts": (
                    parse_iso(ret["updated_at"]) + pd.to_timedelta(r.ints(1, 72, len(ret)), unit="h")
                ).values,
                "reference_id": ret["order_id"].values,
                "supplier_id": None,
                "unit_cost": None,
                "defective_qty": None,
                "counted_quantity": None,
                "reason": r.choice(
                    ["damaged", "wrong_item", "changed_mind", "defective"], len(ret), p=[0.25, 0.15, 0.4, 0.2]
                ),
            }
        )
    )
    # ---- receipts: opening stock + monthly replenishment sized to roughly match demand
    shipped["month"] = parse_iso(shipped["order_date"]).dt.to_period("M").astype(str)
    monthly = shipped.groupby(["product_id", "warehouse_id", "month"])["quantity"].sum().reset_index()
    total = shipped.groupby(["product_id", "warehouse_id"])["quantity"].sum().reset_index()

    opening_qty = np.ceil(total["quantity"].values * 0.3 + 20 + r.ints(0, 30, len(total)))
    opening_ts = pd.Timestamp(cfg.start_date) - pd.to_timedelta(r.ints(1, 25, len(total)), unit="D")
    receipts = [
        pd.DataFrame(
            {
                "event_type": "receipt",
                "product_id": total["product_id"].values,
                "warehouse_id": total["warehouse_id"].values,
                "quantity_delta": opening_qty.astype(int),
                "event_ts": opening_ts.values,
                "reference_id": [f"PO-{i:07d}" for i in range(len(total))],
            }
        )
    ]
    replen_qty = np.ceil(monthly["quantity"].values * r.floats(0.75, 1.35, len(monthly))).astype(int)
    replen_ts = (
        pd.to_datetime(monthly["month"] + "-01")
        + pd.to_timedelta(r.ints(0, 27, len(monthly)), unit="D")
        + pd.to_timedelta(r.ints(6, 20, len(monthly)), unit="h")
    )
    receipts.append(
        pd.DataFrame(
            {
                "event_type": "receipt",
                "product_id": monthly["product_id"].values,
                "warehouse_id": monthly["warehouse_id"].values,
                "quantity_delta": replen_qty,
                "event_ts": replen_ts.values,
                "reference_id": [f"PO-{i + len(total):07d}" for i in range(len(monthly))],
            }
        )
    )
    rec = pd.concat(receipts, ignore_index=True)
    rec["supplier_id"] = rec["product_id"].map(prod_supplier)
    rec["unit_cost"] = np.round(rec["product_id"].map(prod_cost).values * r.floats(0.95, 1.05, len(rec)), 2)
    rates = rec["supplier_id"].map(sup_defect).fillna(0.01).values
    rec["defective_qty"] = r.g.binomial(rec["quantity_delta"].values, rates)
    rec["counted_quantity"] = None
    rec["reason"] = None
    frames.append(rec)

    # ---- adjustments (shrinkage / corrections) and periodic stock counts
    n_adj = max(50, int(len(total) * 0.15))
    pick = r.sample_idx(len(total), n_adj)
    frames.append(
        pd.DataFrame(
            {
                "event_type": "adjustment",
                "product_id": total["product_id"].values[pick],
                "warehouse_id": total["warehouse_id"].values[pick],
                "quantity_delta": r.ints(-8, 6, n_adj),
                "event_ts": r.timestamps(cfg.start_date, cfg.end_date, n_adj).values,
                "reference_id": [f"ADJ-{i:06d}" for i in range(n_adj)],
                "supplier_id": None,
                "unit_cost": None,
                "defective_qty": None,
                "counted_quantity": None,
                "reason": r.choice(
                    ["shrinkage", "damage", "cycle_count_correction", "data_entry_fix"], n_adj
                ),
            }
        )
    )
    n_cnt = max(50, int(len(total) * 0.1))
    pick = r.sample_idx(len(total), n_cnt)
    frames.append(
        pd.DataFrame(
            {
                "event_type": "stock_count",
                "product_id": total["product_id"].values[pick],
                "warehouse_id": total["warehouse_id"].values[pick],
                "quantity_delta": 0,
                "event_ts": r.timestamps(cfg.start_date, cfg.end_date, n_cnt).values,
                "reference_id": [f"CNT-{i:06d}" for i in range(n_cnt)],
                "supplier_id": None,
                "unit_cost": None,
                "defective_qty": None,
                "counted_quantity": r.ints(0, 400, n_cnt),
                "reason": None,
            }
        )
    )

    # drop all-NA columns per frame so concat infers dtypes from real values only
    ev = pd.concat([f.dropna(axis=1, how="all") for f in frames], ignore_index=True)
    ev["event_ts"] = pd.to_datetime(ev["event_ts"])
    ev = ev.sort_values("event_ts", kind="stable").reset_index(drop=True)
    ev.insert(0, "event_id", [f"INV-{i + 1:08d}" for i in range(len(ev))])
    ev["source_system"] = "wms-v2"
    ev["event_ts"] = ev["event_ts"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    ev["quantity_delta"] = ev["quantity_delta"].astype(int)
    return ev
