"""End-to-end synthetic data generation with CDC-style batch splitting.

`generate_all()` builds the complete clean universe once, then materialises one raw
"delivery" per batch cutoff date:

  * customers / products: rows that were created or updated since the previous cutoff,
    presented as of the cutoff (later updates are hidden until the next batch).
  * orders: new orders plus late-arriving status updates for older orders.
  * items / payments / inventory / shipping: assigned by their business timestamp.
  * exchange rates: only the dates since the previous cutoff.

Defects are injected per batch and counted in data/raw/_manifest/<batch>.json.
The second batch also introduces a schema change (new product and order fields).
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ..logging_utils import get_logger
from .base import SYNTHETIC_NOTICE, GenConfig, Rng, cutoff_ts, parse_iso
from .corruption import (
    corrupt_customers,
    corrupt_inventory,
    corrupt_order_items,
    corrupt_orders,
    corrupt_payments,
    corrupt_products,
    corrupt_shipping,
)
from .customers import gen_customers
from .inventory import gen_inventory_events
from .orders import gen_orders
from .products import MATERIALS, gen_products
from .reference import gen_categories, gen_exchange_rates, gen_regions, gen_suppliers, gen_warehouses
from .shipping import gen_shipping_events
from .writers import write_csv, write_json_document, write_log, write_ndjson

log = get_logger("generators")

IN_FLIGHT_STATUS = {
    "delivered": "shipped",
    "returned": "shipped",
    "shipped": "paid",
    "paid": "placed",
    "cancelled": "placed",
    "placed": "placed",
}


def _batch_windows(cfg: GenConfig) -> list[tuple[date | None, date]]:
    prev: date | None = None
    out = []
    for c in cfg.batches:
        out.append((prev, c))
        prev = c
    return out


_ts = parse_iso


def generate_all(cfg: GenConfig, raw_dir: Path) -> dict[str, Any]:
    t0 = time.time()
    rng = Rng(cfg.seed)
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "SYNTHETIC_DATA_NOTICE.txt").write_text(SYNTHETIC_NOTICE + "\n", encoding="utf-8")

    log.info("generating clean universe", scale=cfg.scale, seed=cfg.seed)
    regions = gen_regions()
    categories = gen_categories()
    suppliers = gen_suppliers(cfg, rng)
    warehouses = gen_warehouses(cfg, rng)
    fx = gen_exchange_rates(cfg, rng)
    customers = gen_customers(cfg, rng)
    products = gen_products(cfg, rng, categories, suppliers)
    bundle = gen_orders(cfg, rng, customers, products, fx)
    inventory = gen_inventory_events(cfg, rng, products, warehouses, suppliers, bundle)
    shipping = gen_shipping_events(cfg, rng, bundle, warehouses)
    log.info(
        "clean universe ready",
        customers=len(customers),
        products=len(products),
        orders=len(bundle.orders),
        order_items=len(bundle.order_items),
        payments=len(bundle.payments),
        inventory_events=len(inventory),
        shipping_events=len(shipping),
        seconds=round(time.time() - t0, 1),
    )

    # Static reference data (served by the mock API)
    api_dir = raw_dir / "api"
    write_json_document(
        {"data": regions.to_dict("records"), "count": len(regions), "synthetic": True},
        api_dir / "regions.json",
    )
    write_json_document(
        {"data": warehouses.to_dict("records"), "count": len(warehouses), "synthetic": True},
        api_dir / "warehouses.json",
    )

    manifests = []
    orders_all = bundle.orders
    order_ts = _ts(orders_all["order_date"])
    order_upd = _ts(orders_all["updated_at"])
    cust_signup = _ts(customers["signup_date"])
    cust_upd = _ts(customers["updated_at"])
    prod_created = _ts(products["created_at"])
    prod_upd = _ts(products["updated_at"])
    items_order_ts = bundle.order_items["order_id"].map(
        dict(zip(orders_all["order_id"], order_ts, strict=True))
    )
    pay_ts = _ts(bundle.payments["paid_at"])
    inv_ts = _ts(inventory["event_ts"])
    ship_ts = _ts(shipping["event_ts"])
    valid_categories = set(categories["category_id"])

    for batch_no, (prev, cutoff) in enumerate(_batch_windows(cfg), start=1):
        bid = cutoff.isoformat()
        brng = rng.child(f"batch/{bid}")
        lo = cutoff_ts(prev) if prev else pd.Timestamp.min
        hi = cutoff_ts(cutoff)
        schema_v2 = batch_no >= 2
        report: dict[str, Any] = {"batch_id": bid, "schema_version": 2 if schema_v2 else 1, "datasets": {}}
        log.info("materialising batch", batch_id=bid, window=[str(lo.date()) if prev else None, bid])

        # ---- customers (CDC)
        m = (cust_signup <= hi) & ((cust_signup > lo) | (cust_upd > lo))
        cdf = customers[m].copy()
        cdf["updated_at"] = _ts(cdf["updated_at"]).clip(upper=hi).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        cdf, rep = corrupt_customers(cdf, brng, cfg.defect_rate_multiplier)
        n = write_csv(cdf, raw_dir / "customers" / f"customers_{bid}.csv")
        report["datasets"]["customers"] = {
            "rows": n,
            "clean_rows": int(m.sum()),
            "defects": rep,
            "file": f"customers/customers_{bid}.csv",
        }

        # ---- products (CDC, nested JSON with categories + suppliers sections)
        m = (prod_created <= hi) & ((prod_created > lo) | (prod_upd > lo))
        pdf = products[m].copy()
        pdf["updated_at"] = _ts(pdf["updated_at"]).clip(upper=hi).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        recs = pdf.drop(columns=["_popularity", "_declining"]).to_dict("records")
        if schema_v2:  # schema evolution: two new fields appear in the feed
            eco = brng.child("eco").ints(1, 6, len(recs))
            mats = brng.child("mat").choice(MATERIALS, len(recs))
            for rec, e, mt in zip(recs, eco, mats, strict=True):
                rec["eco_rating"] = int(e)
                rec["attributes"] = {**rec["attributes"], "material": str(mt)}
        recs, rep = corrupt_products(recs, brng, valid_categories, cfg.defect_rate_multiplier)
        doc = {
            "feed": "pim-product-catalog",
            "schema_version": 2 if schema_v2 else 1,
            "generated_at": hi.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "synthetic": True,
            "categories": categories.to_dict("records"),
            "suppliers": suppliers.drop(columns=["_latent_defect_rate"]).to_dict("records"),
            "products": recs,
        }
        write_json_document(doc, raw_dir / "products" / f"product_catalog_{bid}.json")
        report["datasets"]["products"] = {
            "rows": len(recs),
            "clean_rows": int(m.sum()),
            "defects": rep,
            "file": f"products/product_catalog_{bid}.json",
        }

        # ---- orders (new + late-arriving updates) and resent unchanged rows
        new_m = (order_ts > lo) & (order_ts <= hi)
        late_m = (order_ts <= lo) & (order_upd > lo) & (order_upd <= hi)
        odf = orders_all[new_m | late_m].copy()
        in_flight = _ts(odf["updated_at"]) > hi
        odf.loc[in_flight, "status"] = odf.loc[in_flight, "status"].map(IN_FLIGHT_STATUS)
        odf["updated_at"] = _ts(odf["updated_at"]).clip(upper=hi).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        resent = 0
        if prev is not None:
            # at-least-once producers retry recent records: resend 0.3% of the last 45 days
            prior = orders_all[(order_ts <= lo) & (order_ts > lo - pd.Timedelta(days=45)) & (order_upd <= lo)]
            idx = brng.child("resent").sample_idx(len(prior), int(len(prior) * 0.003))
            odf = pd.concat([odf, prior.iloc[idx]], ignore_index=True)
            resent = len(idx)
        if schema_v2:
            odf["promo_campaign"] = np.where(brng.child("promo").bool_mask(len(odf), 0.2), "WINTER-2025", "")
        odf, rep = corrupt_orders(odf, brng, cfg.defect_rate_multiplier)
        rep["resent_unchanged_from_previous_batch"] = resent
        rep["late_arriving_updates"] = int(late_m.sum())
        n = write_csv(odf, raw_dir / "orders" / f"orders_{bid}.csv")
        report["datasets"]["orders"] = {
            "rows": n,
            "clean_rows": int((new_m | late_m).sum()),
            "new_orders": int(new_m.sum()),
            "defects": rep,
            "file": f"orders/orders_{bid}.csv",
        }

        # ---- order items (with their order's first delivery)
        m = (items_order_ts > lo) & (items_order_ts <= hi)
        idf = bundle.order_items[m].copy()
        idf, rep = corrupt_order_items(idf, brng, cfg.defect_rate_multiplier)
        n = write_csv(idf, raw_dir / "orders" / f"order_items_{bid}.csv")
        report["datasets"]["order_items"] = {
            "rows": n,
            "clean_rows": int(m.sum()),
            "defects": rep,
            "file": f"orders/order_items_{bid}.csv",
        }

        # ---- payments
        m = (pay_ts > lo) & (pay_ts <= hi)
        pay = bundle.payments[m].copy()
        pay, rep = corrupt_payments(pay, brng, cfg.defect_rate_multiplier)
        n = write_csv(pay, raw_dir / "orders" / f"payments_{bid}.csv")
        report["datasets"]["payments"] = {
            "rows": n,
            "clean_rows": int(m.sum()),
            "defects": rep,
            "file": f"orders/payments_{bid}.csv",
        }

        # ---- inventory events (NDJSON with malformed lines)
        m = (inv_ts > lo) & (inv_ts <= hi)
        if prev is None:
            m = inv_ts <= hi  # opening stock receipts pre-date the window
        recs = inventory[m].to_dict("records")
        recs = [
            {k: (None if (isinstance(v, float) and np.isnan(v)) else v) for k, v in rec.items()}
            for rec in recs
        ]
        recs, rep = corrupt_inventory(recs, brng, cfg.defect_rate_multiplier)
        n, bad = write_ndjson(
            recs,
            raw_dir / "inventory" / f"inventory_events_{bid}.ndjson",
            brng,
            malformed_rate=0.003 * cfg.defect_rate_multiplier,
        )
        rep["malformed_json_line"] = bad
        report["datasets"]["inventory_events"] = {
            "rows": n,
            "clean_rows": int(m.sum()),
            "defects": rep,
            "file": f"inventory/inventory_events_{bid}.ndjson",
        }

        # ---- shipping events (log lines with malformed lines)
        m = (ship_ts > lo) & (ship_ts <= hi)
        sdf = shipping[m].copy()
        sdf, rep = corrupt_shipping(sdf, brng, cfg.defect_rate_multiplier)
        n, bad = write_log(
            sdf,
            raw_dir / "shipping" / f"shipping_events_{bid}.log",
            brng,
            malformed_rate=0.003 * cfg.defect_rate_multiplier,
        )
        rep["malformed_log_line"] = bad
        report["datasets"]["shipping_events"] = {
            "rows": n,
            "clean_rows": int(m.sum()),
            "defects": rep,
            "file": f"shipping/shipping_events_{bid}.log",
        }

        # ---- exchange rates via API (paginated response documents)
        fxm = (_ts(fx["rate_date"]) > lo) & (_ts(fx["rate_date"]) <= hi)
        fx_batch = fx[fxm]
        write_json_document(
            {
                "data": fx_batch.to_dict("records"),
                "count": len(fx_batch),
                "base_currency": "USD",
                "as_of": bid,
                "synthetic": True,
            },
            api_dir / f"exchange_rates_{bid}.json",
        )
        report["datasets"]["exchange_rates"] = {
            "rows": len(fx_batch),
            "file": f"api/exchange_rates_{bid}.json",
        }

        report["generated_at"] = datetime.now(timezone.utc).isoformat()
        (raw_dir / "_manifest").mkdir(exist_ok=True)
        (raw_dir / "_manifest" / f"{bid}.json").write_text(
            json.dumps(report, indent=2, default=str), encoding="utf-8"
        )
        manifests.append(report)
        log.info("batch written", batch_id=bid, **{k: v["rows"] for k, v in report["datasets"].items()})

    summary = {
        "config": {
            **asdict(cfg),
            "start_date": cfg.start_date.isoformat(),
            "end_date": cfg.end_date.isoformat(),
            "batches": [b.isoformat() for b in cfg.batches],
        },
        "batches": [m["batch_id"] for m in manifests],
        "seconds": round(time.time() - t0, 1),
        "synthetic": True,
    }
    (raw_dir / "_manifest" / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    log.info("generation complete", seconds=summary["seconds"])
    return summary
