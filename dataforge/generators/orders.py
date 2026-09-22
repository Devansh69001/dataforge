"""SOURCE 3 - orders, order items and payments (CSV extracts from the order management system).

Orders are priced in the customer's regional currency using the daily FX feed, so the
gold layer has to convert back to USD with the reference rates (SOURCE 5).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .base import CHANNELS, PAYMENT_METHODS, GenConfig, Rng, make_ids, to_iso
from .reference import BASE_USD_RATES, REGIONS

REGION_CURRENCY = {rc: cur for rc, _, _, _, cur, _ in REGIONS}


@dataclass
class OrderBundle:
    orders: pd.DataFrame
    order_items: pd.DataFrame
    payments: pd.DataFrame


def _fx_lookup(fx: pd.DataFrame, cfg: GenConfig) -> pd.DataFrame:
    """Dense (date x currency) units_per_usd table with outage days forward-filled."""
    dense = fx.pivot(index="rate_date", columns="currency_code", values="usd_per_unit")
    all_days = pd.date_range(cfg.start_date, cfg.end_date, freq="D").strftime("%Y-%m-%d")
    dense = dense.reindex(all_days).ffill().bfill()
    for cur, base in BASE_USD_RATES.items():
        if cur not in dense:
            dense[cur] = 1 / base
    return 1.0 / dense  # units of local currency per USD


def _seasonal_timestamps(cfg: GenConfig, r: Rng, n: int) -> pd.Series:
    """Order timestamps with growth trend, weekly pattern and Q4 peaks."""
    days = cfg.days
    day_idx = np.arange(days)
    dates = pd.to_datetime(cfg.start_date) + pd.to_timedelta(day_idx, unit="D")
    growth = 1.0 + 0.6 * day_idx / days  # +60% daily volume by the end of the window
    weekday = dates.dayofweek.values
    weekly = np.where(weekday >= 5, 0.8, 1.0)  # weekends quieter
    month = dates.month.values
    seasonal = np.where(month == 11, 1.45, np.where(month == 12, 1.35, np.where(month == 1, 0.8, 1.0)))
    weights = growth * weekly * seasonal
    weights = weights / weights.sum()
    chosen = r.choice(days, n, p=weights)
    hours = r.choice(24, n, p=_hour_profile())
    secs = r.ints(0, 3600, n)
    return pd.Series(
        pd.to_datetime(cfg.start_date)
        + pd.to_timedelta(chosen, unit="D")
        + pd.to_timedelta(hours, unit="h")
        + pd.to_timedelta(secs, unit="s")
    )


def _hour_profile() -> np.ndarray:
    p = np.array([1, 1, 1, 1, 1, 2, 3, 5, 7, 8, 9, 9, 9, 8, 8, 8, 9, 10, 11, 12, 11, 8, 5, 3], dtype=float)
    return p / p.sum()


def gen_orders(
    cfg: GenConfig, rng: Rng, customers: pd.DataFrame, products: pd.DataFrame, fx: pd.DataFrame
) -> OrderBundle:
    r = rng.child("orders")
    n = cfg.n_orders
    order_ts = _seasonal_timestamps(cfg, r, n).sort_values().reset_index(drop=True)

    # Customers: heavy-tailed activity; only customers who signed up before the order can buy
    cust_weights = r.pareto_weights(len(customers), shape=1.3)
    cust_idx = r.choice(len(customers), n, p=cust_weights)
    signup = pd.to_datetime(customers["signup_date"].values)[cust_idx]
    late = order_ts.values < signup
    if late.any():
        # re-draw against early sign-ups for the few conflicts
        early_pool = np.where(pd.to_datetime(customers["signup_date"]) < pd.Timestamp(cfg.start_date))[0]
        cust_idx[late] = r.choice(early_pool, late.sum())

    cust = customers.iloc[cust_idx].reset_index(drop=True)
    region = cust["region_code"].values
    currency = np.array([REGION_CURRENCY[rc] for rc in region])

    age_days = (pd.Timestamp(cfg.end_date) - order_ts).dt.days.values
    status = np.where(
        age_days > 14,
        r.choice(["delivered", "cancelled", "returned", "shipped"], n, p=[0.86, 0.06, 0.05, 0.03]),
        np.where(
            age_days > 5,
            r.choice(["shipped", "delivered", "paid", "cancelled"], n, p=[0.45, 0.35, 0.12, 0.08]),
            r.choice(["placed", "paid", "shipped"], n, p=[0.35, 0.45, 0.20]),
        ),
    )
    status_lag = np.select(
        [status == "placed", status == "paid", status == "shipped", status == "delivered"],
        [0, 1, r.ints(1, 3, n), r.ints(3, 10, n)],
        default=r.ints(5, 20, n),
    )
    updated_ts = (
        order_ts + pd.to_timedelta(status_lag, unit="D") + pd.to_timedelta(r.ints(0, 86400, n), unit="s")
    )
    updated_ts = updated_ts.where(
        updated_ts <= pd.Timestamp(cfg.end_date) + pd.Timedelta(hours=23),
        pd.Timestamp(cfg.end_date) + pd.Timedelta(hours=23),
    )

    orders = pd.DataFrame(
        {
            "order_id": make_ids("ORD", n, width=7),
            "customer_id": cust["customer_id"].values,
            "order_date": order_ts,
            "status": status,
            "channel": r.choice(CHANNELS, n, p=[0.5, 0.3, 0.12, 0.08]),
            "currency": currency,
            "region_code": region,
            "shipping_country": cust["country"].values,
            "coupon_code": np.where(
                r.bool_mask(n, 0.12), r.choice(["WELCOME10", "SPRING15", "VIP20", "FREESHIP"], n), ""
            ),
            "updated_at": updated_ts,
        }
    )

    # ---------------------------------------------------------------- items
    n_items = np.minimum(r.g.geometric(0.45, size=n), 8)
    total_items = int(n_items.sum())
    order_pos = np.repeat(np.arange(n), n_items)
    pop = products["_popularity"].values
    declining = products["_declining"].values
    prod_idx = r.choice(len(products), total_items, p=pop)
    # declining products lose share progressively through 2025
    item_ts = order_ts.values[order_pos]
    frac_2025 = np.clip((pd.to_datetime(item_ts) - pd.Timestamp("2025-01-01")).days / 365.0, 0, 1)
    resample = declining[prod_idx] & (r.g.random(total_items) < 0.75 * frac_2025)
    if resample.any():
        healthy = np.where(~declining)[0]
        prod_idx[resample] = r.choice(healthy, resample.sum(), p=pop[healthy] / pop[healthy].sum())

    fx_dense = _fx_lookup(fx, cfg)
    order_day = pd.to_datetime(item_ts).strftime("%Y-%m-%d")
    item_currency = currency[order_pos]
    rate = fx_dense.to_numpy()[
        fx_dense.index.get_indexer(order_day), fx_dense.columns.get_indexer(item_currency)
    ]
    unit_price_usd = products["unit_price"].values[prod_idx]
    unit_price_local = np.round(unit_price_usd * rate, 2)
    qty = r.choice([1, 2, 3, 4, 5], total_items, p=[0.62, 0.22, 0.09, 0.04, 0.03])
    disc = r.choice([0, 5, 10, 15, 20], total_items, p=[0.7, 0.1, 0.1, 0.06, 0.04])
    line_total = np.round(qty * unit_price_local * (1 - disc / 100), 2)

    order_items = pd.DataFrame(
        {
            "order_item_id": make_ids("OI", total_items, width=8),
            "order_id": orders["order_id"].values[order_pos],
            "product_id": products["product_id"].values[prod_idx],
            "quantity": qty,
            "unit_price": unit_price_local,
            "discount_pct": disc,
            "line_total": line_total,
            "currency": item_currency,
        }
    )
    totals = order_items.groupby("order_id", sort=False)["line_total"].sum()
    orders["order_total"] = np.round(orders["order_id"].map(totals).values, 2)

    # ------------------------------------------------------------- payments
    paid_mask = orders["status"].isin(["paid", "shipped", "delivered", "returned"]).values
    paid = orders[paid_mask]
    method = r.choice(PAYMENT_METHODS, len(paid), p=[0.45, 0.2, 0.2, 0.1, 0.05])
    pay_rows = [
        pd.DataFrame(
            {
                "payment_id": [f"PAY-{oid[4:]}-1" for oid in paid["order_id"]],
                "order_id": paid["order_id"].values,
                "payment_method": method,
                "amount": paid["order_total"].values,
                "currency": paid["currency"].values,
                "status": "captured",
                "paid_at": paid["order_date"].values + pd.to_timedelta(r.ints(30, 1800, len(paid)), unit="s"),
            }
        )
    ]
    # failed first attempts
    failed = paid[r.bool_mask(len(paid), 0.04)]
    pay_rows.append(
        pd.DataFrame(
            {
                "payment_id": [f"PAY-{oid[4:]}-0" for oid in failed["order_id"]],
                "order_id": failed["order_id"].values,
                "payment_method": r.choice(PAYMENT_METHODS, len(failed)),
                "amount": failed["order_total"].values,
                "currency": failed["currency"].values,
                "status": "failed",
                "paid_at": failed["order_date"].values
                + pd.to_timedelta(r.ints(5, 25, len(failed)), unit="s"),
            }
        )
    )
    returned = orders[orders["status"] == "returned"]
    pay_rows.append(
        pd.DataFrame(
            {
                "payment_id": [f"PAY-{oid[4:]}-R" for oid in returned["order_id"]],
                "order_id": returned["order_id"].values,
                "payment_method": "refund",
                "amount": -returned["order_total"].values,
                "currency": returned["currency"].values,
                "status": "refunded",
                "paid_at": returned["updated_at"].values,
            }
        )
    )
    payments = pd.concat(pay_rows, ignore_index=True)
    payments["paid_at"] = pd.to_datetime(payments["paid_at"])
    payments = payments.sort_values("paid_at", kind="stable").reset_index(drop=True)

    orders["order_date"] = to_iso(orders["order_date"])
    orders["updated_at"] = to_iso(orders["updated_at"])
    payments["paid_at"] = to_iso(payments["paid_at"])
    return OrderBundle(orders=orders, order_items=order_items, payments=payments)
