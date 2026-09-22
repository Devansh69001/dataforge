"""Controlled data-quality defect injection.

Each `corrupt_*` function mutates a small, seeded fraction of a clean DataFrame and
returns a dict {defect_name: rows_affected}. The counts are written to the batch
manifest so tests can prove the quality layer catches what was injected.

Two families of defects:
  * REJECT-class: violate a hard rule and must land in quarantine
    (negative quantity, orphan FK, invalid date, unknown enum, missing key ...)
  * NORMALISE-class: messy but recoverable and must be fixed in silver
    (casing, whitespace, country aliases, exact duplicates, recomputable totals ...)
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .base import Rng
from .reference import COUNTRY_ALIASES

Report = dict[str, int]


def _pick(r: Rng, n: int, rate: float, mult: float) -> np.ndarray:
    k = int(round(n * rate * mult))
    return r.sample_idx(n, k)


def _dup_rows(df: pd.DataFrame, idx: np.ndarray) -> pd.DataFrame:
    if len(idx) == 0:
        return df
    return pd.concat([df, df.iloc[idx]], ignore_index=True)


# ----------------------------------------------------------------------------- customers
def corrupt_customers(df: pd.DataFrame, rng: Rng, mult: float = 1.0) -> tuple[pd.DataFrame, Report]:
    r = rng.child("corrupt/customers")
    rep: Report = {}
    n = len(df)
    df = df.reset_index(drop=True)  # positional indexing below

    idx = _pick(r, n, 0.02, mult)  # country spelled inconsistently -> normalise
    for i in idx:
        cc = df.at[i, "country"]
        df.at[i, "country"] = str(r.choice(COUNTRY_ALIASES.get(cc, [cc])))
    rep["country_alias"] = len(idx)

    idx = _pick(r, n, 0.015, mult)  # email casing / whitespace -> normalise
    df.loc[idx, "email"] = df.loc[idx, "email"].str.upper().radd("  ")
    rep["email_casing_whitespace"] = len(idx)

    idx = _pick(r, n, 0.01, mult)  # name whitespace padding -> normalise
    df.loc[idx, "first_name"] = " " + df.loc[idx, "first_name"] + "  "
    rep["name_whitespace"] = len(idx)

    idx = _pick(r, n, 0.01, mult)  # missing email -> allowed (warn)
    df.loc[idx, "email"] = None
    rep["missing_email"] = len(idx)

    idx = _pick(r, n, 0.005, mult)  # invalid email format -> reject
    df.loc[idx, "email"] = "not-an-email"
    rep["invalid_email"] = len(idx)

    idx = _pick(r, n, 0.005, mult)  # unparseable signup date -> reject
    df.loc[idx, "signup_date"] = r.choice(["2024-02-30", "31/12/2023", "N/A", "2023-13-01"], len(idx))
    rep["invalid_signup_date"] = len(idx)

    idx = _pick(r, n, 0.002, mult)  # missing primary key -> reject
    df.loc[idx, "customer_id"] = None
    rep["missing_customer_id"] = len(idx)

    idx = _pick(r, n, 0.003, mult)  # same id re-sent with newer updated_at -> keep latest
    newer = df.iloc[idx].copy()
    newer["city"] = newer["city"] + " (moved)"
    newer["updated_at"] = (pd.to_datetime(newer["updated_at"]) + pd.Timedelta(days=3)).dt.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    df = pd.concat([df, newer], ignore_index=True)
    rep["updated_duplicate"] = len(idx)

    idx = _pick(r, n, 0.005, mult)  # exact duplicate rows -> dedup
    df = _dup_rows(df, idx)
    rep["exact_duplicate"] = len(idx)
    return df, rep


# ------------------------------------------------------------------------------ products
def corrupt_products(
    recs: list[dict[str, Any]], rng: Rng, valid_categories: set[str], mult: float = 1.0
) -> tuple[list[dict], Report]:
    r = rng.child("corrupt/products")
    rep: Report = {}
    n = len(recs)
    recs = [dict(x) for x in recs]

    idx = _pick(r, n, 0.02, mult)  # brand casing -> normalise
    for i in idx:
        recs[i]["brand"] = recs[i]["brand"].upper() if r.g.random() < 0.5 else recs[i]["brand"].lower()
    rep["brand_casing"] = len(idx)

    idx = _pick(r, n, 0.005, mult)  # non-positive price -> reject
    for i in idx:
        recs[i]["unit_price"] = float(r.choice([0.0, -9.99, -1.0]))
    rep["non_positive_price"] = len(idx)

    idx = _pick(r, n, 0.003, mult)  # price typed as string with currency symbol -> reject (type violation)
    for i in idx:
        recs[i]["unit_price"] = f"${recs[i]['unit_price']}"
    rep["price_as_string"] = len(idx)

    idx = _pick(r, n, 0.005, mult)  # unknown category -> reject
    for i in idx:
        recs[i]["category_id"] = "CAT-999"
    rep["invalid_category"] = len(idx)

    idx = _pick(r, n, 0.005, mult)  # unknown supplier -> reject
    for i in idx:
        recs[i]["supplier_id"] = "SUP-9999"
    rep["invalid_supplier"] = len(idx)

    idx = _pick(r, n, 0.005, mult)  # malformed nested field -> warn (dimensions nulled)
    for i in idx:
        d = recs[i]["dimensions_cm"]
        recs[i]["dimensions_cm"] = f"{d['length']}x{d['width']}x{d['height']}"
    rep["malformed_dimensions"] = len(idx)

    idx = _pick(r, n, 0.003, mult)  # cost above price -> warn (negative margin flag)
    for i in idx:
        recs[i]["unit_cost"] = (
            round(float(recs[i]["unit_price"]) * 1.2, 2)
            if isinstance(recs[i]["unit_price"], float)
            else recs[i]["unit_cost"]
        )
    rep["cost_above_price"] = len(idx)

    idx = _pick(r, n, 0.005, mult)  # re-sent product with newer updated_at -> keep latest
    for i in idx:
        newer = dict(recs[i])
        newer["unit_price"] = (
            round(float(newer["unit_price"]) * 1.05, 2)
            if isinstance(newer["unit_price"], float)
            else newer["unit_price"]
        )
        newer["updated_at"] = (pd.Timestamp(newer["updated_at"]) + pd.Timedelta(days=2)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        recs.append(newer)
    rep["updated_duplicate"] = len(idx)
    return recs, rep


# -------------------------------------------------------------------------------- orders
def corrupt_orders(df: pd.DataFrame, rng: Rng, mult: float = 1.0) -> tuple[pd.DataFrame, Report]:
    r = rng.child("corrupt/orders")
    rep: Report = {}
    n = len(df)
    df = df.reset_index(drop=True)  # positional indexing below

    idx = _pick(r, n, 0.03, mult)  # status casing / whitespace -> normalise
    df.loc[idx, "status"] = df.loc[idx, "status"].str.upper().radd(" ").add(" ")
    rep["status_casing"] = len(idx)

    idx = _pick(r, n, 0.01, mult)  # currency lower-case -> normalise
    df.loc[idx, "currency"] = df.loc[idx, "currency"].str.lower()
    rep["currency_casing"] = len(idx)

    idx = _pick(r, n, 0.01, mult)  # missing region -> warn, derived from customer in silver
    df.loc[idx, "region_code"] = None
    rep["missing_region"] = len(idx)

    idx = _pick(r, n, 0.005, mult)  # orphan customer -> reject
    df.loc[idx, "customer_id"] = [f"CUST-9{r.ints(10000, 99999)}" for _ in idx]
    rep["orphan_customer"] = len(idx)

    idx = _pick(r, n, 0.003, mult)  # unparseable date -> reject
    df.loc[idx, "order_date"] = r.choice(
        ["2025-13-45T00:00:00Z", "not_a_date", "2024-02-30T10:00:00Z", ""], len(idx)
    )
    rep["invalid_order_date"] = len(idx)

    idx = _pick(r, n, 0.002, mult)  # future date -> reject
    df.loc[idx, "order_date"] = "2031-01-01T00:00:00Z"
    rep["future_order_date"] = len(idx)

    idx = _pick(r, n, 0.002, mult)  # negative total -> reject
    df.loc[idx, "order_total"] = -abs(df.loc[idx, "order_total"].astype(float))
    rep["negative_total"] = len(idx)

    idx = _pick(r, n, 0.002, mult)  # unknown status -> reject
    df.loc[idx, "status"] = "unknown"
    rep["unknown_status"] = len(idx)

    idx = _pick(
        r, n, 0.002, mult
    )  # same order_id, conflicting content, older updated_at -> dedup keeps newest
    conflict = df.iloc[idx].copy()
    conflict["status"] = "placed"
    conflict["updated_at"] = (
        pd.to_datetime(conflict["updated_at"], errors="coerce") - pd.Timedelta(days=1)
    ).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df = pd.concat([df, conflict], ignore_index=True)
    rep["conflicting_duplicate"] = len(idx)

    idx = _pick(r, n, 0.005, mult)  # exact duplicates -> dedup
    df = _dup_rows(df, idx)
    rep["exact_duplicate"] = len(idx)
    return df, rep


def corrupt_order_items(df: pd.DataFrame, rng: Rng, mult: float = 1.0) -> tuple[pd.DataFrame, Report]:
    r = rng.child("corrupt/order_items")
    rep: Report = {}
    n = len(df)
    df = df.reset_index(drop=True)  # positional indexing below

    idx = _pick(r, n, 0.005, mult)  # non-positive quantity -> reject
    df.loc[idx, "quantity"] = r.choice([0, -1, -3], len(idx))
    rep["non_positive_quantity"] = len(idx)

    idx = _pick(r, n, 0.005, mult)  # unknown product -> reject
    df.loc[idx, "product_id"] = "PRD-99999"
    rep["invalid_product"] = len(idx)

    idx = _pick(r, n, 0.003, mult)  # orphan order -> reject
    df.loc[idx, "order_id"] = "ORD-9999999"
    rep["orphan_order"] = len(idx)

    idx = _pick(r, n, 0.003, mult)  # non-positive price -> reject
    df.loc[idx, "unit_price"] = 0.0
    rep["non_positive_price"] = len(idx)

    idx = _pick(r, n, 0.005, mult)  # line_total disagrees with qty*price*(1-disc) -> warn + recompute
    df.loc[idx, "line_total"] = df.loc[idx, "line_total"].astype(float) + 7.77
    rep["line_total_mismatch"] = len(idx)

    idx = _pick(r, n, 0.003, mult)  # exact duplicate line -> dedup
    df = _dup_rows(df, idx)
    rep["exact_duplicate"] = len(idx)
    return df, rep


def corrupt_payments(df: pd.DataFrame, rng: Rng, mult: float = 1.0) -> tuple[pd.DataFrame, Report]:
    r = rng.child("corrupt/payments")
    rep: Report = {}
    n = len(df)
    df = df.reset_index(drop=True)  # positional indexing below

    idx = _pick(r, n, 0.003, mult)  # orphan order -> reject
    df.loc[idx, "order_id"] = "ORD-8888888"
    rep["orphan_order"] = len(idx)

    idx = _pick(r, n, 0.002, mult)  # unknown method -> reject
    df.loc[idx, "payment_method"] = "crypto_coupon"
    rep["invalid_method"] = len(idx)

    idx = _pick(r, n, 0.004, mult)  # amount mismatch vs order -> warn
    df.loc[idx, "amount"] = df.loc[idx, "amount"].astype(float) + 1.5
    rep["amount_mismatch"] = len(idx)

    idx = _pick(r, n, 0.002, mult)  # exact duplicate -> dedup
    df = _dup_rows(df, idx)
    rep["exact_duplicate"] = len(idx)
    return df, rep


# ----------------------------------------------------------------------- inventory events
def corrupt_inventory(recs: list[dict[str, Any]], rng: Rng, mult: float = 1.0) -> tuple[list[dict], Report]:
    r = rng.child("corrupt/inventory")
    rep: Report = {}
    n = len(recs)
    recs = [dict(x) for x in recs]

    idx = _pick(r, n, 0.003, mult)  # unknown event type -> reject
    for i in idx:
        recs[i]["event_type"] = "teleport"
    rep["unknown_event_type"] = len(idx)

    idx = _pick(r, n, 0.004, mult)  # unknown product -> reject
    for i in idx:
        recs[i]["product_id"] = "PRD-99998"
    rep["invalid_product"] = len(idx)

    idx = _pick(r, n, 0.003, mult)  # unknown warehouse -> reject
    for i in idx:
        recs[i]["warehouse_id"] = "WH-99"
    rep["invalid_warehouse"] = len(idx)

    idx = _pick(r, n, 0.003, mult)  # invalid timestamp -> reject
    for i in idx:
        recs[i]["event_ts"] = str(r.choice(["2025-02-30T00:00:00Z", "yesterday", ""]))
    rep["invalid_timestamp"] = len(idx)

    idx = _pick(r, n, 0.003, mult)  # sign disagrees with event type -> reject
    for i in idx:
        if recs[i]["event_type"] in ("receipt", "shipment"):
            recs[i]["quantity_delta"] = -int(recs[i]["quantity_delta"])
    rep["wrong_sign"] = len(idx)

    idx = _pick(r, n, 0.002, mult)  # missing event id -> reject
    for i in idx:
        recs[i]["event_id"] = None
    rep["missing_event_id"] = len(idx)

    idx = _pick(r, n, 0.005, mult)  # exact duplicate events (at-least-once delivery) -> dedup
    recs.extend(dict(recs[i]) for i in idx)
    rep["exact_duplicate"] = len(idx)
    return recs, rep


# ------------------------------------------------------------------------ shipping events
def corrupt_shipping(df: pd.DataFrame, rng: Rng, mult: float = 1.0) -> tuple[pd.DataFrame, Report]:
    r = rng.child("corrupt/shipping")
    rep: Report = {}
    n = len(df)
    df = df.reset_index(drop=True)  # positional indexing below

    idx = _pick(r, n, 0.003, mult)  # missing order id -> reject
    df.loc[idx, "order_id"] = ""
    rep["missing_order_id"] = len(idx)

    idx = _pick(r, n, 0.003, mult)  # orphan order -> reject
    df.loc[idx, "order_id"] = "ORD-7777777"
    rep["orphan_order"] = len(idx)

    idx = _pick(r, n, 0.002, mult)  # unknown event -> reject
    df.loc[idx, "event"] = "lost_in_space"
    rep["unknown_event"] = len(idx)

    idx = _pick(r, n, 0.002, mult)  # event casing -> normalise
    df.loc[idx, "event"] = df.loc[idx, "event"].str.upper()
    rep["event_casing"] = len(idx)

    idx = _pick(r, n, 0.005, mult)  # duplicate lines (log shipper retry) -> dedup
    df = _dup_rows(df, idx)
    rep["exact_duplicate"] = len(idx)
    return df, rep
