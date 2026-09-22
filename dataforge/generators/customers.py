"""SOURCE 1 - customer master (CSV export from a CRM)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from .base import (
    EMAIL_DOMAINS,
    FIRST_NAMES,
    LAST_NAMES,
    STREET_TYPES,
    STREET_WORDS,
    GenConfig,
    Rng,
    make_ids,
    to_iso,
)
from .reference import REGIONS


def gen_customers(cfg: GenConfig, rng: Rng) -> pd.DataFrame:
    r = rng.child("customers")
    n = cfg.n_customers
    first = r.choice(FIRST_NAMES, n)
    last = r.choice(LAST_NAMES, n)
    # Region mix is skewed (NA + EU dominate), matching a realistic e-commerce footprint
    region_p = np.array([0.18, 0.14, 0.06, 0.13, 0.10, 0.06, 0.04, 0.10, 0.05, 0.05, 0.05, 0.04])
    region_idx = r.choice(len(REGIONS), n, p=region_p / region_p.sum())
    regions = np.array([REGIONS[i][0] for i in region_idx])
    countries = np.array([REGIONS[i][2] for i in region_idx])

    signup = r.timestamps(pd.Timestamp("2021-01-01").date(), cfg.end_date, n, trend=0.3)
    # updated_at is >= signup, most rows updated within a year of signup
    upd_days = r.ints(0, 400, n)
    updated = signup + pd.to_timedelta(upd_days, unit="D")
    updated = updated.where(updated <= pd.Timestamp(cfg.end_date), pd.Timestamp(cfg.end_date))
    birth_year = r.ints(1950, 2006, n)
    birth_month = r.ints(1, 13, n)
    birth_day = r.ints(1, 29, n)

    df = pd.DataFrame(
        {
            "customer_id": make_ids("CUST", n, width=6),
            "first_name": first,
            "last_name": last,
            "email": [
                f"{fn.lower()}.{ln.lower()}{i}@{r.choice(EMAIL_DOMAINS)}"
                for i, (fn, ln) in enumerate(zip(first, last, strict=True))
            ],
            "phone": [f"+{r.ints(1, 99)}-{r.ints(200, 999)}-{r.ints(1000, 9999)}" for _ in range(n)],
            "birth_date": [
                f"{y}-{m:02d}-{d:02d}" for y, m, d in zip(birth_year, birth_month, birth_day, strict=True)
            ],
            "signup_date": signup.dt.strftime("%Y-%m-%d"),
            "customer_segment": r.choice(["consumer", "business", "vip"], n, p=[0.82, 0.13, 0.05]),
            "country": countries,
            "region_code": regions,
            "city": [f"{w} City" for w in r.choice(STREET_WORDS, n)],
            "street_address": [
                f"{r.ints(1, 9999)} {w} {t}"
                for w, t in zip(r.choice(STREET_WORDS, n), r.choice(STREET_TYPES, n), strict=True)
            ],
            "marketing_opt_in": r.choice(["true", "false"], n, p=[0.55, 0.45]),
            "updated_at": to_iso(updated),
        }
    )
    return df
