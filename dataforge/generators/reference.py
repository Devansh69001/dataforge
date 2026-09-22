"""Reference / master data: regions, categories, suppliers, warehouses, exchange rates.

Regions and exchange rates are exposed through the simulated REST API (SOURCE 5);
categories, suppliers and warehouses ride along with the product catalog feed.
"""

from __future__ import annotations

from datetime import timedelta

import numpy as np
import pandas as pd

from .base import BRANDS, GenConfig, Rng

# region_code, region_name, country_code, country_name, currency, base delivery days
REGIONS = [
    ("NA-EAST", "North America East", "US", "United States", "USD", 2.5),
    ("NA-WEST", "North America West", "US", "United States", "USD", 3.0),
    ("NA-CA", "Canada", "CA", "Canada", "CAD", 3.5),
    ("EU-WEST", "Western Europe", "DE", "Germany", "EUR", 3.0),
    ("EU-UK", "United Kingdom", "GB", "United Kingdom", "GBP", 2.5),
    ("EU-SOUTH", "Southern Europe", "ES", "Spain", "EUR", 4.0),
    ("EU-NORTH", "Nordics", "SE", "Sweden", "SEK", 3.5),
    ("APAC-IN", "India", "IN", "India", "INR", 4.5),
    ("APAC-SG", "Southeast Asia", "SG", "Singapore", "SGD", 4.0),
    ("APAC-AU", "Australia & NZ", "AU", "Australia", "AUD", 5.0),
    ("LATAM-BR", "Brazil", "BR", "Brazil", "BRL", 6.0),
    ("MEA-AE", "Middle East", "AE", "United Arab Emirates", "AED", 4.5),
]

# Dirty spellings that appear in raw customer/order files; silver maps them back to ISO codes.
COUNTRY_ALIASES = {
    "US": ["United States", "united states", "USA", "U.S.", "US", "United States of America", " usa "],
    "CA": ["Canada", "canada", "CA", "CAN"],
    "DE": ["Germany", "germany", "DE", "Deutschland"],
    "GB": ["United Kingdom", "UK", "U.K.", "Great Britain", "GB", "united kingdom"],
    "ES": ["Spain", "spain", "ES", "España"],
    "SE": ["Sweden", "sweden", "SE"],
    "IN": ["India", "india", "IN", "IND"],
    "SG": ["Singapore", "singapore", "SG"],
    "AU": ["Australia", "australia", "AU", "AUS"],
    "BR": ["Brazil", "brazil", "BR", "Brasil"],
    "AE": ["United Arab Emirates", "UAE", "U.A.E.", "AE"],
}

CATEGORY_TREE = {
    "Electronics": ["Headphones", "Smartphones", "Laptops", "Wearables", "Cameras"],
    "Home & Kitchen": ["Cookware", "Small Appliances", "Bedding", "Storage", "Lighting"],
    "Sports & Outdoors": ["Fitness Equipment", "Cycling", "Camping", "Running", "Water Sports"],
    "Fashion": ["Footwear", "Outerwear", "Accessories", "Activewear", "Denim"],
    "Beauty & Health": ["Skincare", "Haircare", "Supplements", "Grooming", "Oral Care"],
    "Toys & Games": ["Board Games", "Building Sets", "Puzzles", "Outdoor Play", "Educational"],
    "Office": ["Desks & Chairs", "Stationery", "Printers", "Organisation", "Monitors"],
    "Garden & Tools": ["Power Tools", "Hand Tools", "Planters", "Grills", "Garden Furniture"],
}

WAREHOUSE_CITIES = [
    ("Newark", "NA-EAST"),
    ("Reno", "NA-WEST"),
    ("Toronto", "NA-CA"),
    ("Leipzig", "EU-WEST"),
    ("Coventry", "EU-UK"),
    ("Zaragoza", "EU-SOUTH"),
    ("Gothenburg", "EU-NORTH"),
    ("Pune", "APAC-IN"),
    ("Johor Bahru", "APAC-SG"),
    ("Melbourne", "APAC-AU"),
    ("Campinas", "LATAM-BR"),
    ("Dubai", "MEA-AE"),
]

BASE_USD_RATES = {
    "USD": 1.0,
    "CAD": 1.36,
    "EUR": 0.92,
    "GBP": 0.79,
    "SEK": 10.6,
    "INR": 83.5,
    "SGD": 1.35,
    "AUD": 1.52,
    "BRL": 5.1,
    "AED": 3.67,
}


def gen_regions() -> pd.DataFrame:
    rows = [
        {
            "region_code": rc,
            "region_name": rn,
            "country_code": cc,
            "country_name": cn,
            "currency_code": cur,
            "base_delivery_days": bd,
            "timezone": "UTC",
        }
        for rc, rn, cc, cn, cur, bd in REGIONS
    ]
    return pd.DataFrame(rows)


def gen_categories() -> pd.DataFrame:
    rows = []
    cid = 1
    for parent, children in CATEGORY_TREE.items():
        parent_id = f"CAT-{cid:03d}"
        rows.append(
            {"category_id": parent_id, "category_name": parent, "parent_category_id": None, "level": 1}
        )
        cid += 1
        for child in children:
            rows.append(
                {
                    "category_id": f"CAT-{cid:03d}",
                    "category_name": child,
                    "parent_category_id": parent_id,
                    "level": 2,
                }
            )
            cid += 1
    return pd.DataFrame(rows)


def gen_suppliers(cfg: GenConfig, rng: Rng) -> pd.DataFrame:
    r = rng.child("suppliers")
    n = cfg.n_suppliers
    regions = gen_regions()
    reg_idx = r.ints(0, len(regions), n)
    # latent defect rate: most suppliers good, a long tail bad -> drives supplier_performance mart
    defect_rate = np.clip(r.g.beta(1.2, 40, size=n), 0.001, 0.25)
    rows = []
    for i in range(n):
        reg = regions.iloc[reg_idx[i]]
        rows.append(
            {
                "supplier_id": f"SUP-{i + 1:04d}",
                "supplier_name": f"{BRANDS[i % len(BRANDS)]} {['Supply', 'Trading', 'Industries', 'Goods', 'Manufacturing'][i % 5]} {i + 1}",
                "country_code": reg["country_code"],
                "region_code": reg["region_code"],
                "lead_time_days": int(r.ints(3, 45)),
                "quality_tier": "A" if defect_rate[i] < 0.02 else ("B" if defect_rate[i] < 0.06 else "C"),
                "_latent_defect_rate": float(defect_rate[i]),  # internal, not exported to raw
                "active": bool(r.g.random() > 0.05),
            }
        )
    return pd.DataFrame(rows)


def gen_warehouses(cfg: GenConfig, rng: Rng) -> pd.DataFrame:
    r = rng.child("warehouses")
    rows = []
    for i, (city, region) in enumerate(WAREHOUSE_CITIES[: cfg.n_warehouses]):
        rows.append(
            {
                "warehouse_id": f"WH-{i + 1:02d}",
                "warehouse_name": f"{city} Fulfilment Center",
                "city": city,
                "region_code": region,
                "capacity_units": int(r.ints(80_000, 400_000)),
                "opened_date": (
                    pd.Timestamp("2018-01-01") + pd.Timedelta(days=int(r.ints(0, 2000)))
                ).strftime("%Y-%m-%d"),
            }
        )
    return pd.DataFrame(rows)


def gen_exchange_rates(cfg: GenConfig, rng: Rng) -> pd.DataFrame:
    """Daily USD-based rates as a slow random walk. A few days are deliberately missing
    (API outage) so the silver layer has to forward-fill."""
    r = rng.child("fx")
    days = cfg.days
    dates = [cfg.start_date + timedelta(days=i) for i in range(days)]
    rows = []
    for cur, base in BASE_USD_RATES.items():
        steps = r.g.normal(0, 0.002, size=days).cumsum()
        rates = base * (1 + steps)
        for d, rate in zip(dates, rates, strict=True):
            rows.append(
                {"rate_date": d.isoformat(), "currency_code": cur, "usd_per_unit": round(1 / rate, 6)}
            )
    df = pd.DataFrame(rows)
    missing_days = set(r.choice(days, size=max(1, days // 60), replace=False).tolist())
    keep = ~df["rate_date"].isin({dates[i].isoformat() for i in missing_days})
    return df[keep].reset_index(drop=True)
