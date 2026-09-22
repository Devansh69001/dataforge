"""SOURCE 2 - product catalog (nested JSON feed from a PIM system)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from .base import BRANDS, PRODUCT_ADJECTIVES, GenConfig, Rng, to_iso

COLORS = ["Black", "White", "Graphite", "Navy", "Olive", "Sand", "Crimson", "Teal"]
SIZES = ["XS", "S", "M", "L", "XL", "One Size"]
MATERIALS = ["aluminium", "cotton", "steel", "plastic", "wood", "leather", "polyester"]


def gen_products(cfg: GenConfig, rng: Rng, categories: pd.DataFrame, suppliers: pd.DataFrame) -> pd.DataFrame:
    r = rng.child("products")
    n = cfg.n_products
    leaf = categories[categories["level"] == 2].reset_index(drop=True)
    cat_idx = r.ints(0, len(leaf), n)
    sup_idx = r.ints(0, len(suppliers), n)
    price = np.round(np.exp(r.g.normal(3.4, 0.9, size=n)), 2)  # log-normal, median ~ $30
    price = np.clip(price, 2.5, 2500)
    margin = r.floats(0.25, 0.65, n)
    cost = np.round(price * (1 - margin), 2)
    created = r.timestamps(pd.Timestamp("2022-01-01").date(), cfg.start_date, n)
    # updates are spread uniformly until the end of the window so every batch sees some changes
    span_days = (pd.Timestamp(cfg.end_date) - created).dt.days.values
    updated = created + pd.to_timedelta(np.floor(r.g.random(n) * span_days).astype(int), unit="D")

    # popularity weights: heavy tail so a few products dominate sales
    popularity = r.pareto_weights(n, shape=1.1)
    # ~8% of products are on a declining trend during 2025 (drives "declining sales" analysis)
    declining = r.bool_mask(n, 0.08)

    rows = []
    for i in range(n):
        cat = leaf.iloc[cat_idx[i]]
        rows.append(
            {
                "product_id": f"PRD-{i + 1:05d}",
                "sku": f"{cat['category_name'][:3].upper()}-{r.ints(10000, 99999)}-{i + 1:05d}",
                "name": f"{r.choice(PRODUCT_ADJECTIVES)} {cat['category_name'].rstrip('s')} {r.ints(100, 999)}",
                "brand": BRANDS[int(r.ints(0, len(BRANDS)))],
                "category_id": cat["category_id"],
                "supplier_id": suppliers.iloc[sup_idx[i]]["supplier_id"],
                "unit_price": float(price[i]),
                "unit_cost": float(cost[i]),
                "currency": "USD",
                "weight_kg": round(float(np.exp(r.g.normal(0, 1))), 3),
                "dimensions_cm": {
                    "length": int(r.ints(5, 120)),
                    "width": int(r.ints(5, 80)),
                    "height": int(r.ints(2, 60)),
                },
                "attributes": {"color": str(r.choice(COLORS)), "size": str(r.choice(SIZES))},
                "is_active": bool(r.g.random() > 0.06),
                "created_at": None,
                "updated_at": None,
                "_popularity": float(popularity[i]),
                "_declining": bool(declining[i]),
            }
        )
    df = pd.DataFrame(rows)
    df["created_at"] = to_iso(created)
    df["updated_at"] = to_iso(updated)
    return df
