"""SOURCE 6 - shipping tracker events as application log lines.

Format (one event per line):
    2025-03-04T11:22:03Z INFO [shipping-tracker] event=in_transit shipment_id=SHP-0001234 order_id=ORD-0001234 \
    carrier="NorthStar Logistics" warehouse_id=WH-04 region=EU-WEST location="Leipzig Hub"
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .base import CARRIERS, GenConfig, Rng, parse_iso
from .orders import OrderBundle
from .reference import REGIONS

REGION_BASE_DAYS = {rc: bd for rc, _, _, _, _, bd in REGIONS}
CARRIER_FACTOR = {
    "SwiftParcel": 0.85,
    "GlobeFreight": 1.2,
    "NorthStar Logistics": 1.0,
    "BlueRoute": 0.95,
    "ParcelHive": 1.1,
}


def gen_shipping_events(
    cfg: GenConfig, rng: Rng, bundle: OrderBundle, warehouses: pd.DataFrame
) -> pd.DataFrame:
    """Returns a DataFrame of parsed events; the writer renders them as log lines."""
    r = rng.child("shipping")
    orders = bundle.orders
    ship = orders[orders["status"].isin(["shipped", "delivered", "returned"])].reset_index(drop=True)
    n = len(ship)
    wh_by_region = warehouses.set_index("region_code")["warehouse_id"].to_dict()
    wh_city = warehouses.set_index("warehouse_id")["city"].to_dict()

    carrier = r.choice(CARRIERS, n, p=[0.3, 0.15, 0.25, 0.2, 0.1])
    order_ts = parse_iso(ship["order_date"])
    label_ts = order_ts + pd.to_timedelta(r.ints(2, 48, n), unit="h")
    transit_ts = label_ts + pd.to_timedelta(r.ints(4, 30, n), unit="h")
    base_days = np.array([REGION_BASE_DAYS[rc] for rc in ship["region_code"]])
    cfactor = np.array([CARRIER_FACTOR[c] for c in carrier])
    transit_days = base_days * cfactor * np.exp(r.g.normal(0, 0.25, size=n))
    exception = r.bool_mask(n, 0.06)
    transit_days = transit_days + np.where(exception, r.floats(1, 4, n), 0)
    delivered_ts = label_ts + pd.to_timedelta(transit_days * 24, unit="h")
    wh = ship["region_code"].map(wh_by_region).fillna(warehouses["warehouse_id"].iloc[0]).values
    shipment_id = np.array([f"SHP-{oid[4:]}" for oid in ship["order_id"]])

    def frame(mask, event, ts, location):
        return pd.DataFrame(
            {
                "event_ts": ts[mask].values,
                "event": event,
                "shipment_id": shipment_id[mask],
                "order_id": ship["order_id"].values[mask],
                "carrier": carrier[mask],
                "warehouse_id": wh[mask],
                "region": ship["region_code"].values[mask],
                "location": location[mask] if isinstance(location, np.ndarray) else location,
            }
        )

    all_mask = np.ones(n, dtype=bool)
    hub = np.array([f"{wh_city[w]} Hub" for w in wh])
    frames = [
        frame(all_mask, "label_created", label_ts, hub),
        frame(all_mask, "in_transit", transit_ts, np.array(["Carrier network"] * n)),
    ]
    exc_ts = transit_ts + pd.to_timedelta(r.ints(12, 60, n), unit="h")
    frames.append(frame(exception, "delivery_exception", exc_ts, np.array(["Carrier network"] * n)))
    delivered_mask = ship["status"].isin(["delivered", "returned"]).values
    frames.append(frame(delivered_mask, "delivered", delivered_ts, np.array(["Customer address"] * n)))
    returned_mask = (ship["status"] == "returned").values
    ret_ts = delivered_ts + pd.to_timedelta(r.ints(3 * 24, 14 * 24, n), unit="h")
    frames.append(frame(returned_mask, "returned", ret_ts, hub))

    ev = pd.concat(frames, ignore_index=True)
    ev["event_ts"] = pd.to_datetime(ev["event_ts"])
    ev = ev.sort_values(["event_ts", "shipment_id"], kind="stable").reset_index(drop=True)
    ev["event_ts"] = ev["event_ts"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return ev


def render_log_line(row: dict) -> str:
    return (
        f"{row['event_ts']} INFO [shipping-tracker] event={row['event']} shipment_id={row['shipment_id']} "
        f'order_id={row["order_id"]} carrier="{row["carrier"]}" warehouse_id={row["warehouse_id"]} '
        f'region={row["region"]} location="{row["location"]}"'
    )
