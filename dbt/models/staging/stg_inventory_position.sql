select
    position_key, product_key, product_id, warehouse_key, warehouse_id, on_hand_units, avg_unit_cost_usd,
    inventory_value_usd, units_received, units_shipped, units_defective, last_receipt_ts, last_shipment_ts, last_event_ts
from {{ source('warehouse', 'agg_inventory_position') }}
