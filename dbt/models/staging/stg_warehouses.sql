select warehouse_key, warehouse_id, warehouse_name, city, region_code, region_key, capacity_units
from {{ source('warehouse', 'dim_warehouse') }}
