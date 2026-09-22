select supplier_key, supplier_id, supplier_name, country_code, region_code, lead_time_days, quality_tier, is_active
from {{ source('warehouse', 'dim_supplier') }}
