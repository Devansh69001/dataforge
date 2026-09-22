select region_key, region_code, region_name, country_code, country_name, currency_code, base_delivery_days
from {{ source('warehouse', 'dim_region') }}
