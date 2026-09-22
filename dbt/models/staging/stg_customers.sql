select
    customer_key, customer_id, full_name, customer_segment, country_code, region_code, region_key, city, signup_date, marketing_opt_in
from {{ source('warehouse', 'dim_customer') }}
