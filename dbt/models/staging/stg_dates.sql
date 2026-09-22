select date_key, full_date, year, quarter, month, month_name, year_month, week_of_year, day_of_week, day_name, is_weekend
from {{ source('warehouse', 'dim_date') }}
