-- Delivery performance by region, carrier and month.
select
    region_code,
    region_name,
    carrier,
    order_month,
    count(*) as shipments,
    count(*) filter (where is_delivered) as delivered,
    count(*) filter (where had_exception) as exceptions,
    count(*) filter (where is_returned) as returned,
    avg(delivery_days) filter (where is_delivered) as avg_delivery_days,
    percentile_cont(0.5) within group (order by delivery_days) filter (where is_delivered) as median_delivery_days,
    percentile_cont(0.9) within group (order by delivery_days) filter (where is_delivered) as p90_delivery_days,
    avg(case when delivered_on_time then 1.0 else 0.0 end) filter (where is_delivered) as on_time_rate,
    avg(case when had_exception then 1.0 else 0.0 end) as exception_rate,
    max(base_delivery_days) as sla_days
from {{ ref('int_shipping_enriched') }}
group by region_code, region_name, carrier, order_month
