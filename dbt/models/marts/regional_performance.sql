-- Sales and delivery performance per region and month.
with sales as (
    select
        region_key,
        order_month,
        count(*) filter (where is_revenue) as revenue_orders,
        sum(order_total_usd) filter (where is_revenue) as revenue_usd,
        count(distinct customer_key) filter (where is_revenue) as active_customers,
        count(*) filter (where is_returned) as returned_orders
    from {{ ref('stg_orders') }}
    group by region_key, order_month
),
ship as (
    select
        region_key,
        order_month,
        avg(delivery_days) as avg_delivery_days,
        avg(case when delivered_on_time then 1.0 else 0.0 end) as on_time_rate,
        count(*) as shipments
    from {{ ref('int_shipping_enriched') }}
    where is_delivered
    group by region_key, order_month
)
select
    r.region_code,
    r.region_name,
    r.country_name,
    r.currency_code,
    s.order_month,
    s.revenue_orders,
    s.revenue_usd,
    s.active_customers,
    s.returned_orders,
    case when s.revenue_orders > 0 then s.revenue_usd / s.revenue_orders end as avg_order_value_usd,
    sh.shipments,
    sh.avg_delivery_days,
    sh.on_time_rate
from sales s
join {{ ref('stg_regions') }} r on r.region_key = s.region_key
left join ship sh on sh.region_key = s.region_key and sh.order_month = s.order_month
