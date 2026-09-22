-- One row per customer: lifetime value, recency and a lifecycle stage relative to the
-- newest order date in the warehouse (stable regardless of when the model is run).
with bounds as (
    select max(order_date) as as_of from {{ ref('stg_orders') }}
)
select
    c.customer_key,
    c.customer_id,
    c.full_name,
    c.customer_segment,
    c.country_code,
    c.region_code,
    c.signup_date,
    coalesce(o.total_orders, 0) as total_orders,
    coalesce(o.revenue_orders, 0) as revenue_orders,
    coalesce(o.cancelled_orders, 0) as cancelled_orders,
    coalesce(o.returned_orders, 0) as returned_orders,
    coalesce(o.revenue_usd, 0) as lifetime_revenue_usd,
    coalesce(o.refunded_usd, 0) as lifetime_refunded_usd,
    coalesce(o.revenue_usd, 0) - coalesce(o.refunded_usd, 0) as net_lifetime_value_usd,
    case when coalesce(o.revenue_orders, 0) > 0 then o.revenue_usd / o.revenue_orders end as avg_order_value_usd,
    o.first_order_date,
    o.last_order_date,
    (b.as_of - o.last_order_date) as recency_days,
    o.avg_days_between_orders,
    case
        when o.last_order_date is null then 'never_purchased'
        when b.as_of - o.last_order_date <= 90 then 'active'
        when b.as_of - o.last_order_date <= 180 then 'at_risk'
        else 'churned'
    end as lifecycle_stage,
    ntile(10) over (order by coalesce(o.revenue_usd, 0) desc) as value_decile
from {{ ref('stg_customers') }} c
left join {{ ref('int_customer_orders') }} o on o.customer_key = c.customer_key
cross join bounds b
