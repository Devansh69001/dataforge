-- One row per customer with order-level aggregates (revenue orders only for monetary fields).
select
    o.customer_key,
    count(*) as total_orders,
    count(*) filter (where o.is_revenue) as revenue_orders,
    count(*) filter (where o.is_cancelled) as cancelled_orders,
    count(*) filter (where o.is_returned) as returned_orders,
    sum(o.order_total_usd) filter (where o.is_revenue) as revenue_usd,
    sum(o.refunded_amount_usd) as refunded_usd,
    min(o.order_date) as first_order_date,
    max(o.order_date) as last_order_date,
    avg(o.days_since_prev_order) as avg_days_between_orders
from {{ ref('stg_orders') }} o
group by o.customer_key
