with monthly as (
    select
        year_month,
        min(sales_date) as month_start,
        sum(orders_placed) as orders_placed,
        sum(revenue_orders) as revenue_orders,
        sum(cancelled_orders) as cancelled_orders,
        sum(returned_orders) as returned_orders,
        sum(revenue_usd) as revenue_usd,
        sum(gross_margin_usd) as gross_margin_usd,
        sum(discount_usd) as discount_usd,
        sum(refunded_usd) as refunded_usd,
        sum(units_sold) as units_sold,
        sum(new_customers) as new_customers
    from {{ ref('daily_sales_summary') }}
    group by year_month
),
customers as (
    select order_month as year_month, count(distinct customer_key) as active_customers
    from {{ ref('stg_orders') }}
    where is_revenue
    group by order_month
)
select
    m.year_month,
    m.month_start,
    m.orders_placed,
    m.revenue_orders,
    m.cancelled_orders,
    m.returned_orders,
    m.revenue_usd,
    m.gross_margin_usd,
    case when m.revenue_usd > 0 then m.gross_margin_usd / m.revenue_usd end as gross_margin_pct,
    m.discount_usd,
    m.refunded_usd,
    m.units_sold,
    c.active_customers,
    m.new_customers,
    case when m.revenue_orders > 0 then m.revenue_usd / m.revenue_orders end as avg_order_value_usd,
    lag(m.revenue_usd) over (order by m.year_month) as prev_month_revenue_usd,
    case when lag(m.revenue_usd) over (order by m.year_month) > 0
         then (m.revenue_usd - lag(m.revenue_usd) over (order by m.year_month)) / lag(m.revenue_usd) over (order by m.year_month)
    end as revenue_mom_growth
from monthly m
left join customers c on c.year_month = m.year_month
