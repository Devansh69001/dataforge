-- Per-product performance including a "declining" flag (last 90 days vs prior 90 days)
-- and sales velocity (units/day over the last 30 days) - inputs to the low-stock analysis.
with totals as (
    select
        product_key,
        count(distinct order_id) as orders,
        sum(quantity) as units_sold,
        sum(line_total_usd) as revenue_usd,
        sum(gross_margin_usd) as gross_margin_usd,
        count(distinct customer_key) as unique_customers,
        min(order_date) as first_sold_date,
        max(order_date) as last_sold_date
    from {{ ref('int_revenue_lines') }}
    group by product_key
)
select
    p.product_key,
    p.product_id,
    p.sku,
    p.product_name,
    p.brand,
    p.category_name,
    p.parent_category_name,
    p.supplier_name,
    p.unit_price_usd,
    p.unit_margin_usd,
    p.is_active,
    coalesce(t.orders, 0) as orders,
    coalesce(t.units_sold, 0) as units_sold,
    coalesce(t.revenue_usd, 0) as revenue_usd,
    coalesce(t.gross_margin_usd, 0) as gross_margin_usd,
    coalesce(t.unique_customers, 0) as unique_customers,
    t.first_sold_date,
    t.last_sold_date,
    coalesce(s.revenue_last_90d, 0) as revenue_last_90d,
    coalesce(s.revenue_prior_90d, 0) as revenue_prior_90d,
    case when coalesce(s.revenue_prior_90d, 0) > 0 then s.revenue_last_90d / s.revenue_prior_90d end as revenue_trend_ratio,
    (coalesce(s.revenue_prior_90d, 0) > 0
        and coalesce(s.revenue_last_90d, 0) / s.revenue_prior_90d < {{ var('declining_threshold') }}) as is_declining,
    round(coalesce(s.units_last_30d, 0) / 30.0, 3) as velocity_units_per_day,
    rank() over (order by coalesce(t.revenue_usd, 0) desc) as revenue_rank
from {{ ref('stg_products') }} p
left join totals t on t.product_key = p.product_key
left join {{ ref('int_product_period_sales') }} s on s.product_key = p.product_key
