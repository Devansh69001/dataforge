-- Revenue by category and month with each category's share of the month.
select
    parent_category_name,
    category_name,
    order_month,
    count(distinct order_id) as orders,
    sum(quantity) as units_sold,
    sum(line_total_usd) as revenue_usd,
    sum(gross_margin_usd) as gross_margin_usd,
    sum(sum(line_total_usd)) over (partition by order_month) as month_revenue_usd,
    sum(line_total_usd) / nullif(sum(sum(line_total_usd)) over (partition by order_month), 0) as revenue_share
from {{ ref('int_revenue_lines') }}
group by parent_category_name, category_name, order_month
