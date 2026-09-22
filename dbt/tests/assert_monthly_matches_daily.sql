-- The monthly mart must reconcile with the daily mart it is built from (within rounding).
with daily as (
    select year_month, sum(revenue_usd) as revenue_usd from {{ ref('daily_sales_summary') }} group by year_month
)
select m.year_month, m.revenue_usd as monthly, d.revenue_usd as daily
from {{ ref('monthly_revenue_summary') }} m
join daily d on d.year_month = m.year_month
where abs(m.revenue_usd - d.revenue_usd) > 0.01
