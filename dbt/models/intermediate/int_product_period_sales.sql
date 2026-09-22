-- Per product: revenue in the trailing 90 days vs the 90 days before, relative to the
-- latest order date in the warehouse (so the model is stable regardless of run date).
with bounds as (
    select max(order_date) as as_of from {{ ref('stg_orders') }}
),
lines as (
    select l.product_key, l.order_date, l.line_total_usd, l.quantity, b.as_of
    from {{ ref('int_revenue_lines') }} l cross join bounds b
)
select
    product_key,
    max(as_of) as as_of_date,
    sum(case when order_date >  as_of - 90  then line_total_usd else 0 end) as revenue_last_90d,
    sum(case when order_date <= as_of - 90 and order_date > as_of - 180 then line_total_usd else 0 end) as revenue_prior_90d,
    sum(case when order_date >  as_of - 90  then quantity else 0 end) as units_last_90d,
    sum(case when order_date <= as_of - 90 and order_date > as_of - 180 then quantity else 0 end) as units_prior_90d,
    sum(case when order_date >  as_of - 30  then quantity else 0 end) as units_last_30d
from lines
group by product_key
