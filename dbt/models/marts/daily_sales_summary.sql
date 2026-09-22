{{ config(
    materialized='incremental',
    unique_key='sales_date',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}
-- Daily revenue roll-up. Incremental: only days newer than (max loaded day - lookback)
-- are recomputed, which absorbs late-arriving status updates without a full rebuild.
{% set lookback = var("incremental_lookback_days") %}
with orders as (
    select *
    from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    where order_date >= (
        select coalesce(max(sales_date), date '1900-01-01') - {{ lookback }}
        from {{ this }}
    )
    {% endif %}
),
lines as (
    select order_date, sum(gross_margin_usd) as gross_margin_usd, sum(quantity) as units_sold
    from {{ ref('stg_order_items') }}
    where is_revenue
    {% if is_incremental() %}
    and order_date >= (
        select coalesce(max(sales_date), date '1900-01-01') - {{ lookback }}
        from {{ this }}
    )
    {% endif %}
    group by order_date
)
select
    o.order_date as sales_date,
    d.year_month,
    d.day_name,
    d.is_weekend,
    count(*) as orders_placed,
    count(*) filter (where o.is_revenue) as revenue_orders,
    count(*) filter (where o.is_cancelled) as cancelled_orders,
    count(*) filter (where o.is_returned) as returned_orders,
    count(distinct o.customer_key) filter (where o.is_revenue) as active_customers,
    count(distinct o.customer_key) filter (where o.is_first_order) as new_customers,
    coalesce(sum(o.order_total_usd) filter (where o.is_revenue), 0) as revenue_usd,
    coalesce(sum(o.discount_usd) filter (where o.is_revenue), 0) as discount_usd,
    coalesce(sum(o.refunded_amount_usd), 0) as refunded_usd,
    coalesce(max(l.gross_margin_usd), 0) as gross_margin_usd,
    coalesce(max(l.units_sold), 0) as units_sold,
    case when count(*) filter (where o.is_revenue) > 0
         then sum(o.order_total_usd) filter (where o.is_revenue) / count(*) filter (where o.is_revenue) end as avg_order_value_usd,
    now() as computed_at
from orders o
join {{ ref('stg_dates') }} d on d.full_date = o.order_date
left join lines l on l.order_date = o.order_date
group by o.order_date, d.year_month, d.day_name, d.is_weekend
