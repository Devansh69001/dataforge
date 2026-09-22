-- Supplier scorecard: defect rate from goods receipts, return rate from customer
-- returns of the supplier's products, and revenue contribution.
with receipts as (
    select
        supplier_key,
        count(*) as receipts,
        sum(quantity_delta) as units_received,
        sum(coalesce(defective_qty, 0)) as units_defective,
        sum(receipt_value_usd) as received_value_usd
    from {{ ref('stg_inventory_events') }}
    where event_type = 'receipt'
    group by supplier_key
),
returns as (
    select
        p.supplier_key,
        count(*) as returned_lines,
        sum(i.quantity) as units_returned
    from {{ ref('stg_order_items') }} i
    join {{ ref('stg_products') }} p on p.product_key = i.product_key
    where i.is_returned
    group by p.supplier_key
),
sales as (
    select supplier_key, sum(quantity) as units_sold, sum(line_total_usd) as revenue_usd, sum(gross_margin_usd) as gross_margin_usd
    from {{ ref('int_revenue_lines') }}
    group by supplier_key
)
select
    s.supplier_key,
    s.supplier_id,
    s.supplier_name,
    s.country_code,
    s.quality_tier,
    s.lead_time_days,
    s.is_active,
    (select count(*) from {{ ref('stg_products') }} p where p.supplier_key = s.supplier_key) as products_supplied,
    coalesce(r.receipts, 0) as receipts,
    coalesce(r.units_received, 0) as units_received,
    coalesce(r.units_defective, 0) as units_defective,
    case when coalesce(r.units_received, 0) > 0 then r.units_defective::numeric / r.units_received end as defect_rate,
    coalesce(rt.units_returned, 0) as units_returned,
    case when coalesce(sa.units_sold, 0) > 0 then rt.units_returned::numeric / sa.units_sold end as return_rate,
    coalesce(sa.units_sold, 0) as units_sold,
    coalesce(sa.revenue_usd, 0) as revenue_usd,
    coalesce(sa.gross_margin_usd, 0) as gross_margin_usd,
    rank() over (order by case when coalesce(r.units_received, 0) > 0 then r.units_defective::numeric / r.units_received end desc nulls last) as defect_rank
from {{ ref('stg_suppliers') }} s
left join receipts r on r.supplier_key = s.supplier_key
left join returns rt on rt.supplier_key = s.supplier_key
left join sales sa on sa.supplier_key = s.supplier_key
