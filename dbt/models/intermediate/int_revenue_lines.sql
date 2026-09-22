-- Revenue-bearing order lines enriched with product hierarchy and geography.
-- "Revenue" = orders in paid/shipped/delivered state (cancelled & returned excluded).
select
    i.order_item_key,
    i.order_id,
    i.order_date,
    i.order_month,
    i.customer_key,
    i.product_key,
    p.product_id,
    p.product_name,
    p.brand,
    p.category_key,
    p.category_name,
    p.parent_category_name,
    p.supplier_key,
    p.supplier_name,
    i.region_key,
    r.region_code,
    r.region_name,
    i.quantity,
    i.line_total_usd,
    i.line_cost_usd,
    i.gross_margin_usd
from {{ ref('stg_order_items') }} i
join {{ ref('stg_products') }} p on p.product_key = i.product_key
left join {{ ref('stg_regions') }} r on r.region_key = i.region_key
where i.is_revenue
