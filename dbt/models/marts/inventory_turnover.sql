-- Inventory turnover per product (last 90 days, relative to the newest inventory event):
--   turnover_ratio = cost of goods shipped / average inventory value
--   days_of_cover  = on-hand units / daily sales velocity
-- and the two flags analysts ask for: low stock, and high velocity but low inventory.
with bounds as (
    select max(event_date) as as_of from {{ ref('stg_inventory_events') }}
),
shipped as (
    select
        e.product_key,
        sum(-e.quantity_delta) as units_shipped_90d,
        sum(-e.quantity_delta * p.unit_cost_usd) as cogs_90d
    from {{ ref('stg_inventory_events') }} e
    join {{ ref('stg_products') }} p on p.product_key = e.product_key
    cross join bounds b
    where e.event_type = 'shipment' and e.event_date > b.as_of - 90
    group by e.product_key
),
position as (
    select
        product_key,
        product_id,
        sum(on_hand_units) as on_hand_units,
        sum(inventory_value_usd) as inventory_value_usd,
        count(*) as warehouses_stocked,
        sum(case when on_hand_units <= 0 then 1 else 0 end) as warehouses_out_of_stock
    from {{ ref('stg_inventory_position') }}
    group by product_key, product_id
)
select
    pos.product_key,
    pos.product_id,
    p.product_name,
    p.category_name,
    p.supplier_name,
    pos.on_hand_units,
    pos.inventory_value_usd,
    pos.warehouses_stocked,
    pos.warehouses_out_of_stock,
    coalesce(s.units_shipped_90d, 0) as units_shipped_90d,
    coalesce(s.cogs_90d, 0) as cogs_90d,
    case when pos.inventory_value_usd > 0 then coalesce(s.cogs_90d, 0) / pos.inventory_value_usd end as turnover_ratio_90d,
    m.velocity_units_per_day,
    case when m.velocity_units_per_day > 0 then round(pos.on_hand_units / m.velocity_units_per_day, 1) end as days_of_cover,
    (m.velocity_units_per_day > 0 and pos.on_hand_units / m.velocity_units_per_day < {{ var('low_stock_days_of_cover') }}) as is_low_stock,
    (m.velocity_units_per_day >= 1
        and pos.on_hand_units / nullif(m.velocity_units_per_day, 0) < {{ var('low_stock_days_of_cover') }}) as high_velocity_low_stock
from position pos
join {{ ref('stg_products') }} p on p.product_key = pos.product_key
left join shipped s on s.product_key = pos.product_key
left join {{ ref('product_sales_metrics') }} m on m.product_key = pos.product_key
