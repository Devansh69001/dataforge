-- Per-warehouse stock position, throughput and outbound delivery performance.
with stock as (
    select
        warehouse_key,
        count(*) as skus_stocked,
        sum(on_hand_units) as on_hand_units,
        sum(inventory_value_usd) as inventory_value_usd,
        sum(case when on_hand_units <= 0 then 1 else 0 end) as skus_out_of_stock,
        sum(units_received) as units_received,
        sum(units_shipped) as units_shipped
    from {{ ref('stg_inventory_position') }}
    group by warehouse_key
),
recent as (
    select
        e.warehouse_key,
        sum(case when e.event_type = 'shipment' then -e.quantity_delta else 0 end) as units_shipped_90d,
        sum(case when e.event_type = 'receipt' then e.quantity_delta else 0 end) as units_received_90d,
        sum(case when e.event_type = 'adjustment' then abs(e.quantity_delta) else 0 end) as units_adjusted_90d
    from {{ ref('stg_inventory_events') }} e
    cross join (select max(event_date) as as_of from {{ ref('stg_inventory_events') }}) b
    where e.event_date > b.as_of - 90
    group by e.warehouse_key
),
ship as (
    select
        warehouse_key,
        count(*) as shipments,
        avg(delivery_days) filter (where is_delivered) as avg_delivery_days,
        avg(case when had_exception then 1.0 else 0.0 end) as exception_rate,
        avg(case when delivered_on_time then 1.0 else 0.0 end) filter (where is_delivered) as on_time_rate
    from {{ ref('int_shipping_enriched') }}
    group by warehouse_key
)
select
    w.warehouse_key,
    w.warehouse_id,
    w.warehouse_name,
    w.city,
    w.region_code,
    w.capacity_units,
    st.skus_stocked,
    st.on_hand_units,
    st.inventory_value_usd,
    st.skus_out_of_stock,
    case when w.capacity_units > 0 then st.on_hand_units::numeric / w.capacity_units end as capacity_utilisation,
    r.units_shipped_90d,
    r.units_received_90d,
    r.units_adjusted_90d,
    case when r.units_shipped_90d > 0 then r.units_adjusted_90d::numeric / r.units_shipped_90d end as shrinkage_rate_90d,
    sh.shipments,
    sh.avg_delivery_days,
    sh.exception_rate,
    sh.on_time_rate
from {{ ref('stg_warehouses') }} w
left join stock st on st.warehouse_key = w.warehouse_key
left join recent r on r.warehouse_key = w.warehouse_key
left join ship sh on sh.warehouse_key = w.warehouse_key
