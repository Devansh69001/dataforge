select
    s.*,
    r.region_name,
    r.base_delivery_days,
    w.warehouse_name,
    case when s.delivery_days is not null and s.delivery_days <= r.base_delivery_days + 1 then true
         when s.delivery_days is not null then false end as delivered_on_time
from {{ ref('stg_shipments') }} s
left join {{ ref('stg_regions') }} r on r.region_key = s.region_key
left join {{ ref('stg_warehouses') }} w on w.warehouse_key = s.warehouse_key
