select
    product_key, product_id, sku, product_name, brand, category_key, category_id, category_name, parent_category_name,
    supplier_key, supplier_id, supplier_name, unit_price_usd, unit_cost_usd, unit_margin_usd, is_active, has_negative_margin
from {{ source('warehouse', 'dim_product') }}
