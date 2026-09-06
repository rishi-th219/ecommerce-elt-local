with products as (
    select * from {{ ref('stg_products') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
    product_id,
    sku,
    product_name,
    category,
    unit_cost,
    retail_price,
    cast(round(((retail_price - unit_cost) / nullif(retail_price, 0) * 100.0), 2) as numeric(10, 2)) as baseline_margin_pct,
    created_at
from products
