with source_data as (
    select
        id as raw_id,
        payload,
        _ingested_at
    from {{ source('raw', 'raw_products') }}
)

select
    (payload->>'product_id')::varchar(64) as product_id,
    (payload->>'sku')::varchar(64) as sku,
    (payload->>'product_name')::varchar(255) as product_name,
    (payload->>'category')::varchar(100) as category,
    (payload->>'unit_cost')::numeric(10, 2) as unit_cost,
    (payload->>'retail_price')::numeric(10, 2) as retail_price,
    (payload->>'created_at')::timestamp as created_at,
    _ingested_at
from source_data
