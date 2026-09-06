with source_data as (
    select
        id as raw_id,
        payload,
        _ingested_at
    from {{ source('raw', 'raw_order_items') }}
)

select
    (payload->>'order_item_id')::varchar(64) as order_item_id,
    (payload->>'order_id')::varchar(64) as order_id,
    (payload->>'product_id')::varchar(64) as product_id,
    (payload->>'quantity')::integer as quantity,
    (payload->>'unit_price')::numeric(10, 2) as unit_price,
    (payload->>'discount_amount')::numeric(10, 2) as discount_amount,
    (payload->>'net_amount')::numeric(10, 2) as net_amount,
    _ingested_at
from source_data
