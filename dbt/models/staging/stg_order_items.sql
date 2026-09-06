with source_data as (
    select
        id as raw_id,
        payload,
        _ingested_at
    from {{ source('raw', 'raw_order_items') }}
)

select
    cast({{ json_extract('payload', 'order_item_id') }} as varchar(64)) as order_item_id,
    cast({{ json_extract('payload', 'order_id') }} as varchar(64)) as order_id,
    cast({{ json_extract('payload', 'product_id') }} as varchar(64)) as product_id,
    cast({{ json_extract('payload', 'quantity') }} as integer) as quantity,
    cast({{ json_extract('payload', 'unit_price') }} as numeric(10, 2)) as unit_price,
    cast({{ json_extract('payload', 'discount_amount') }} as numeric(10, 2)) as discount_amount,
    cast({{ json_extract('payload', 'net_amount') }} as numeric(10, 2)) as net_amount,
    _ingested_at
from source_data
