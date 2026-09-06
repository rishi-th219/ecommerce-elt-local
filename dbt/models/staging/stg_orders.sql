with source_data as (
    select
        id as raw_id,
        payload,
        _ingested_at
    from {{ source('raw', 'raw_orders') }}
)

select
    cast({{ json_extract('payload', 'order_id') }} as varchar(64)) as order_id,
    cast({{ json_extract('payload', 'customer_id') }} as varchar(64)) as customer_id,
    cast({{ json_extract('payload', 'session_id') }} as varchar(64)) as session_id,
    cast({{ json_extract('payload', 'order_date') }} as {{ dbt.type_timestamp() }}) as order_date,
    cast({{ json_extract('payload', 'order_status') }} as varchar(50)) as order_status,
    cast({{ json_extract('payload', 'payment_method') }} as varchar(50)) as payment_method,
    cast({{ json_extract('payload', 'gross_amount') }} as numeric(12, 2)) as gross_amount,
    cast({{ json_extract('payload', 'discount_amount') }} as numeric(12, 2)) as discount_amount,
    cast({{ json_extract('payload', 'net_revenue') }} as numeric(12, 2)) as net_revenue,
    _ingested_at
from source_data
