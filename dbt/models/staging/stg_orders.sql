with source_data as (
    select
        id as raw_id,
        payload,
        _ingested_at
    from {{ source('raw', 'raw_orders') }}
)

select
    (payload->>'order_id')::varchar(64) as order_id,
    (payload->>'customer_id')::varchar(64) as customer_id,
    (payload->>'session_id')::varchar(64) as session_id,
    (payload->>'order_date')::timestamp as order_date,
    (payload->>'order_status')::varchar(50) as order_status,
    (payload->>'payment_method')::varchar(50) as payment_method,
    (payload->>'gross_amount')::numeric(12, 2) as gross_amount,
    (payload->>'discount_amount')::numeric(12, 2) as discount_amount,
    (payload->>'net_revenue')::numeric(12, 2) as net_revenue,
    _ingested_at
from source_data
