with source_data as (
    select
        id as raw_id,
        payload,
        _ingested_at
    from {{ source('raw', 'raw_customers') }}
)

select
    (payload->>'customer_id')::varchar(64) as customer_id,
    (payload->>'first_name')::varchar(100) as first_name,
    (payload->>'last_name')::varchar(100) as last_name,
    (payload->>'email')::varchar(255) as email,
    (payload->>'city')::varchar(100) as city,
    (payload->>'state')::varchar(50) as state,
    (payload->>'postal_code')::varchar(20) as postal_code,
    (payload->>'country')::varchar(50) as country,
    (payload->>'created_at')::timestamp as created_at,
    (payload->>'updated_at')::timestamp as updated_at,
    _ingested_at
from source_data
