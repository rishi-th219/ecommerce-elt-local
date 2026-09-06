with source_data as (
    select
        id as raw_id,
        payload,
        _ingested_at
    from {{ source('raw', 'raw_sessions') }}
)

select
    (payload->>'session_id')::varchar(64) as session_id,
    (payload->>'customer_id')::varchar(64) as customer_id,
    (payload->>'session_start')::timestamp as session_start,
    (payload->>'session_end')::timestamp as session_end,
    (payload->>'duration_seconds')::integer as duration_seconds,
    (payload->>'device_type')::varchar(50) as device_type,
    (payload->>'traffic_source')::varchar(100) as traffic_source,
    (payload->>'page_views_count')::integer as page_views_count,
    (payload->>'converted')::boolean as is_converted,
    _ingested_at
from source_data
