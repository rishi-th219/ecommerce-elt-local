with source_data as (
    select
        id as raw_id,
        payload,
        _ingested_at
    from {{ source('raw', 'raw_sessions') }}
)

select
    cast({{ json_extract('payload', 'session_id') }} as varchar(64)) as session_id,
    cast({{ json_extract('payload', 'customer_id') }} as varchar(64)) as customer_id,
    cast({{ json_extract('payload', 'session_start') }} as {{ dbt.type_timestamp() }}) as session_start,
    cast({{ json_extract('payload', 'session_end') }} as {{ dbt.type_timestamp() }}) as session_end,
    cast({{ json_extract('payload', 'duration_seconds') }} as integer) as duration_seconds,
    cast({{ json_extract('payload', 'device_type') }} as varchar(50)) as device_type,
    cast({{ json_extract('payload', 'traffic_source') }} as varchar(100)) as traffic_source,
    cast({{ json_extract('payload', 'page_views_count') }} as integer) as page_views_count,
    cast(
        case 
            when lower(cast({{ json_extract('payload', 'converted') }} as varchar(10))) in ('true', '1') then 1 
            else 0 
        end as {% if target.type == 'sqlserver' %}bit{% else %}boolean{% endif %}
    ) as is_converted,
    _ingested_at
from source_data
