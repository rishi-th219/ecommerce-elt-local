with source_data as (
    select
        id as raw_id,
        payload,
        _ingested_at
    from {{ source('raw', 'raw_customers') }}
)

select
    cast({{ json_extract('payload', 'customer_id') }} as varchar(64)) as customer_id,
    cast({{ json_extract('payload', 'first_name') }} as varchar(100)) as first_name,
    cast({{ json_extract('payload', 'last_name') }} as varchar(100)) as last_name,
    cast({{ json_extract('payload', 'email') }} as varchar(255)) as email,
    cast({{ json_extract('payload', 'city') }} as varchar(100)) as city,
    cast({{ json_extract('payload', 'state') }} as varchar(50)) as state,
    cast({{ json_extract('payload', 'postal_code') }} as varchar(20)) as postal_code,
    cast({{ json_extract('payload', 'country') }} as varchar(50)) as country,
    cast({{ json_extract('payload', 'created_at') }} as {{ dbt.type_timestamp() }}) as created_at,
    cast({{ json_extract('payload', 'updated_at') }} as {{ dbt.type_timestamp() }}) as updated_at,
    _ingested_at
from source_data
