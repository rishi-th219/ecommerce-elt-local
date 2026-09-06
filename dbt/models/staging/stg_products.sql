with source_data as (
    select
        id as raw_id,
        payload,
        _ingested_at
    from {{ source('raw', 'raw_products') }}
)

select
    cast({{ json_extract('payload', 'product_id') }} as varchar(64)) as product_id,
    cast({{ json_extract('payload', 'sku') }} as varchar(64)) as sku,
    cast({{ json_extract('payload', 'product_name') }} as varchar(255)) as product_name,
    cast({{ json_extract('payload', 'category') }} as varchar(100)) as category,
    cast({{ json_extract('payload', 'unit_cost') }} as numeric(10, 2)) as unit_cost,
    cast({{ json_extract('payload', 'retail_price') }} as numeric(10, 2)) as retail_price,
    cast({{ json_extract('payload', 'created_at') }} as {{ dbt.type_timestamp() }}) as created_at,
    _ingested_at
from source_data
