with customer_events as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        city,
        state,
        postal_code,
        country,
        created_at,
        updated_at,
        _ingested_at
    from {{ ref('stg_customers') }}
),

ordered_records as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        city,
        state,
        postal_code,
        country,
        created_at,
        updated_at,
        lead(updated_at) over (
            partition by customer_id
            order by updated_at asc, _ingested_at asc
        ) as next_updated_at
    from customer_events
),

scd2_dimensions as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'updated_at']) }} as customer_key,
        customer_id,
        first_name,
        last_name,
        email,
        city,
        state,
        postal_code,
        country,
        created_at,
        updated_at as valid_from,
        next_updated_at as valid_to,
        case when next_updated_at is null then true else false end as is_current
    from ordered_records
)

select * from scd2_dimensions
