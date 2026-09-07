{#-
    Kimball SCD Type 2 customer dimension.

    Three things worth calling out:

    1. Deduplication -- the raw feed can replay the same profile event twice.
       Two rows sharing (customer_id, updated_at) would produce an identical
       surrogate key, breaking the uniqueness of `customer_key`. The latest
       ingested copy of each (customer_id, updated_at) wins.

    2. Beginning of time -- the first version of each customer is anchored to
       `scd2_beginning_of_time` rather than to `updated_at`. Orders that predate
       the first captured profile row (early-arriving facts) would otherwise
       find no valid dimension version at all. `created_at` is retained as a
       descriptive attribute.

    3. Unknown member -- a `-1` row so `fct_orders` can route failed lookups to
       a real dimension record instead of a NULL foreign key.
-#}

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

deduplicated as (
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
        updated_at
    from (
        select
            *,
            row_number() over (
                partition by customer_id, updated_at
                order by _ingested_at desc
            ) as event_rank
        from customer_events
    ) ranked
    where event_rank = 1
),

versioned as (
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
        row_number() over (
            partition by customer_id
            order by updated_at asc
        ) as version_number,
        lead(updated_at) over (
            partition by customer_id
            order by updated_at asc
        ) as next_updated_at
    from deduplicated
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
        case
            when version_number = 1
                then cast('{{ var("scd2_beginning_of_time") }}' as {{ dbt.type_timestamp() }})
            else updated_at
        end as valid_from,
        next_updated_at as valid_to,
        updated_at as effective_updated_at,
        {{ bool_flag('next_updated_at is null') }} as is_current
    from versioned
),

unknown_member as (
    select
        cast('{{ var("unknown_key") }}' as varchar(64)) as customer_key,
        cast('{{ var("unknown_key") }}' as varchar(64)) as customer_id,
        cast('Unknown' as varchar(100)) as first_name,
        cast('Unknown' as varchar(100)) as last_name,
        cast('unknown@unknown.invalid' as varchar(255)) as email,
        cast('Unknown' as varchar(100)) as city,
        cast('XX' as varchar(50)) as state,
        cast('00000' as varchar(20)) as postal_code,
        cast('Unknown' as varchar(50)) as country,
        cast('{{ var("scd2_beginning_of_time") }}' as {{ dbt.type_timestamp() }}) as created_at,
        cast('{{ var("scd2_beginning_of_time") }}' as {{ dbt.type_timestamp() }}) as valid_from,
        cast(null as {{ dbt.type_timestamp() }}) as valid_to,
        cast('{{ var("scd2_beginning_of_time") }}' as {{ dbt.type_timestamp() }}) as effective_updated_at,
        {{ cast_bool(bool_literal(true)) }} as is_current
)

select * from scd2_dimensions
union all
select * from unknown_member
