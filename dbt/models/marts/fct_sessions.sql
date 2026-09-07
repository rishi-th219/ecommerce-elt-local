{#-
    Session fact -- one row per web session, promoted out of the intermediate
    layer into `marts` so BI tools connected to the star schema can join
    marketing traffic to sales revenue without reaching across schemas.

    Grain: one row per `session_id`.
-#}

with sessions as (
    select
        session_id,
        customer_id,
        session_start,
        session_end,
        duration_seconds,
        duration_minutes,
        device_type,
        traffic_source,
        page_views_count,
        is_converted,
        converted_order_id,
        order_count,
        session_attributed_revenue
    from {{ ref('int_sessions') }}
),

dim_customers as (
    select
        customer_key,
        customer_id,
        valid_from,
        valid_to
    from {{ ref('dim_customers') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['s.session_id']) }} as session_key,
    s.session_id,
    coalesce(dc.customer_key, '{{ var("unknown_key") }}') as customer_key,
    coalesce({{ date_to_key('s.session_start') }}, {{ var('unknown_date_key') }}) as session_date_key,
    s.session_start,
    s.session_end,
    s.device_type,
    s.traffic_source,
    s.duration_seconds,
    s.duration_minutes,
    s.page_views_count,
    s.is_converted,
    s.converted_order_id,
    s.order_count,
    s.session_attributed_revenue
from sessions s
left join dim_customers dc
    on s.customer_id = dc.customer_id
    and s.session_start >= dc.valid_from
    and (s.session_start < dc.valid_to or dc.valid_to is null)
