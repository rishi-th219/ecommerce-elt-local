{#-
    One row per web session, with order conversion revenue attributed back to
    the session that produced it.

    Orders are pre-aggregated to the session grain before the join: a customer
    can place more than one order inside a single browsing session, and joining
    the order table directly would fan the session out into duplicate rows
    (silently double counting sessions, page views and durations).
-#}

with sessions as (
    select
        session_id,
        customer_id,
        session_start,
        session_end,
        duration_seconds,
        device_type,
        traffic_source,
        page_views_count,
        is_converted
    from {{ ref('stg_sessions') }}
),

session_orders as (
    select
        session_id,
        min(order_id) as first_order_id,
        count(*) as order_count,
        sum(net_revenue) as attributed_revenue
    from {{ ref('stg_orders') }}
    where session_id is not null
    group by session_id
)

select
    s.session_id,
    s.customer_id,
    s.session_start,
    s.session_end,
    s.duration_seconds,
    cast(round(s.duration_seconds / 60.0, 2) as numeric(10, 2)) as duration_minutes,
    s.device_type,
    s.traffic_source,
    s.page_views_count,
    s.is_converted,
    o.first_order_id as converted_order_id,
    coalesce(o.order_count, 0) as order_count,
    cast(coalesce(o.attributed_revenue, 0.0) as numeric(12, 2)) as session_attributed_revenue
from sessions s
left join session_orders o
    on s.session_id = o.session_id
