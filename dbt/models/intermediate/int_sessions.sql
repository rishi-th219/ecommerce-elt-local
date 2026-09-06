with sessions as (
    select * from {{ ref('stg_sessions') }}
),

orders as (
    select
        order_id,
        session_id,
        order_date,
        net_revenue
    from {{ ref('stg_orders') }}
    where session_id is not null
)

select
    s.session_id,
    s.customer_id,
    s.session_start,
    s.session_end,
    s.duration_seconds,
    round(s.duration_seconds / 60.0, 2) as duration_minutes,
    s.device_type,
    s.traffic_source,
    s.page_views_count,
    s.is_converted,
    o.order_id as converted_order_id,
    coalesce(o.net_revenue, 0.0) as session_attributed_revenue
from sessions s
left join orders o
    on s.session_id = o.session_id
