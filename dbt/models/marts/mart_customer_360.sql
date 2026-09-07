{#-
    Customer 360 / RFM summary -- one row per customer, joining the current
    profile version to lifetime purchase behaviour and web engagement.

    Recency, Frequency and Monetary values are scored into quintiles with
    NTILE(5) and collapsed into a coarse segment label that a marketing team can
    filter on directly in Power BI.

    Recency is measured against the latest order date in the warehouse rather
    than `current_date`, so the segmentation is stable no matter when the
    snapshot is queried.

    Facts are rolled up by `customer_id`, not by `customer_key`. A customer who
    has relocated owns several SCD2 versions, and their orders are attributed to
    whichever version was in force at the time -- aggregating on the current
    `customer_key` alone would silently drop every order placed before the move.

    Grain: one row per `customer_id`.
-#}

with customers as (
    select
        customer_key,
        customer_id,
        first_name,
        last_name,
        email,
        city,
        state,
        country,
        created_at
    from {{ ref('dim_customers') }}
    where is_current = {{ bool_literal(true) }}
      and customer_id <> '{{ var("unknown_key") }}'
),

customer_versions as (
    select
        customer_key,
        customer_id
    from {{ ref('dim_customers') }}
),

orders as (
    select
        v.customer_id,
        f.order_id,
        f.order_timestamp,
        f.order_status,
        f.net_revenue,
        f.gross_profit
    from {{ ref('fct_orders') }} f
    inner join customer_versions v
        on f.customer_key = v.customer_key
),

warehouse_watermark as (
    select max(order_timestamp) as latest_order_timestamp
    from orders
),

customer_orders as (
    select
        o.customer_id,
        count(distinct o.order_id) as lifetime_order_count,
        cast(sum(o.net_revenue) as numeric(14, 2)) as lifetime_net_revenue,
        cast(sum(o.gross_profit) as numeric(14, 2)) as lifetime_gross_profit,
        min(o.order_timestamp) as first_order_at,
        max(o.order_timestamp) as last_order_at,
        count(distinct case when o.order_status = 'returned' then o.order_id end) as returned_order_count
    from orders o
    group by o.customer_id
),

customer_sessions as (
    select
        v.customer_id,
        count(*) as session_count,
        sum(s.page_views_count) as total_page_views
    from {{ ref('fct_sessions') }} s
    inner join customer_versions v
        on s.customer_key = v.customer_key
    group by v.customer_id
),

combined as (
    select
        c.customer_key,
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.city,
        c.state,
        c.country,
        c.created_at as customer_since,
        coalesce(co.lifetime_order_count, 0) as lifetime_order_count,
        cast(coalesce(co.lifetime_net_revenue, 0) as numeric(14, 2)) as lifetime_net_revenue,
        cast(coalesce(co.lifetime_gross_profit, 0) as numeric(14, 2)) as lifetime_gross_profit,
        cast(coalesce(co.lifetime_net_revenue, 0) / nullif(co.lifetime_order_count, 0) as numeric(14, 2)) as avg_order_value,
        coalesce(co.returned_order_count, 0) as returned_order_count,
        co.first_order_at,
        co.last_order_at,
        {{ days_between('co.last_order_at', 'w.latest_order_timestamp') }} as days_since_last_order,
        coalesce(cs.session_count, 0) as session_count,
        coalesce(cs.total_page_views, 0) as total_page_views
    from customers c
    cross join warehouse_watermark w
    left join customer_orders co
        on c.customer_id = co.customer_id
    left join customer_sessions cs
        on c.customer_id = cs.customer_id
),

scored as (
    select
        *,
        case
            when lifetime_order_count = 0 then 0
            else ntile(5) over (
                partition by case when lifetime_order_count = 0 then 1 else 0 end
                order by days_since_last_order desc
            )
        end as recency_score,
        case
            when lifetime_order_count = 0 then 0
            else ntile(5) over (
                partition by case when lifetime_order_count = 0 then 1 else 0 end
                order by lifetime_order_count asc
            )
        end as frequency_score,
        case
            when lifetime_order_count = 0 then 0
            else ntile(5) over (
                partition by case when lifetime_order_count = 0 then 1 else 0 end
                order by lifetime_net_revenue asc
            )
        end as monetary_score
    from combined
)

select
    customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    city,
    state,
    country,
    customer_since,
    lifetime_order_count,
    lifetime_net_revenue,
    lifetime_gross_profit,
    avg_order_value,
    returned_order_count,
    first_order_at,
    last_order_at,
    days_since_last_order,
    session_count,
    total_page_views,
    recency_score,
    frequency_score,
    monetary_score,
    case
        when lifetime_order_count = 0 then 'Never Purchased'
        when recency_score >= 4 and frequency_score >= 4 then 'Champion'
        when recency_score >= 4 and frequency_score < 4 then 'Promising'
        when recency_score = 3 then 'Needs Attention'
        when monetary_score >= 4 then 'At Risk (High Value)'
        else 'Hibernating'
    end as rfm_segment
from scored
