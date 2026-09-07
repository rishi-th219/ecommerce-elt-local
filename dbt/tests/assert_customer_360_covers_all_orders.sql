-- Singular test: every order must be represented in the customer summary mart.
--
-- The failure this guards against is subtle: a customer who relocates owns
-- several SCD2 versions, and their orders are attributed to whichever version
-- was in force at the time. Rolling the facts up on the *current* customer_key
-- alone silently drops every order placed before the move -- the mart still
-- passes uniqueness and not_null, it is just quietly wrong. Comparing totals is
-- the only thing that catches it.
with mart_totals as (
    select
        sum(lifetime_order_count) as mart_order_count,
        sum(session_count) as mart_session_count
    from {{ ref('mart_customer_360') }}
),

fact_totals as (
    select
        (select count(distinct order_id) from {{ ref('fct_orders') }}) as fact_order_count,
        (select count(*) from {{ ref('fct_sessions') }}) as fact_session_count
)

select
    m.mart_order_count,
    f.fact_order_count,
    m.mart_session_count,
    f.fact_session_count
from mart_totals m
cross join fact_totals f
where m.mart_order_count <> f.fact_order_count
   or m.mart_session_count <> f.fact_session_count
