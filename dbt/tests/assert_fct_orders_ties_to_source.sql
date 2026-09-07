-- Singular test: the fact table must reconcile to the source order headers.
-- Net revenue summed across fct_orders line items has to equal the order-level
-- net_revenue reported by the source system, within a cent of rounding drift.
-- This is the check that catches a join fan-out in the fact table -- the class
-- of bug that unique/not_null tests cannot see.
with fact_totals as (
    select
        order_id,
        sum(net_revenue) as fact_net_revenue
    from {{ ref('fct_orders') }}
    group by order_id
),

source_totals as (
    select
        order_id,
        net_revenue as source_net_revenue
    from {{ ref('stg_orders') }}
)

select
    s.order_id,
    s.source_net_revenue,
    f.fact_net_revenue
from source_totals s
inner join fact_totals f
    on s.order_id = f.order_id
where abs(s.source_net_revenue - f.fact_net_revenue) > 0.01
