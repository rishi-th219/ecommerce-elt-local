{#-
    Transactional fact at the grain of one order line item.

    Customer resolution is a single point-in-time join. Because the first
    version of every customer in `dim_customers` is anchored to the beginning of
    time, the historically-correct version is always found -- the second
    "current version" fallback join the model used to carry was redundant work
    on every row.

    Any foreign key that still fails to resolve is routed to the dimension's
    Unknown member rather than being left NULL, so no measure silently drops out
    of an inner-joined BI query.
-#}

with margins as (
    select
        order_item_id,
        order_id,
        customer_id,
        product_id,
        session_id,
        order_date,
        order_status,
        payment_method,
        quantity,
        unit_price,
        unit_cost,
        gross_amount,
        discount_amount,
        net_revenue,
        cogs,
        gross_profit,
        profit_margin_pct
    from {{ ref('int_order_margins') }}
),

dim_customers as (
    select
        customer_key,
        customer_id,
        valid_from,
        valid_to
    from {{ ref('dim_customers') }}
),

dim_products as (
    select
        product_key,
        product_id
    from {{ ref('dim_products') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['m.order_item_id']) }} as fct_order_item_key,
    m.order_item_id,
    m.order_id,
    coalesce(dc.customer_key, '{{ var("unknown_key") }}') as customer_key,
    coalesce(dp.product_key, '{{ var("unknown_key") }}') as product_key,
    coalesce({{ date_to_key('m.order_date') }}, {{ var('unknown_date_key') }}) as order_date_key,
    m.session_id,
    m.order_date as order_timestamp,
    m.order_status,
    m.payment_method,
    m.quantity,
    m.unit_price,
    m.unit_cost,
    m.gross_amount,
    m.discount_amount,
    m.net_revenue,
    m.cogs,
    m.gross_profit,
    m.profit_margin_pct
from margins m
left join dim_customers dc
    on m.customer_id = dc.customer_id
    and m.order_date >= dc.valid_from
    and (m.order_date < dc.valid_to or dc.valid_to is null)
left join dim_products dp
    on m.product_id = dp.product_id
