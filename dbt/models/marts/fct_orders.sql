with margins as (
    select * from {{ ref('int_order_margins') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

dim_products as (
    select * from {{ ref('dim_products') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['m.order_item_id']) }} as fct_order_item_key,
    m.order_item_id,
    m.order_id,
    coalesce(
        dc_exact.customer_key,
        dc_current.customer_key
    ) as customer_key,
    dp.product_key,
    {{ date_to_key('m.order_date') }} as order_date_key,
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
left join dim_customers dc_exact
    on m.customer_id = dc_exact.customer_id
    and m.order_date >= dc_exact.valid_from
    and (m.order_date < dc_exact.valid_to or dc_exact.valid_to is null)
left join dim_customers dc_current
    on m.customer_id = dc_current.customer_id
    and dc_current.is_current = {% if target.type == 'sqlserver' %}1{% else %}true{% endif %}
left join dim_products dp
    on m.product_id = dp.product_id
