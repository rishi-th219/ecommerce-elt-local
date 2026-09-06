with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select
        order_id,
        customer_id,
        session_id,
        order_date,
        order_status,
        payment_method
    from {{ ref('stg_orders') }}
),

products as (
    select
        product_id,
        sku,
        product_name,
        category,
        unit_cost,
        retail_price
    from {{ ref('stg_products') }}
)

select
    oi.order_item_id,
    oi.order_id,
    o.customer_id,
    o.session_id,
    o.order_date,
    o.order_status,
    o.payment_method,
    oi.product_id,
    p.sku,
    p.category,
    oi.quantity,
    oi.unit_price,
    p.unit_cost,
    round((oi.quantity * oi.unit_price)::numeric, 2) as gross_amount,
    oi.discount_amount,
    round(((oi.quantity * oi.unit_price) - oi.discount_amount)::numeric, 2) as net_revenue,
    round((oi.quantity * p.unit_cost)::numeric, 2) as cogs,
    round((((oi.quantity * oi.unit_price) - oi.discount_amount) - (oi.quantity * p.unit_cost))::numeric, 2) as gross_profit,
    round(
        (
            (((oi.quantity * oi.unit_price) - oi.discount_amount) - (oi.quantity * p.unit_cost)) 
            / nullif(((oi.quantity * oi.unit_price) - oi.discount_amount), 0) * 100
        )::numeric, 2
    ) as profit_margin_pct
from order_items oi
inner join orders o
    on oi.order_id = o.order_id
inner join products p
    on oi.product_id = p.product_id
