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
    cast(round(oi.quantity * oi.unit_price, 2) as numeric(12, 2)) as gross_amount,
    oi.discount_amount,
    cast(round((oi.quantity * oi.unit_price) - oi.discount_amount, 2) as numeric(12, 2)) as net_revenue,
    cast(round(oi.quantity * p.unit_cost, 2) as numeric(12, 2)) as cogs,
    cast(round(((oi.quantity * oi.unit_price) - oi.discount_amount) - (oi.quantity * p.unit_cost), 2) as numeric(12, 2)) as gross_profit,
    cast(round(
        (
            (((oi.quantity * oi.unit_price) - oi.discount_amount) - (oi.quantity * p.unit_cost)) 
            / nullif(((oi.quantity * oi.unit_price) - oi.discount_amount), 0) * 100.0
        ), 2
    ) as numeric(10, 2)) as profit_margin_pct
from order_items oi
inner join orders o
    on oi.order_id = o.order_id
inner join products p
    on oi.product_id = p.product_id
