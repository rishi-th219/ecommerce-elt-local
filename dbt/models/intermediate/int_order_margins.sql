{#-
    Line-item economics: revenue, COGS and gross profit at the order-item grain.

    The revenue arithmetic is computed once in `line_economics` and referenced
    downstream, rather than repeating `(quantity * unit_price) - discount_amount`
    in every measure.
-#}

with order_items as (
    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount_amount
    from {{ ref('stg_order_items') }}
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
),

line_economics as (
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
        p.product_name,
        p.category,
        oi.quantity,
        oi.unit_price,
        p.unit_cost,
        oi.discount_amount,
        cast(round(oi.quantity * oi.unit_price, 2) as numeric(12, 2)) as gross_amount,
        cast(round((oi.quantity * oi.unit_price) - oi.discount_amount, 2) as numeric(12, 2)) as net_revenue,
        cast(round(oi.quantity * p.unit_cost, 2) as numeric(12, 2)) as cogs
    from order_items oi
    inner join orders o
        on oi.order_id = o.order_id
    inner join products p
        on oi.product_id = p.product_id
)

select
    order_item_id,
    order_id,
    customer_id,
    session_id,
    order_date,
    order_status,
    payment_method,
    product_id,
    sku,
    product_name,
    category,
    quantity,
    unit_price,
    unit_cost,
    gross_amount,
    discount_amount,
    net_revenue,
    cogs,
    cast(net_revenue - cogs as numeric(12, 2)) as gross_profit,
    cast(round((net_revenue - cogs) / nullif(net_revenue, 0) * 100.0, 2) as numeric(10, 2)) as profit_margin_pct
from line_economics
