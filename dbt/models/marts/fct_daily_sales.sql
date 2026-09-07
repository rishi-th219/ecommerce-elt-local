{#-
    Periodic snapshot fact -- one row per calendar day of trading, pre-aggregated
    from `fct_orders`.

    Kimball's second fact table flavour: the transaction fact answers "what
    happened in this order line", the periodic snapshot answers "how did the
    business perform on this day" without scanning the line-item grain. Days
    with no trading are absent by design; join to `dim_date` for a dense series.

    Grain: one row per `order_date_key`.
-#}

with order_lines as (
    select
        order_date_key,
        order_id,
        customer_key,
        order_status,
        quantity,
        gross_amount,
        discount_amount,
        net_revenue,
        cogs,
        gross_profit
    from {{ ref('fct_orders') }}
)

select
    order_date_key as date_key,
    count(distinct order_id) as order_count,
    count(distinct customer_key) as customer_count,
    count(*) as order_line_count,
    sum(quantity) as units_sold,
    cast(sum(gross_amount) as numeric(14, 2)) as gross_amount,
    cast(sum(discount_amount) as numeric(14, 2)) as discount_amount,
    cast(sum(net_revenue) as numeric(14, 2)) as net_revenue,
    cast(sum(cogs) as numeric(14, 2)) as cogs,
    cast(sum(gross_profit) as numeric(14, 2)) as gross_profit,
    cast(round(sum(gross_profit) / nullif(sum(net_revenue), 0) * 100.0, 2) as numeric(10, 2)) as profit_margin_pct,
    cast(sum(case when order_status = 'completed' then net_revenue else 0 end) as numeric(14, 2)) as completed_net_revenue,
    cast(sum(case when order_status = 'returned' then net_revenue else 0 end) as numeric(14, 2)) as returned_net_revenue,
    cast(sum(case when order_status = 'cancelled' then net_revenue else 0 end) as numeric(14, 2)) as cancelled_net_revenue
from order_lines
group by order_date_key
