-- Singular test: Verify net_revenue is always non-negative in the fact table
select
    order_item_id,
    net_revenue
from {{ ref('fct_orders') }}
where net_revenue < 0
