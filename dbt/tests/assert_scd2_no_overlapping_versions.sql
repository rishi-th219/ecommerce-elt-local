-- Singular test: no customer may have two SCD Type 2 versions whose validity
-- windows overlap. `valid_to > valid_from` (asserted separately) is not enough:
-- a duplicated source event can open a new version before the previous one was
-- closed, which would make a point-in-time join in fct_orders return two rows
-- for the same order line and double count revenue.
with versions as (
    select
        customer_id,
        customer_key,
        valid_from,
        valid_to,
        lead(valid_from) over (
            partition by customer_id
            order by valid_from
        ) as next_valid_from
    from {{ ref('dim_customers') }}
)

select
    customer_id,
    customer_key,
    valid_from,
    valid_to,
    next_valid_from
from versions
where next_valid_from is not null
  and (valid_to is null or valid_to > next_valid_from)
