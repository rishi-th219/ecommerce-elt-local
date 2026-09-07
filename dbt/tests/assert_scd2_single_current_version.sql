-- Singular test: exactly one version per customer may be flagged as current.
-- Two current rows would make `where is_current` queries double count a customer.
select
    customer_id,
    count(*) as current_version_count
from {{ ref('dim_customers') }}
where is_current = {{ bool_literal(true) }}
group by customer_id
having count(*) <> 1
