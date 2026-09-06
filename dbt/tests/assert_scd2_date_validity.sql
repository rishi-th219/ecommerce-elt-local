-- Singular test: Verify SCD Type 2 valid_to is strictly after valid_from
select
    customer_key,
    customer_id,
    valid_from,
    valid_to
from {{ ref('dim_customers') }}
where valid_to is not null
  and valid_to <= valid_from
