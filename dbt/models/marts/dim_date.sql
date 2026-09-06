with date_spine as (
    select
        datum::date as full_date
    from generate_series(
        '2023-01-01'::date,
        '2026-12-31'::date,
        '1 day'::interval
    ) as datum
)

select
    to_char(full_date, 'YYYYMMDD')::integer as date_key,
    full_date,
    extract(isodow from full_date)::integer as day_of_week,
    trim(to_char(full_date, 'FMDay')) as day_name,
    extract(day from full_date)::integer as day_of_month,
    extract(month from full_date)::integer as month,
    trim(to_char(full_date, 'FMMonth')) as month_name,
    extract(quarter from full_date)::integer as quarter,
    'Q' || extract(quarter from full_date)::text as fiscal_quarter,
    extract(year from full_date)::integer as year,
    case when extract(isodow from full_date) in (6, 7) then true else false end as is_weekend
from date_spine
order by date_key
