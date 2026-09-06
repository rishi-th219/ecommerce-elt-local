{% if target.type == 'sqlserver' %}
with date_spine as (
    select
        dateadd(day, value, cast('2023-01-01' as date)) as full_date
    from generate_series(0, 1460)
)

select
    convert(int, convert(varchar(8), full_date, 112)) as date_key,
    full_date,
    ((datepart(weekday, full_date) + @@datefirst - 2) % 7) + 1 as day_of_week,
    datename(weekday, full_date) as day_name,
    datepart(day, full_date) as day_of_month,
    datepart(month, full_date) as month,
    datename(month, full_date) as month_name,
    datepart(quarter, full_date) as quarter,
    concat('Q', cast(datepart(quarter, full_date) as varchar(1))) as fiscal_quarter,
    datepart(year, full_date) as year,
    cast(case when ((datepart(weekday, full_date) + @@datefirst - 2) % 7) + 1 in (6, 7) then 1 else 0 end as bit) as is_weekend
from date_spine
{% else %}
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
{% endif %}
