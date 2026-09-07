{#-
    Conformed calendar dimension.

    The spine bounds come from `date_spine_start` / `date_spine_days` in
    dbt_project.yml, so extending the calendar is a variable change rather than
    an edit to two dialect-specific SQL branches.

    Row `{{ var('unknown_date_key') }}` is the Kimball Unknown member, used by
    facts whose date is missing or unparseable.
-#}

{%- set spine_start = var('date_spine_start') -%}
{%- set spine_days = var('date_spine_days') -%}

{% if target.type == 'sqlserver' %}

with date_spine as (
    select
        dateadd(day, value, cast('{{ spine_start }}' as date)) as full_date
    from generate_series(0, {{ spine_days }})
),

calendar as (
    select
        convert(int, convert(varchar(8), full_date, 112)) as date_key,
        full_date,
        ((datepart(weekday, full_date) + @@datefirst - 2) % 7) + 1 as day_of_week,
        cast(datename(weekday, full_date) as varchar(30)) as day_name,
        datepart(day, full_date) as day_of_month,
        datepart(month, full_date) as month,
        cast(datename(month, full_date) as varchar(30)) as month_name,
        datepart(quarter, full_date) as quarter,
        cast(concat('Q', cast(datepart(quarter, full_date) as varchar(1))) as varchar(10)) as fiscal_quarter,
        datepart(year, full_date) as year,
        {{ bool_flag('((datepart(weekday, full_date) + @@datefirst - 2) % 7) + 1 in (6, 7)') }} as is_weekend
    from date_spine
),

unknown_member as (
    select
        cast({{ var('unknown_date_key') }} as int) as date_key,
        cast('{{ var("scd2_beginning_of_time") }}' as date) as full_date,
        cast(1 as int) as day_of_week,
        cast('Unknown' as varchar(30)) as day_name,
        cast(1 as int) as day_of_month,
        cast(1 as int) as month,
        cast('Unknown' as varchar(30)) as month_name,
        cast(1 as int) as quarter,
        cast('Unknown' as varchar(10)) as fiscal_quarter,
        cast(1900 as int) as year,
        {{ cast_bool(bool_literal(false)) }} as is_weekend
)

{% else %}

with date_spine as (
    select
        cast(datum as date) as full_date
    from generate_series(
        cast('{{ spine_start }}' as date),
        cast('{{ spine_start }}' as date) + interval '{{ spine_days }} day',
        interval '1 day'
    ) as datum
),

calendar as (
    select
        to_char(full_date, 'YYYYMMDD')::integer as date_key,
        full_date,
        extract(isodow from full_date)::integer as day_of_week,
        cast(trim(to_char(full_date, 'FMDay')) as varchar(30)) as day_name,
        extract(day from full_date)::integer as day_of_month,
        extract(month from full_date)::integer as month,
        cast(trim(to_char(full_date, 'FMMonth')) as varchar(30)) as month_name,
        extract(quarter from full_date)::integer as quarter,
        cast('Q' || extract(quarter from full_date)::text as varchar(10)) as fiscal_quarter,
        extract(year from full_date)::integer as year,
        {{ bool_flag('extract(isodow from full_date) in (6, 7)') }} as is_weekend
    from date_spine
),

unknown_member as (
    select
        cast({{ var('unknown_date_key') }} as integer) as date_key,
        cast('{{ var("scd2_beginning_of_time") }}' as date) as full_date,
        cast(1 as integer) as day_of_week,
        cast('Unknown' as varchar(30)) as day_name,
        cast(1 as integer) as day_of_month,
        cast(1 as integer) as month,
        cast('Unknown' as varchar(30)) as month_name,
        cast(1 as integer) as quarter,
        cast('Unknown' as varchar(10)) as fiscal_quarter,
        cast(1900 as integer) as year,
        {{ cast_bool(bool_literal(false)) }} as is_weekend
)

{% endif %}

select * from calendar
union all
select * from unknown_member
