{#-
    Product catalog dimension (SCD Type 1 -- attributes are overwritten in place).
    Includes the Kimball `-1` Unknown member so fact rows with an unresolvable
    product reference still join to a dimension row.
-#}

with products as (
    select
        product_id,
        sku,
        product_name,
        category,
        unit_cost,
        retail_price,
        created_at
    from {{ ref('stg_products') }}
),

catalog as (
    select
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
        product_id,
        sku,
        product_name,
        category,
        unit_cost,
        retail_price,
        cast(round((retail_price - unit_cost) / nullif(retail_price, 0) * 100.0, 2) as numeric(10, 2)) as baseline_margin_pct,
        created_at
    from products
),

unknown_member as (
    select
        cast('{{ var("unknown_key") }}' as varchar(64)) as product_key,
        cast('{{ var("unknown_key") }}' as varchar(64)) as product_id,
        cast('UNKNOWN' as varchar(64)) as sku,
        cast('Unknown Product' as varchar(255)) as product_name,
        cast('Unknown' as varchar(100)) as category,
        cast(0 as numeric(10, 2)) as unit_cost,
        cast(0 as numeric(10, 2)) as retail_price,
        cast(null as numeric(10, 2)) as baseline_margin_pct,
        cast('{{ var("scd2_beginning_of_time") }}' as {{ dbt.type_timestamp() }}) as created_at
)

select * from catalog
union all
select * from unknown_member
