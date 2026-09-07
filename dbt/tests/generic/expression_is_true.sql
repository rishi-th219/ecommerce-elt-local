{% test expression_is_true(model, expression, column_name=None) %}
{#-
    Assert that a SQL predicate holds for every row.

    This shadows `dbt_utils.expression_is_true` deliberately. That version emits
    `select 1 from ...`, and dbt-sqlserver materializes a test as a view, so SQL
    Server rejects it outright:

        Create View or Function failed because no column name was specified
        for column 1. (4511)

    Selecting the rows themselves gives every output column a name, works on
    both adapters, and makes `--store-failures` output readable.

    Rows where the predicate evaluates to NULL are not reported -- completeness
    is the job of the `not_null` tests.

    Usage (column level, the expression is appended to the column name):
        - expression_is_true:
            expression: ">= 0"

    Usage (model level, the expression stands alone):
        - expression_is_true:
            expression: "gross_amount - discount_amount = net_revenue"
-#}
{%- set predicate = (column_name ~ ' ' if column_name else '') ~ expression -%}

select *
from {{ model }}
where not ({{ predicate }})

{% endtest %}
