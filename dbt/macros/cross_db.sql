{#-
    Cross-engine helper macros.

    The project targets both Microsoft SQL Server (primary) and PostgreSQL
    (portability proof). Anything that differs in dialect between the two
    lives here so models stay free of inline `{% if target.type %}` blocks.
-#}

{% macro assert_supported_target(macro_name) -%}
    {%- if target.type not in ['sqlserver', 'postgres'] -%}
        {{ exceptions.raise_compiler_error(
            "ecommerce_elt: macro '" ~ macro_name ~ "' has no implementation for adapter '"
            ~ target.type ~ "'. Supported adapters: sqlserver, postgres."
        ) }}
    {%- endif -%}
{%- endmacro %}


{% macro type_bool() -%}
    {#- Boolean storage type: SQL Server has no BOOLEAN, it uses BIT -#}
    {{- assert_supported_target('type_bool') -}}
    {%- if target.type == 'sqlserver' -%}bit{%- else -%}boolean{%- endif -%}
{%- endmacro %}


{% macro cast_bool(expression) -%}
    {#- Cast an already-boolean-typed expression to the engine's boolean type -#}
    cast({{ expression }} as {{ type_bool() }})
{%- endmacro %}


{% macro bool_flag(condition) -%}
    {#-
        Turn a SQL predicate into a stored boolean column. SQL Server stores
        BIT (1/0); PostgreSQL stores BOOLEAN and rejects an integer -> boolean
        cast outright, so the two branches cannot share a CASE expression.
    -#}
    cast(case when {{ condition }} then {{ bool_literal(true) }} else {{ bool_literal(false) }} end as {{ type_bool() }})
{%- endmacro %}


{% macro bool_literal(value) -%}
    {#- Engine-appropriate boolean literal for WHERE / JOIN predicates -#}
    {{- assert_supported_target('bool_literal') -}}
    {%- if target.type == 'sqlserver' -%}
        {%- if value -%}1{%- else -%}0{%- endif -%}
    {%- else -%}
        {%- if value -%}true{%- else -%}false{%- endif -%}
    {%- endif -%}
{%- endmacro %}


{% macro days_between(start_expression, end_expression) -%}
    {#- Whole days elapsed between two timestamps -#}
    {{- assert_supported_target('days_between') -}}
    {%- if target.type == 'sqlserver' -%}
        datediff(day, {{ start_expression }}, {{ end_expression }})
    {%- else -%}
        (cast({{ end_expression }} as date) - cast({{ start_expression }} as date))
    {%- endif -%}
{%- endmacro %}
