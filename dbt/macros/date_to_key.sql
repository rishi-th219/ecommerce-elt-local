{% macro date_to_key(date_column) -%}
    {#- Cross-platform date to integer YYYYMMDD key converter -#}
    {%- if target.type == 'sqlserver' -%}
        CONVERT(INT, CONVERT(VARCHAR(8), {{ date_column }}, 112))
    {%- else -%}
        to_char({{ date_column }}, 'YYYYMMDD')::integer
    {%- endif -%}
{%- endmacro %}
