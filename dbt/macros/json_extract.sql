{% macro json_extract(column_name, json_key) -%}
    {#- Cross-platform JSON extraction for SQL Server (JSON_VALUE) and PostgreSQL (->>) -#}
    {%- if target.type == 'sqlserver' -%}
        JSON_VALUE({{ column_name }}, '$.{{ json_key }}')
    {%- else -%}
        ({{ column_name }}->>'{{ json_key }}')
    {%- endif -%}
{%- endmacro %}
