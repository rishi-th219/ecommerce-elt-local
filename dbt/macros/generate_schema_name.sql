{% macro generate_schema_name(custom_schema_name, node) -%}
    {#- Custom macro to ensure clean schema names (raw, staging, intermediate, marts) without default prefixes -#}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
