{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
        Layer schemas (staging, intermediate, marts) are written verbatim on the
        deployment targets listed in the `prod_targets` var, so BI tools, the docs
        and the sample queries can rely on stable schema names.

        On any other target (e.g. a personal `dev` output, or CI) the schema is
        prefixed with the target name -- `dev_marts`, `ci_marts` -- so two people
        running `dbt run` against the same shared database cannot overwrite each
        other's tables, or production's.

        Override with: dbt run --target dev --vars '{prod_targets: [mssql]}'
    -#}
    {%- set prod_targets = var('prod_targets', ['mssql', 'postgres']) -%}

    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name in prod_targets -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.name | trim }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
