{% macro generate_schema_name(custom_schema_name, node) -%}

    {#
        This custom macro overrides dbt's default schema generation logic.
        - If a `custom_schema_name` (from `+schema` in dbt_project.yml or config) is provided,
          it will use ONLY that name.
        - If no `custom_schema_name` is provided, it will fall back to the default schema
          defined in your `profiles.yml` for the current target.
    #}

    {%- if custom_schema_name is none -%}

        {{ target.schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}