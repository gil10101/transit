-- This Snowflake account rejects the SHOW-command grammar dbt-snowflake emits
-- (`show terse schemas ... limit`, `show objects in <schema>`) with syntax errors.
-- These overrides serve the same adapter contracts from information_schema, which is
-- version-stable. Column names/values mirror dbt/adapters/snowflake/impl.py parsing.

{% macro snowflake__list_schemas(database) %}
    {% call statement('list_schemas', fetch_result=True) %}
        select schema_name as "name"
        from {{ database }}.information_schema.schemata
    {% endcall %}
    {{ return(load_result('list_schemas').table) }}
{% endmacro %}


{% macro snowflake__show_objects_sql(schema, max_results_per_iter=10000, watermark=none) %}
    {%- set parts = (schema | string).replace('"', '').split('.') -%}
    {%- set database = parts[0] -%}
    {%- set schema_name = parts[1] -%}
    {%- set _sql -%}
        select
            table_catalog as "database_name",
            table_schema as "schema_name",
            table_name as "name",
            case table_type
                when 'BASE TABLE' then 'TABLE'
                when 'VIEW' then 'VIEW'
                else table_type
            end as "kind",
            case when coalesce(is_dynamic, 'NO') = 'YES' then 'Y' else 'N' end as "is_dynamic",
            case when coalesce(is_iceberg, 'NO') = 'YES' then 'Y' else 'N' end as "is_iceberg"
        from {{ database }}.information_schema.tables
        where table_schema = '{{ schema_name }}'
        {% if watermark is not none %} and table_name > '{{ watermark }}' {% endif %}
        order by table_name
        limit {{ max_results_per_iter }}
    {%- endset -%}
    {%- do return(_sql) -%}
{% endmacro %}


{% macro snowflake__list_function_relations_without_caching(schema_relation) %}
    {%- if schema_relation is string -%}
        {%- set schema = schema_relation -%}
    {%- else -%}
        {%- set schema = schema_relation.include(identifier=False) -%}
    {%- endif -%}
    {%- set parts = (schema | string).replace('"', '').split('.') -%}
    {% call statement('list_functions', fetch_result=True) %}
        select
            function_catalog as "catalog_name",
            function_schema as "schema_name",
            function_name as "name",
            'N' as "is_builtin"
        from {{ parts[0] }}.information_schema.functions
        where function_schema = '{{ parts[1] }}'
    {% endcall %}
    {{ return(load_result('list_functions').table) }}
{% endmacro %}
