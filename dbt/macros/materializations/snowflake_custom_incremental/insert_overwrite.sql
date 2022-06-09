{% macro get_insert_overwrite_sql(target, source, unique_key, dest_columns, predicates=none) -%}
  {{ adapter.dispatch('insert_overwrite_get_sql', 'dbt')(target, source, unique_key, dest_columns, predicates) }}
{%- endmacro %}

{% macro snowflake__insert_overwrite_get_sql(target, source_sql, unique_key, dest_columns, predicates) -%}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute='name')) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {%- set dml -%}

        {{ insert_overwrite_get_sql(target, source_sql, unique_key, dest_columns, predicates) }}

    {%- endset -%}

    {% do return(snowflake_dml_explicit_transaction(dml)) %}

{% endmacro %}


{% macro insert_overwrite_get_sql(target, source, unique_key, dest_columns, predicates) -%}
    {%- set predicates = [] if predicates is none else [] + predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute='name')) -%}
    {%- set update_columns = config.get('merge_update_columns', default = dest_columns | map(attribute="quoted") | list) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    insert overwrite into {{ target }}
      select * from {{ source }}

{% endmacro %}