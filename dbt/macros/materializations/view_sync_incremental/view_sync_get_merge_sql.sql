{% macro view_sync_get_merge_sql(target, source, unique_key, dest_columns, predicates=none) -%}
  {{ adapter.dispatch('view_sync_get_merge_sql', 'dbt')(target, source, unique_key, dest_columns, predicates) }}
{%- endmacro %}

{% macro snowflake__view_sync_get_merge_sql(target, source_sql, unique_key, dest_columns, predicates) -%}

    {#
       Workaround for Snowflake not being happy with a merge on a constant-false predicate.
       When no unique_key is provided, this macro will do a regular insert. If a unique_key
       is provided, then this macro will do a proper merge instead.
    #}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute='name')) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {%- set dml -%}

        {{ view_sync_get_sql(target, source_sql, unique_key, dest_columns, predicates) }}

    {%- endset -%}

    {% do return(snowflake_dml_explicit_transaction(dml)) %}

{% endmacro %}


{% macro view_sync_get_sql(target, source, unique_key, dest_columns, predicates) -%}
    {#- set predicates = [] if predicates is none else [] + predicates -#}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}
    
    insert overwrite into {{ target }} ( {{ dest_cols_csv }} )
      select {{ dest_cols_csv }} from {{ source }}

{% endmacro %}