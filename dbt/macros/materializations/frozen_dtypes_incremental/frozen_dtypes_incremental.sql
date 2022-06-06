<<<<<<< HEAD:dbt/macros/materializations/view_sync_incremental/view_sync_incremental.sql
{% macro dbt_snowflake_validate_get_incremental_strategy(config) %}
  {#-- Find and validate the incremental strategy #}
  {%- set strategy = config.get("incremental_strategy", default="merge") -%}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected 'merge'
  {%- endset %}
  {% if strategy not in ['insert_overwrite'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return(strategy) %}
{% endmacro %}

{% macro dbt_snowflake_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key, dest_columns) %}
  {% if strategy == 'insert_overwrite' %}
    {% do return(view_sync_get_merge_sql(target_relation, tmp_relation, unique_key, dest_columns, predicates=none)) %}
  {% else %}
    {% do exceptions.raise_compiler_error('invalid strategy: ' ~ strategy) %}
  {% endif %}
{% endmacro %}

{% macro dbt_snowflake_view_sync_create_temp_relation(strategy, tmp_relation, sql) %}
  {% if strategy == 'insert_overwrite' %}
    {% do return(create_view_as(tmp_relation, sql)) %}
  {% else %}
    {% do exceptions.raise_compiler_error('invalid strategy: ' ~ strategy) %}
  {% endif %}
{% endmacro %}

{% materialization view_sync_incremental, adapter='snowflake' -%}
=======
{% materialization frozen_dtypes_incremental, adapter='snowflake' -%}
>>>>>>> 5a5b122c5411d837e6fe176672e768dd44e30b09:dbt/macros/materializations/frozen_dtypes_incremental/frozen_dtypes_incremental.sql

  {% set original_query_tag = set_query_tag() %}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {%- set allow_data_type_changes = config.get('allow_data_type_changes', True) -%}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {#-- Validate early so we don't run SQL if the strategy is invalid --#}
  {% set strategy = dbt_snowflake_validate_get_incremental_strategy(config) -%}
  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}

  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}

  {% elif existing_relation.is_view %}
    {#-- Can't overwrite a view with a table - we must drop --#}
    {{ log("Dropping relation " ~ target_relation ~ " because it is a view and this model is a table.") }}
    {% do adapter.drop_relation(existing_relation) %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}

  {% elif full_refresh_mode %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}

  {% else %}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    
    {#-- adjust data types (or not) in the target table based on config #}
    {% if allow_data_type_changes %}
      
       {% do adapter.expand_target_column_types(
           from_relation=tmp_relation, 
           to_relation=target_relation
        ) %}

    {% endif %}
    
    {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
    {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}
    {% set build_sql = dbt_snowflake_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key, dest_columns) %}
<<<<<<< HEAD:dbt/macros/materializations/view_sync_incremental/view_sync_incremental.sql
=======

>>>>>>> 5a5b122c5411d837e6fe176672e768dd44e30b09:dbt/macros/materializations/frozen_dtypes_incremental/frozen_dtypes_incremental.sql
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

<<<<<<< HEAD:dbt/macros/materializations/view_sync_incremental/view_sync_incremental.sql

=======
>>>>>>> 5a5b122c5411d837e6fe176672e768dd44e30b09:dbt/macros/materializations/frozen_dtypes_incremental/frozen_dtypes_incremental.sql
  {{ run_hooks(post_hooks) }}

  {% set target_relation = target_relation.incorporate(type='table') %}
  {% do persist_docs(target_relation, model) %}

  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}