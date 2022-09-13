{% materialization custom_snapshot, adapter='snowflake' -%}

  {% set original_query_tag = set_query_tag() %}

  {#-- Set vars --#}
  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set grant_config = config.get('grants') %}
  {% set tmp_relation = make_temp_relation(this).incorporate(type='view') %}
  
  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none %}
    {%- call statement('main') -%}
      {{ create_table_as(False, target_relation, sql) }}
    {%- endcall -%}

  {% else %}
    {%- call statement('create_tmp_relation') -%}
      {{ create_view_as(tmp_relation, sql) }}
    {%- endcall -%}

    {%- call statement('main') -%}
      {{ custom_snapshot_build(source_relation=tmp_relation, target_relation=target_relation) }}
    {%- endcall -%}
  
  {% endif %}

  {% do drop_relation_if_exists(tmp_relation) %}

  {{ run_hooks(post_hooks) }}

  {% set target_relation = target_relation.incorporate(type='table') %}

  {% do persist_docs(target_relation, model) %}

  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}