{#  
    This macro is intended to run as a run-operation to help clean up schemas in dev databases (or wherever)
    
    Run the following dbt command to dry run the drop commands (basically print but not execute the drops) for all schemas in the current database:
      dbt run-operation cleanup_dev_schemas

    Run the following dbt command to actually drop all schemas in the current database that contain 'randy' and do not contain 'snowflake_meta' in the schema name:
      dbt run-operation cleanup_dev_schemas --args "{dry_run: False, schema_name_contains: 'randy', schema_name_does_not_contain: 'snowflake_meta'}"
    
#}
{% macro cleanup_dev_schemas(dry_run=True, schema_name_contains=None, schema_name_does_not_contain=None, database=target.database, role=None) %}
    {% if not (target.name == 'dev' or target.name == 'default') or not execute %}
      {{ return(None) }}
    {% endif %}


    {% set cleanup_query %}

      {% if role %}
        USE ROLE {{role}};
      {% endif %}

      WITH 
      
      SCHEMAS_TO_CLEAN AS (
        SELECT
          SCHEMA_NAME,
          CONCAT_WS('.', CATALOG_NAME, SCHEMA_NAME) AS SCHEMA_FQN -- FULLY QUALIFIED NAME = DATABASE.SCHEMA
        FROM 
          {{ target.database }}.INFORMATION_SCHEMA.SCHEMATA
        WHERE 
          SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
        {% if schema_name_contains %}
          AND SCHEMA_NAME ILIKE '%{{schema_name_contains}}%'
        {% endif %}
        {% if schema_name_does_not_contain %}
          AND NOT SCHEMA_NAME ILIKE '%{{schema_name_does_not_contain}}%'
        {% endif %}
      )

      SELECT 
        'DROP SCHEMA ' || SCHEMA_FQN || ';' as DROP_COMMANDS
      FROM 
        SCHEMAS_TO_CLEAN

  {% endset %}

    
  {% set drop_commands = run_query(cleanup_query).columns[0].values() %}


  {% if drop_commands %}
    {% for drop_command in drop_commands %}
      {% do log(drop_command, dry_run) %}
      {% if not dry_run %}
        {% do run_query(drop_command) %}
      {% endif %}
    {% endfor %}
  {% else %}
    {% do log('No schemas to clean.', True) %}
  {% endif %}
{% endmacro %}