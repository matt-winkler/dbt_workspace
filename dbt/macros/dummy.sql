{% macro dummy() %}
    {{ log(var('dbt_artifacts_schema'), False) }}
{% endmacro %}