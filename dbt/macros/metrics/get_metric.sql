-- Example usage: 
-- dbt run-operation get_metric --args '{metric: orders_over_time}'

{% macro get_metric_sql_for(metric_name) %}

  {% set metrics = graph.metrics.values() %}
  
  {% set metric = (metrics | selectattr('name', 'equalto', metric_name) | list).pop() %}

  {{ return(metric) }}
  {{ dbt_utils.log_info(metric) }}
  {{ dbt_utils.log_info("hellow") }}

{% endmacro %}