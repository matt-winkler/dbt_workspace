{% macro gdp_create_environment(report=env_var('DBT_GDP_REPORT', 'test')) %}
  
  {% set sql %}
     
     select {{report}}

  {% endset %}

  {{log(sql, info=true)}}

{% endmacro %}