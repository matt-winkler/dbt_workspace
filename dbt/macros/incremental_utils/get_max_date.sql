{% macro get_max_date(relation, column_name) %}
   
   {% set sql %}
       select max({{column_name}}) from {{relation}}
   {% endset %}

   {% set results = run_query(sql) %}

   {{ return(results.columns[0].values()[0]) }}

{% endmacro %}
