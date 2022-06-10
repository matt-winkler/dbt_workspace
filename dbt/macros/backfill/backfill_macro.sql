{% macro backfill(project_name, node_name, date_part, start_date, end_date) %}
   
   {% set log_msg %}
      running backfill process on model {{node_name}}
   {% endset %}
   {{ log(log_msg, info=True) }}

   {% set lookup_key = 'model' ~ '.' ~ project_name ~ '.' ~ node_name %}
   {% set model_sql = graph.nodes[lookup_key]['raw_sql'] %}
   {{ log(model_sql, info=True) }}

   {% set backfill_query = dbt_utils.date_spine(date_part, "'" ~ start_date ~ "'", "'" ~ end_date ~ "'") %}
   {% set backfill_query_results = run_query(backfill_query) %}
   {% set backfill_dates = [] %}

   {% for date_value in backfill_query_results.columns[0].values() %}
      {{ backfill_dates.append(date_value) }}
   {% endfor %}

   {{ log(backfill_dates, info=True) }}

   {% set backfill_sql %}
   

   {% endset %}


   {{ log(backfill_sql, info=True) }}

{% endmacro %}