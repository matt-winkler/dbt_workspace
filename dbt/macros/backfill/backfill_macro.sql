{% macro backfill(node_name, date_column_name, target_table, start_date, end_date, backfill_interval=7) %}
   
   {% set log_msg %}
      running backfill process on model {{node_name}}
   {% endset %}
   {{ log(log_msg, info=True) }}

   {% set lookup_key = 'model' ~ '.' ~ project_name ~ '.' ~ node_name %}
   {% set model_sql = graph.nodes[lookup_key]['raw_sql'] %}

   {# -- the backfill-watermark is to treat model config separately from other jinja statements, if using. 
      -- overall, suggest keeping the model config / macro-ing with this approach VERY LIGHT - it'll be hard to regex here if not #} 
   {% set model_sql = modules.re.sub('{{.+}}--backfill-watermark--', '', model_sql, flags=modules.re.S) %}

   {{ log('de-configured model sql' ~ model_sql, info=True) }}

   {% set backfill_query = dbt_utils.date_spine("day", "'" ~ start_date ~ "'", "'" ~ end_date ~ "'") %}
   {% set backfill_query_results = run_query(backfill_query) %}
   {% set backfill_dates = [] %}

   {% for date_value in backfill_query_results.columns[0].values() %}
      {{ backfill_dates.append(
          date_value
         ) 
      }}
   {% endfor %}

   {% set backfill_sql %}
     
     {% for b_date in backfill_dates %}

        {% if loop.index % backfill_interval == 0 %}
          
          {% set b_start = (b_date - modules.datetime.timedelta(days=backfill_interval)).strftime('%Y-%m-%d') %}
          {% set b_date = b_date.strftime('%Y-%m-%d') %}

          {% set insert_sql %}
             insert into {{target_table}} {{model_sql}} ~ 'where ' ~ {{date_column_name}} >= '{{b_start}}' and {{date_column_name}} < '{{b_date}}';
          {% endset %}
          
          {{ log(insert_sql, info=true) }}
        
        {% endif %} 

     {% endfor %}

   {% endset %}

{% endmacro %}