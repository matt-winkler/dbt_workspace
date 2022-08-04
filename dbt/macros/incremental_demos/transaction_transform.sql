{% macro telemetry_transform(start_date, end_date) %}
  
  {% if start_date <= '2022-06-01' and end_date < '2022-06-01' %}
      
      event_params:asset_in:items:consumable:item_id as item_key,
      event_params:asset_in:items:consumable:name as name,
      
      -- this is a column name that's changed over time
      event_params:asset_in:items:consumable:quantity as amount

  {% elif start_date <= '2022-06-01' and end_date >= '2022-06-01' %}

      event_params:asset_in:items:consumable:item_id as item_key,
      event_params:asset_in:items:consumable:name as name,
      -- this is a column name that's changed
      event_params:asset_in:items:consumable:amount as amount

  {% endif %}
  
{% endmacro %}