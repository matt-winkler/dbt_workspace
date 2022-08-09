{% macro dbt_tag_as_snowflake_object_tag(model) %}
  
  {% for tag in model.config.tags %}
    
    {% set tag_key = tag.split('=')[0] %}
    {% set tag_value = tag.split('=')[1] %}
    
    alter table {{model.name}} set tag {{tag_key}}='{{tag_value}}';

  {% endfor %}
   
{% endmacro %}