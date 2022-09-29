{% test data_type_validation(model) %}
  
  {% set cols = get_column_names(model) %}

  {{log(cols, info=true)}}

{% endtest %}