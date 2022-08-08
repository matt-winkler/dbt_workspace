{% macro cast_array_as_type(array, target_type) %}
  
  {% for item in array %}
     
     {%- if not loop.last %}
       cast({{ item }} as {{ target_type }}) as {{ item }},
     {%- else %}
       cast({{ item }} as {{ target_type }}) as {{ item }}
     {%- endif -%}

  {% endfor %}

{% endmacro %}