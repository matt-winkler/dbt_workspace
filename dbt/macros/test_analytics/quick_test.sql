{% macro quick_test() %}
  {% set something = builtins.ref('my_first_model') %}
  {{log(something, info=true)}}
{% endmacro %}