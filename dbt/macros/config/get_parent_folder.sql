{% macro get_parent_folder() %}
    
    {% set parent_folder = model.path.split('/')[-2] %}

    {# this is a default value from the dbt Cloud IDE #}
    {% if parent_folder == 'rpc' %}
      {% set parent_folder = target.user %}
    {% endif %}

    {{return(parent_folder)}}

{% endmacro %}