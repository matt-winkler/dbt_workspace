{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    
    {% set log_msg='getting custom schema:\ntarget_name:' ~ target.name ~ '\ncustom_schema_name:' ~ custom_schema_name %}
    {% do log(log_msg, False) %}

    
    {%- if target.name == 'local' and custom_schema_name -%}
       
       {{ default_schema ~ '_' ~ custom_schema_name }}

    {%- elif custom_schema_name -%}
        
        {{ target.name.lower() ~ '_' ~ custom_schema_name }}
    
    {%- else -%}
        {{ default_schema }} 
    
    {%- endif -%}

{%- endmacro %}