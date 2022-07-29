

{% set queries_to_union = [] %}


{% if var('s1_updated', True) %}
    {% set query_to_append %}select * from {{source('test', 't1')}}{% endset %}
    {% do queries_to_union.append(query_to_append) %}
{% endif %}

-- {{source('test', 't2')}}
{% if var('s2_updated', False) %}
    {% set query_to_append %}select * from {{source('test', 't2')}}{% endset %}
    {% do queries_to_union.append(query_to_append) %}
{% endif %}
 
{{ queries_to_union | join(' 
    union all 
    ') 
}}