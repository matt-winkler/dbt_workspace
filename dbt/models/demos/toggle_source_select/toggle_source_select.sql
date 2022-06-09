

{% if var('s1_updated', False) and var('s2_updated', True) %}
   
   select * from {{source('test1', 't1')}}
   union all
   select * from {{source('test2', 't2')}}

{% elif var('s1_updated', False) %}

   select * from {{source('test1', 't1')}}

{% elif var('s2_updated', True) %}

   select * from {{source('test2', 't2')}}

{% else %}

{% endif %}
