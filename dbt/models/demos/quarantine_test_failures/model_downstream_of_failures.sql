select id, 1 as static 
from {{ref('model_with_failures')}} a
{% if true %}
  left join (
      select id 
      from {{target.database}}.{{target.schema}}__dbt_test__audit.unique_my_first_model_id
      group by 1
   ) b
    on a.id = b.id 
  where b.id is null  
{% endif %}