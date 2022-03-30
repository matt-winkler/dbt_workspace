
-- force a dependency
-- depends_on: {{ ref('order_data') }}

select * 
from {{ 
  metrics.metric(
    metric_name='orders_over_time',
    grain='month',
    dimensions=['product_status']
    ) 
}}

where orders_over_time != 0