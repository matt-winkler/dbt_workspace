{% snapshot snpsht__snowflake_metering_history %}

{{
    config(
        unique_key = 'end_time',
        strategy   = 'timestamp',
        updated_at = 'end_time'
    )
}}

select 
    {{dbt_utils.surrogate_key(['service_type', 'entity_id', 'end_time'])}} as meter_id,
    * 
    
from {{ source('snowflake_meta', 'metering_history') }}

{% endsnapshot %}