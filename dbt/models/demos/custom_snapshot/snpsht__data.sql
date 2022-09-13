
{{
    config(
        materialized = 'custom_snapshot'
    )
}}

select rpm_account_id,
       free_user as free_user_count,
       free_ingest as free_ingest_qty,
       start_date::timestamp as effective_start_timestamp,
       -- these are default values  assigned for new records
       -- changed records are handled by teh custom_snapshot materialization process
       '9999-12-31'::timestamp as effective_end_timestamp,
       True as is_active,
       current_timestamp as edw_create_timestamp, 
       current_timestamp as edw_update_timestamp, 
       current_user as edw_create_user, 
       current_user as edw_update_user
from {{ ref('vw__mock_source') }}


