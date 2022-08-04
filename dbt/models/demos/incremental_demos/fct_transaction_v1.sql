{{
    config(
        materialized='incremental',
        unique_key='id_version_sk',
        incremental_strategy='merge',
        merge_update_columns=get_game_columns(),
        on_schema_change='append_new_columns'
    )
}}

-- discuss generating the primary key for this table
with trans_data as (
    select
       t.*
    from   {{ref('prep_telemetry__transaction_v1')}} t
    join   {{ref('dim_item')}} dt 
      on t.item_key = dt.item_key
),

with_sk_id as (
    select {{dbt_utils.surrogate_key(['game_id', 'item_key', 'dt', 'hour'])}} as id_version_sk,
           * 
    from   trans_data
)

select * from with_sk_id

