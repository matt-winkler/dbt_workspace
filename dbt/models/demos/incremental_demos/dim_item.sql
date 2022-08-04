{{
    config(
        materialized='table'
    )
}}
-- this could totally be an incremental with the item_key as the unique_key

with metadata as (
    select * from {{ref('stg_metadata')}}
),

stg_transactions as (
    select * from {{ref('prep_telemetry__transaction_v1')}}
),

joined as (
    select t.item_key,
           t.game_id,
           m.title_code,
           m.item_name,
           m.item_price,
           m.str,
           m.con,
           m.int,
           t.name
    from   stg_transactions t
    left join   stg_metadata m 
      on t.item_key = m.item_key
    
)

select * from joined