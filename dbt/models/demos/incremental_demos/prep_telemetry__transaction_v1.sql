{{
    config(
        materialized='table'
    )
}}

-- this might be an insert into a static table, say called "prep_telemetry__transaction" (custom materialization with explicit inserts)
-- for transaction there are ~ 6 temp table steps
-- explore use of aliases and post_hook vs. custom materialization
-- or, they might union the result of this view and the prep_telemetry__transaction_v2 view together

{% set constant_fields = ['game_id', 'event_type', 'dt', 'hour'] %}
{% set backfill = var('backfill', False) %}
{% set start_date = var('start_date', '2022-06-01') %}
{% set end_date = var('end_date', '2022-07-01') %}
{% set game_id = var('game_id', 1) %}

with transactions as (
    select 
           {{constant_fields|join(', ')}}
           ,parse_json(event_params) as event_params
    from {{ source('games', 'game_telemetry') }}
    where 1=1
    and game_id = {{ game_id }}
    and event_type = 'transaction'
    {% if run_mode == 'backfill' %}
      and dt >= '{{start_date}}'
    {% else %}
      and dt between '{{start_date}}' and '{{end_date}}'
    {% endif %}

),

transformed as (
    select {{constant_fields|join(', ')}}
           ,
           --mapdata.title_code,
           {{transaction_transform(version_number, start_date=start_date, end_date=end_date)}}
    from source_data
    --left join mapdata 
),

login_transform as (

  where event_type = 'login'
)

select * from transformed