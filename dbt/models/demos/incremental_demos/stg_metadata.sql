{{
    config(
        materialized='view'
    )
}}

{% set game_id = var('game_id', 1) %}

select * 
from {{source('games', 'game_metadata')}}
--where game_id = {{game_id}}