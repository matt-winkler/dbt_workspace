{{
    config(
        materialized='incremental',
        unique_key='id_version_sk',
        incremental_strategy='merge',
        merge_update_columns=get_game_columns(),
        on_schema_change='append_new_columns'
    )
}}

select {{dbt_utils.surrogate_key(['id', 'version'])}} as id_version_sk,
       *
from {{ref('stg_games_data')}}
