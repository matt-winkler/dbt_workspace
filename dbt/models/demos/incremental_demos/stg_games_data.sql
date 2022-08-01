{{
    config(
        materialized='table'
    )
}}


{% set game_specific_columns = get_game_columns() %}

with source_data as (
    select 
           -- id, version and title are assumed to pull into all games models. These could be abstracted into a variable as well
           id, 
           version, 
           title, 
           {{game_specific_columns|join(', ')}}
    from {{ source('games', 'raw_games_data') }}
    where 1=1
    and id = {{ var('game_id', 'default_id') }}
    and version = '{{ var("game_version", "default_version") }}'
    {% if run_mode == 'backfill' %}
      and date between {{ var('start_date') }} and {{ var('end_date') }}
    {% endif %}

),
-- sometimes, telemetry isn't properly implemented and that might be the case for some time
-- after launch is worse than before launch
-- needs ability to control the parsing logic on the telemetry by date range, then union the results. May also be inconsistent. 
renamed as (
    select id, version, title,
           {{cast_array_as_type(array=game_specific_columns, target_type='varchar(50)')}}
    from source_data
)

select * from renamed