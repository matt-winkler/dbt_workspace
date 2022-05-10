
{{
    config(
        tags=['source_method']
    )
}}

--select * from {{ source('team_b_data', 'team_b_model_source') }}
select id, dt, my_column from {{ source('team_b_data', 'team_b_model_source') }}