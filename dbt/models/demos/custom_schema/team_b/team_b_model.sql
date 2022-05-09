
{{
    config(
        tags=['custom_schema_demo']
    )
}}


select * from {{ ref('team_a_model') }}