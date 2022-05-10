
{{
    config(
        tags=['ref_method']
    )
}}

select * from {{ ref('team_b_model_ref') }}