
{{
    config(
        tags=['source_method']
    )
}}


select * from {{ source('team_a_data', 'team_a_model') }}
{% if target.name == 'local' %}
where dt >= '2022-02-01'
{% endif %}