
{{
    config(
        tags=['ref_method']
    )
}}


select * from {{ ref('team_a_model') }}
{% if target.name == 'local' %}
where dt >= '2022-02-01'
{% endif %}