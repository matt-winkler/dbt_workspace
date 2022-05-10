-- When you reference this in another model, notice how the schema location is changed from the project default.

{{
    config(
        tags=['source_method', 'ref_method']
    )
}}

with the_data as (
    select 1 as id, '2022-01-01'::date as dt, 'Hello, world!' as my_column union all
    select 2 as id, '2022-02-01'::date as dt, 'Hello, world!' as my_column
)

select * from the_data
{% if target.name == 'local' %}
where dt >= '2022-02-01'
{% endif %}