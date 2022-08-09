{{
    config(
        materialized='table', 
        tags=['classification=sensitive'],
        post_hook="{{dbt_tag_as_snowflake_object_tag(model)}}"
    )
}}

select 'hello, world!' as col