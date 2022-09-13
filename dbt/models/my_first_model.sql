{{
    config(
        materialized='table'
    )
}}


select 'hello, world!' as col
union all
select 'hello, world!' as col