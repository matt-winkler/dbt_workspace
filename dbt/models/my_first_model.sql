{{
    config(
        materialized='table'
    )
}}


select 'hello, world!' as col, fail as something
union all
select 'hello, world!' as col