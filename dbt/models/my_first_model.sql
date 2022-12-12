{{
    config(
        materialized='view',
        quote=False
    )
}}

select 'hello, world!' as col
union all
select 'hello, world!' as col