{{
    config(
        materialized='view',
        quote=False
    )
}}

select 'hello, world!' as col, 'test' as "CamelCase.Col"
union all
select 'hello, world!' as col, 'test2' as "CamelCase.Col"