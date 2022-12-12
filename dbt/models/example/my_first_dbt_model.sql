{{
    config(
        materialized='table'
    )
}}

select 1 as id
union all
select null as id