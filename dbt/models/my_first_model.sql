{{
    config(
        materialized='view'
    )
}}


select 1 as id, 'foo' as textcolumn
union all
select 1 as id, 'bar' as textcolumn