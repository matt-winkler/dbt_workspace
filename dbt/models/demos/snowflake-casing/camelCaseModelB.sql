{{
    config(
        materialized='table'
    )
}}

select 1 as id, 'placed' as "OrderStatus"
union all
select 2 as id, 'shipped' as "OrderStatus"
union all
select 3 as id, 'invalidstatuscode' as "OrderStatus"