{{
    config(
        materialized='view'
    )
}}

select 'placed' as "OrderStatus", 1 as order_status_id
union all
select 'shipped' as "OrderStatus", 2 as order_status_id
union all
select 'invalidstatuscode' as "OrderStatus", 3 as order_status_id