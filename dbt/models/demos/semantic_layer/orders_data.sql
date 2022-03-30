{{
    config(
        materialized='table'
    )
}}

select 1 as id, '2022-01-01 00:00:00'::timestamp as order_date, 'shipped' as product_status, 'oregon' as location, 100 as amount
union all
select 2 as id, '2022-01-02 00:00:00'::timestamp as order_date, 'placed' as product_status, 'oregon' as location, 200 as amount
union all
select 3 as id, '2022-01-03 00:00:00'::timestamp as order_date, 'shipped' as product_status, 'california' as location, 300 as amount
union all
select 4 as id, '2022-01-04 00:00:00'::timestamp as order_date, 'placed' as product_status, 'california' as location, 400 as amount
union all
select 5 as id, '2022-01-05 00:00:00'::timestamp as order_date, 'shipped' as product_status, 'california' as location, 500 as amount
union all
select 6 as id, '2022-02-01 00:00:00'::timestamp as order_date, 'shipped' as product_status, 'oregon' as location, 100 as amount
union all
select 7 as id, '2022-02-02 00:00:00'::timestamp as order_date, 'completed' as product_status, 'oregon' as location, 200 as amount
union all
select 8 as id, '2022-02-03 00:00:00'::timestamp as order_date, 'completed' as product_status, 'oregon' as location, 300 as amount
union all
select 9 as id, '2022-02-04 00:00:00'::timestamp as order_date, 'shipped' as product_status, 'california' as location, 400 as amount
union all
select 10 as id, '2022-02-05 00:00:00'::timestamp as order_date, 'shipped' as product_status, 'oregon' as location, 500 as amount