{{
    config(
        materialized='view'
    )
}}

-- this effectively performs an insert like we had in the original stored procedure
with source_data as (
    select * from {{ source('raw', 'orders')}}
)

select * from source_data