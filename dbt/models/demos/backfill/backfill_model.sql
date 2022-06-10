{{
    config(
        materialized='table'
    )
}}--backfill-watermark--


select o_orderdate, o_totalprice, o_orderkey, o_custkey from raw.tpch_sf001.orders