-- see the frozen_dtypes_incremental materialization code in macros/materializations/frozen_dtypes_incremental/frozen_dtypes_incremental.sql
-- this example configured with on_schema_change='ignore' and allow_data_type_changes=False for the use case when DDL operations (e.g. ALTER TABLE) ARE NOT allowed in the target table
{{
    config(
        materialized='frozen_dtypes_incremental',
        unique_key='id',
        incremental_strategy='merge',
        on_schema_change='ignore',
        allow_data_type_changes=False
    )
}}


with source_data as (
    select 1 as id, 'foo' as text_col_1, 'bar' as text_col_2 union all
    select 2 as id, 'baz' as text_col_1, 'qux' as text_col_2 union all
    select 3 as id, 'quux' as text_col_1, 'quuz' as text_col_2 union all
    select 4 as id, 'corge' as text_col_1, 'grault' as text_col_2
    
    -- comment / uncomment this to add longer varchar length fields to the "source" data and test downstream table modification behavior
    union all select 5 as id, 'cnvcalocdsaioucdhsa' as text_col_1, 'aafdklasncdklascnadsdcdsacdsa' as text_col_2
)

select id,
       text_col_1,
       text_col_2

from source_data
