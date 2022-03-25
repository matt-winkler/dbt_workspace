{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

-- note: could also use dbt_utils.surrogate_key to identify the change set

{% if is_incremental() %}
{% set max_date = get_max_date(relation=this, column_name='_date') %}
{% endif %}

-- add/remove or change dates in this dataset to adjust the "source" of the incremental model
with upstream_data as (
    select 1 as id, 123 as employee_id, '2022-01-01' as _date, 'active' as _status
    union all 
    select 2 as id, 234 as employee_id, '2022-02-01' as _date, 'active' as _status
    union all
    select 3 as id, 345 as employee_id, '2022-03-01' as _date, 'deleted' as _status
    union all select 4 as id, 456 as employee_id, '2022-04-01' as _date, 'active' as _status
    union all select 5 as id, 567 as employee_id, '2022-05-01' as _date, 'active' as _status
),

new_records as (
    select * 
    from upstream_data
    {% if is_incremental () %}
      where _date > '{{max_date}}'
    {% endif %}
),

{% if is_incremental() %}
changed_records as (
    select upstream.*
    from upstream_data upstream
    join {{this}} _current
      on upstream.id = _current.id
    left join new_records _new 
      on upstream.id = _new.id
    where upstream._status != _current._status
    and _new.id is null
),
{% endif %}

unioned as (
     select * from new_records
     {% if is_incremental() %}
     union all
     select * from changed_records
     {% endif %}
)

select * from unioned
