{% snapshot snpsht__snowflake_query_history %}

{{
    config(
        unique_key = 'query_id',
        strategy   = 'timestamp',
        updated_at = 'end_time',
        target_schema = target.schema
    )
}}

select * from {{ source('snowflake_meta', 'query_history') }}

{% endsnapshot %}