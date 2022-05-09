-- When you reference this in another model, notice how the schema location is changed from the project default.

{{
    config(
        tags=['custom_schema_demo']
    )
}}

select 'Hello, world!' as my_column