{#

This piece of total ART was written by the one, the only - Serge G (aka @serge in dbt community slack)

Check out the thread in slack here: 
https://getdbt.slack.com/archives/CJN7XRF1B/p1616024325013100?thread_ts=1614562753.060700&cid=CJN7XRF1B

#}

{%- macro unpack_nested_json(model, variant_column_name) %}
    {%-set parse_json %}
        select distinct path                                                   as path
                    , typeof(value)                                            as type
                    , rtrim(regexp_replace(path, '[^a-z_]+', '_'), '_')        as alias
        from {{ model }},
            lateral flatten(input => parse_json({{  variant_column_name }}), recursive => true, outer => true) t
        where typeof(t.value) NOT in  ('NULL_VALUE', 'OBJECT')
        and regexp_substr(t.path, '\\[\\d+\\]') is null
    {% endset -%}

    {%- set parse_results = run_query(parse_json) -%}

    {% if execute -%}
        {%- for path,type,alias in parse_results.rows %}
            GET({{ variant_column_name }}, '{{ path| trim }}')::{{ type | lower }} as {{ alias | lower }}{{ ',' if not loop.last else '' }}
        {% endfor -%}
    {% endif %}
{% endmacro -%}