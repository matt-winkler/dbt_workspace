{% macro get_game_columns() %}

   {% set game_columns = var('game_version_column_mappings')[var('game_id', 'default_id')][var('game_version', 'default_version')] %}

   {{return(game_columns)}}

{% endmacro %}