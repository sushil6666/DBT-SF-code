{#
  set_query_tag(team)
  Builds a structured JSON QUERY_TAG for Snowflake query attribution.
  Stamp every query with model name, environment, run ID, and team.
  Usage in model config pre_hook:
    pre_hook="{{ set_query_tag('analytics') }}"
  In Snowflake query history filter on QUERY_TAG:PARSE_JSON(QUERY_TAG):model::string = 'fct_visits'
#}

{% macro set_query_tag(team='unknown') %}

    {% if target.type == 'snowflake' %}
        ALTER SESSION SET QUERY_TAG = '{
            "model":        "{{ this.name }}",
            "schema":       "{{ this.schema }}",
            "env":          "{{ target.name }}",
            "run_id":       "{{ invocation_id }}",
            "team":         "{{ team }}",
            "executed_at":  "{{ run_started_at }}"
        }'
    {% endif %}

{% endmacro %}


{% macro clear_query_tag() %}

    {% if target.type == 'snowflake' %}
        ALTER SESSION SET QUERY_TAG = ''
    {% endif %}

{% endmacro %}
