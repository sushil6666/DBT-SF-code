{{ config(
    materialized='table',
    enabled=false,
    tags=['macro_demo', 'demo_13'],
    pre_hook="{{ set_query_tag('analytics') }}",
    post_hook="{{ clear_query_tag() }}"
) }}

{#
  Every query run by this model will carry a structured JSON QUERY_TAG in Snowflake:
  { "model": "demo_13_query_tag", "env": "dev", "run_id": "...", "team": "analytics" }
  Inspect in Snowflake: SELECT QUERY_TAG, QUERY_TEXT FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  Filter: PARSE_JSON(QUERY_TAG):team::string = 'analytics'
#}

SELECT
    visit_id,
    customer_id,
    house_id,
    visit_date,
    total_visit_spend,
    satisfaction_rating
FROM {{ ref('fct_visits') }}
