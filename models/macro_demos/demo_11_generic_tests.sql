{{ config(
    materialized='view',
    enabled=false,
    tags=['macro_demo', 'demo_11']
) }}

{#
  The custom generic tests (valid_ticket_types, rating_in_range) are defined in
  macros/11_generic_tests/test_business_rule.sql and registered in schema_demo_11_generic_tests.yml.
  Run: dbt test --select demo_11_generic_tests (after enabling)
#}

SELECT
    t.ticket_id,
    t.customer_id,
    t.house_id,
    t.visit_date,
    t.ticket_type,
    v.satisfaction_rating
FROM {{ ref('stg_sales__tickets') }} t
LEFT JOIN {{ ref('stg_visitor_feedback__visitor_feedback') }} v
    ON t.ticket_id = v.ticket_id
