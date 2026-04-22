{{ config(
    materialized='table',
    enabled=false,
    tags=['macro_demo', 'demo_01']
) }}

SELECT
    visit_id,
    customer_id,
    house_id,
    visit_date,
    total_visit_spend,
    satisfaction_rating,
    {{ audit_columns() }}
FROM {{ ref('fct_visits') }}
