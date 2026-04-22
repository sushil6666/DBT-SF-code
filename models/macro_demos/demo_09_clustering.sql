{{ config(
    materialized='table',
    enabled=false,
    tags=['macro_demo', 'demo_09'],
    post_hook="{{ apply_cluster_by(['visit_date', 'house_id']) }}"
) }}

SELECT
    visit_id,
    customer_id,
    house_id,
    visit_date,
    total_visit_spend,
    satisfaction_rating,
    number_of_tickets
FROM {{ ref('fct_visits') }}
