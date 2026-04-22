{{ config(
    materialized='table',
    enabled=false,
    tags=['macro_demo', 'demo_07'],
    pre_hook="{{ pre_swap_clone() }}",
    post_hook="{{ post_swap_table(this.schema ~ '.demo_07_zero_downtime_staging') }}"
) }}

{#
  Clone-and-swap demo on fct_visits.
  pre_hook  → clones current prod table to _backup before the build
  post_hook → atomically swaps the staging build into prod
  BI tools see zero downtime — the SWAP is instantaneous.
#}

SELECT
    visit_id,
    customer_id,
    house_id,
    visit_date,
    total_visit_spend,
    satisfaction_rating,
    number_of_tickets
FROM {{ ref('fct_visits') }}
