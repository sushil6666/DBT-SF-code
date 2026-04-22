{{ config(
    materialized='table',
    tags=['macro_demo', 'demo_07']
) }}

{#
  Clone-and-swap pattern demo — hooks shown below but not active by default
  because the table must already exist for pre_swap_clone() to succeed.

  To activate on subsequent runs after this table exists:
    config(
        pre_hook="{{ pre_swap_clone() }}",
        post_hook="{{ post_swap_table(this.schema ~ '.demo_07_zero_downtime_staging') }}"
    )
  pre_hook  → CLONE current prod table to _backup before the build
  post_hook → ALTER TABLE staging SWAP WITH prod (instantaneous for BI tools)
#}

SELECT
    ticket_id,
    customer_id,
    visit_date,
    ticket_type,
    total_visit_spend,
    avg_rating
FROM {{ ref('fct_visits') }}
