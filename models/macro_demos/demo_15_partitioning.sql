{{ config(
    materialized='incremental',
    unique_key='visit_id',
    enabled=false,
    tags=['macro_demo', 'demo_15'],
    post_hook="{{ get_cluster_by_sql(var('cluster_columns', ['visit_date', 'house_id'])) }}"
) }}

{#
  Snowflake: CLUSTER BY (visit_date, house_id) applied via post-hook.
  Override cluster columns at runtime:
    dbt run --vars '{"cluster_columns": ["visit_date", "customer_id"]}' --select demo_15_partitioning
  BigQuery: swap get_cluster_by_sql for a partition_by config key (see macro docs).
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

{% if is_incremental() %}
WHERE {{ incremental_filter('visit_date') }}
{% endif %}
