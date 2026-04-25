{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'insert_overwrite',
        cluster_by           = ['revenue_month'],
        tags                 = ['macro_demo', 'demo_24', 'incremental', 'insert_overwrite']
    )
}}

{#
  STRATEGY: insert_overwrite
  -----------------------------------------------------------------------
  Maintains a monthly revenue rollup. Each run fully recomputes the
  current month and the prior month (for late in-month corrections),
  then overwrites those two partitions in the target table.

  HOW IT WORKS (Snowflake adapter):
    Snowflake does NOT have BigQuery-style named partitions.
    The adapter issues: INSERT OVERWRITE INTO <target> SELECT ... FROM <source>
    Snowflake uses the cluster_by column to identify which micro-partitions
    to overwrite. Without cluster_by this would replace the ENTIRE table.

  WHY INSERT OVERWRITE OVER DELETE+INSERT HERE:
    Monthly aggregates are fully recomputed from scratch — every row in
    the month partition is recalculated. insert_overwrite expresses this
    intent more directly and avoids a separate DELETE statement.

  CLUSTER_BY revenue_month — REQUIRED:
    This is not optional. Without it, INSERT OVERWRITE replaces all rows
    in the table on every run, making it equivalent to a full refresh.

  FILTER LOGIC:
    Recomputes the current month (in-progress data) and the prior month
    (absorbs corrections to already-closed monthly figures).
    date_trunc('month', ...) ensures the filter aligns to partition edges.
#}

with monthly_revenue as (

    select
        date_trunc('month', visit_date)                 as revenue_month,
        category,
        location,
        payment_method,
        sum(total_amount)                               as total_revenue,
        sum(quantity)                                   as total_units_sold,
        count(distinct transaction_id)                  as total_transactions,
        count(distinct customer_id)                     as unique_customers,
        avg(total_amount)                               as avg_transaction_value,
        max(total_amount)                               as max_transaction_value
    from {{ ref('fct_sales') }}

    {% if is_incremental() %}
        -- Recompute the current month and the previous month.
        -- date_trunc aligns the filter to partition boundaries so the
        -- overwrite covers exactly the two partitions being refreshed.
        where visit_date >= date_trunc(
            'month', dateadd('month', -1, current_date())
        )
    {% endif %}

    group by 1, 2, 3, 4

)

select * from monthly_revenue
