{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'insert_overwrite',
        on_schema_change     = 'sync_all_columns',
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
    dbt implements insert_overwrite on Snowflake as truncate + re-insert:
      INSERT OVERWRITE INTO <target> SELECT ... FROM <source>
    This deletes all rows in the target and reinserts whatever the SELECT
    returns. The scope of the overwrite is determined by the is_incremental()
    WHERE clause below — not by cluster_by. cluster_by is a performance
    optimisation (micro-partition pruning), not a correctness requirement.

  WHY INSERT OVERWRITE OVER DELETE+INSERT HERE:
    Monthly aggregates are fully recomputed from scratch — every row in
    the month window is recalculated. insert_overwrite expresses this
    intent directly and avoids a separate DELETE statement.

  CLUSTER_BY revenue_month — strongly recommended for performance:
    Without it, Snowflake must scan all micro-partitions during the overwrite.
    With it, Snowflake prunes to only the partitions touched by the WHERE filter.

  FILTER LOGIC:
    Recomputes the current month (in-progress data) and the prior month
    (absorbs corrections to already-closed monthly figures).
    date_trunc('month', ...) ensures the filter aligns to partition edges.

  on_schema_change = 'sync_all_columns':
    Because partitions are fully overwritten, adding a new column
    upstream will appear correctly in the rewritten partitions.
    Unaffected historical month-partitions will have NULL for the new
    column until they are next included in an overwrite window.
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
