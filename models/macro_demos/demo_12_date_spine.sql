{{ config(
    materialized='table',
    enabled=false,
    tags=['macro_demo', 'demo_12']
) }}

{#
  Date spine joined to fct_visits to ensure every day in the season appears,
  even days with zero visits (gaps would be invisible without a spine).
  Activate: dbt run --select demo_12_date_spine
#}

WITH spine AS (

    {{ generate_date_spine(
        start_date=var('halloween_analysis_start_date', '2023-01-01'),
        end_date='2023-12-31'
    ) }}

),

daily_visits AS (

    SELECT
        visit_date,
        COUNT(*)                   AS total_visits,
        SUM(total_visit_spend)     AS total_revenue,
        AVG(satisfaction_rating)   AS avg_satisfaction
    FROM {{ ref('fct_visits') }}
    GROUP BY 1

)

SELECT
    s.spine_date                            AS visit_date,
    COALESCE(d.total_visits, 0)             AS total_visits,
    COALESCE(d.total_revenue, 0)            AS total_revenue,
    d.avg_satisfaction
FROM spine s
LEFT JOIN daily_visits d
    ON s.spine_date = d.visit_date
ORDER BY 1
