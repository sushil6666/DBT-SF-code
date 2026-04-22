{{ config(
    materialized='table',
    enabled=false,
    tags=['macro_demo', 'demo_05'],
    post_hook="{{ attach_row_access_policy('haunt_zone_policy', 'zone_assignment') }}"
) }}

SELECT
    employee_id,
    employee_name,
    role,
    zone_assignment,
    hire_date,
    hourly_rate
FROM {{ ref('dim_employees') }}
