{{ config(
    materialized='view',
    enabled=false,
    tags=['macro_demo', 'demo_08']
) }}

SELECT
    customer_id,
    customer_name,
    email,
    phone,
    loyalty_tier,
    {{ data_quality_score([
        {'col': 'customer_id',   'type': 'not_null',   'weight': 4},
        {'col': 'email',         'type': 'valid_email', 'weight': 3},
        {'col': 'customer_name', 'type': 'not_null',   'weight': 2},
        {'col': 'phone',         'type': 'not_null',   'weight': 1}
    ]) }} AS dq_score
FROM {{ ref('stg_customer_data__customers') }}
