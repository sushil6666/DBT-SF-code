{{ config(
    materialized='view',
    enabled=false,
    tags=['macro_demo', 'demo_03']
) }}

SELECT
    customer_id,
    customer_name,
    {{ mask_pii('email', 'hash') }}    AS email_masked,
    {{ mask_pii('phone', 'partial') }} AS phone_masked,
    {{ mask_pii('address', 'redact') }} AS address_masked,
    age_group,
    loyalty_tier
FROM {{ ref('stg_customer_data__customers') }}
