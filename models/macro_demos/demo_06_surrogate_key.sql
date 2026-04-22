{{ config(
    materialized='table',
    enabled=false,
    tags=['macro_demo', 'demo_06']
) }}

SELECT
    {{ cross_db_surrogate_key(['transaction_id', 'customer_id', 'visit_date']) }} AS sk_transaction,
    transaction_id,
    customer_id,
    visit_date,
    ticket_type,
    amount,
    payment_method
FROM {{ ref('stg_sales_transactions__sales_transactions') }}
