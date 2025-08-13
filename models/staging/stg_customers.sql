-- models/staging/stg_customers.sql
-- Staging model for customers

{{ config(
    materialized='table',
    alias='stg_customers'
) }}

select
    c_custkey     as customer_id,
    c_name        as customer_name,
    c_address     as address,
    c_nationkey   as nation_id,
    c_phone       as phone,
    c_acctbal     as acct_balance,
    c_mktsegment  as market_segment,
    c_comment     as comment
from {{ source('semantic_data','CUSTOMER') }}
