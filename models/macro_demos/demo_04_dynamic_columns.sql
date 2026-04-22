{{ config(
    materialized='view',
    enabled=false,
    tags=['macro_demo', 'demo_04']
) }}

{# Dynamically selects all columns from stg_sales_transactions except internal load fields #}
{% set source_relation = ref('stg_sales_transactions__sales_transactions') %}

SELECT
    {{ get_columns_except(source_relation, exclude_cols=['_dbt_source_relation', '_loaded_at']) }}
FROM {{ source_relation }}
