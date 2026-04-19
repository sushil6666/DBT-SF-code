{% snapshot snp_ticket_sales_history %}

{{
    config(
        target_schema = 'snapshots',
        strategy      = 'check',
        unique_key    = 'sale_id',
        check_cols    = ['ticket_price', 'discount_percent', 'payment_method']
    )
}}

select * from {{ ref('fct_all_ticket_sales') }}

{% endsnapshot %}
