{% snapshot snp_haunted_house_attributes %}

{{
    config(
        target_schema = 'snapshots',
        strategy      = 'check',
        unique_key    = 'haunted_house_id',
        check_cols    = ['haunted_house_name', 'capacity', 'fear_level']
    )
}}

select * from {{ ref('stg_haunted_houses') }}

{% endsnapshot %}
