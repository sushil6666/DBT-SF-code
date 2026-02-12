with
    source as (
        select
            to_boolean(is_vip_member) as is_vip_member
            , case
                when gender = 'Male' then 'M'
                when gender = 'Female' then 'F'
                else 'Other'
            end as gender
            , email
            , phone
            , preferred_scare_level
            , updated_at
            , zip_code
            , age
            , last_name
            , address
            , first_name
            , created_at
            , customer_id
            , to_boolean(marketing_opt_in) as marketing_opt_in
            , state
            , registration_date
            , loyalty_points
            , city
        from {{ ref('snp_customers') }}
        where dbt_valid_to is null
    )

select *
from source