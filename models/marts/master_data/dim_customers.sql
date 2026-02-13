with
    customers as (
        select
            is_vip_member
            , gender
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
            , marketing_opt_in
            , state
            , registration_date
            , loyalty_points
            , city
        from {{ ref('stg_customer_data__customers') }}
    )

    , us_states as (
        select
            state_code
            , state_name
        from {{ ref('seed_us_states') }}
    )

    , valid_tlds as (
        select
            domain
        from {{ ref('seed_valid_top_level_domains') }}
    )

    , enriched_customers as (
        select
            customers.*
            , us_states.state_name
        from customers
        left join us_states on customers.state = us_states.state_code
    )

    , customer_segments as (
        select
            -- Original fields
            customer_id
            , first_name
            , last_name
            , email
            , phone
            , address
            , city
            , state
            , state_name
            , zip_code
            , age
            , gender
            , is_vip_member
            , marketing_opt_in
            , preferred_scare_level
            , loyalty_points
            , registration_date
            , created_at
            , updated_at

            -- Email validation
            , case
                when email is null or email = '' then false
                when regexp_like(
                    email,
                    '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
                )
                and exists (
                    select 1
                    from valid_tlds
                    where lower(split_part(email, '@', -1)) = lower(domain)
                ) then true
                else false
            end as is_valid_email

            -- Age-based segmentation
            , case
                when age < 18 then 'Teenager'
                when age between 18 and 25 then 'Young Adult'
                when age between 26 and 35 then 'Adult'
                when age between 36 and 50 then 'Middle Age'
                when age between 51 and 65 then 'Senior'
                when age > 65 then 'Elderly'
                else 'Unknown'
            end as age_group

            -- Loyalty tier segmentation
            , case
                when loyalty_points >= 1000 then 'Platinum'
                when loyalty_points >= 500 then 'Gold'
                when loyalty_points >= 200 then 'Silver'
                when loyalty_points >= 50 then 'Bronze'
                else 'New Member'
            end as loyalty_tier

            -- Customer value segmentation
            , case
                when is_vip_member = true and loyalty_points >= 500 then 'High Value'
                when is_vip_member = true or loyalty_points >= 200 then 'Medium Value'
                when loyalty_points >= 50 then 'Low Value'
                else 'New Customer'
            end as customer_value_segment

            , case
                when datediff('day', registration_date, current_date) <= 30 then 'New Customer'
                when datediff('day', registration_date, current_date) <= 90 then 'Recent Customer'
                when datediff('day', registration_date, current_date) <= 365 then 'Established Customer'
                when datediff('day', registration_date, current_date) <= 1095 then 'Long-term Customer'
                else 'Loyal Customer'
            end as customer_lifecycle_stage

            -- Risk assessment for retention
            , case
                when datediff('day', registration_date, current_date) > 365 and loyalty_points < 50 then 'At Risk'
                when datediff('day', registration_date, current_date) > 180 and loyalty_points < 100 then 'Potential Risk'
                when loyalty_points >= 200 then 'Low Risk'
                else 'Monitor'
            end as retention_risk_level

            -- Upsell opportunity
            , case
                when is_vip_member = false and loyalty_points >= 200 then 'VIP Upsell Candidate'
                when loyalty_points between 100 and 199 then 'Loyalty Program Promoter'
                when marketing_opt_in = false and loyalty_points >= 50 then 'Marketing Opt-in Candidate'
                when preferred_scare_level in (1, 2) and age between 18 and 35 then 'Scare Level Upsell'
                else 'Standard'
            end as upsell_opportunity

        from enriched_customers
    )

select *
from customer_segments