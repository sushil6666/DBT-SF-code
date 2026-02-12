with
    online_sales as (
        select
            purchase_date
            , sale_id
            , payment_method
            , updated_at
            , visit_hour
            , ticket_id
            , purchase_timestamp
            , purchase_channel
            , ticket_price
            , discount_percent
            , customer_id
            , created_at
            , visit_date
            , true as is_online
        from {{ ref('stg_sales__ticket_sales_online') }}
    )

    , physical_sales as (
        select
            purchase_date
            , sale_id
            , payment_method
            , updated_at
            , visit_hour
            , ticket_id
            , purchase_timestamp
            , purchase_channel
            , ticket_price
            , discount_percent
            , customer_id
            , created_at
            , visit_date
            , false as is_online
        from {{ ref('stg_sales__ticket_sales_physical') }}
    )

    , unioned_sales as (
        select * from online_sales
        union all
        select * from physical_sales
    )

    , enriched_sales as (
        select
            -- Original fields
            sale_id
            , customer_id
            , ticket_id
            , purchase_date
            , visit_date
            , purchase_timestamp
            , ticket_price
            , discount_percent
            , visit_hour
            , case
                when payment_method = 'mobile_pay' then 'other'
                else payment_method
            end as payment_method
            , purchase_channel
            , created_at
            , updated_at
            , is_online

            , case
                when discount_percent >= 50 then 'High Discount'
                when discount_percent >= 25 then 'Medium Discount'
                when discount_percent > 0 then 'Low Discount'
                else 'No Discount'
            end as discount_category

            -- Business logic flags
            , case
                when visit_date = purchase_date then true
                else false
            end as same_day_visit

            , case
                when visit_date > purchase_date then true
                else false
            end as advance_purchase

            , case
                when visit_hour >= 18 then 'Evening'
                when visit_hour >= 12 then 'Afternoon'
                when visit_hour >= 6 then 'Morning'
                else 'Night'
            end as visit_time_category

        from unioned_sales
    )

select *
from enriched_sales