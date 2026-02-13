{{ config(static_analysis='off') }}

with
    date_spine as (
        {{ dbt_utils.date_spine(
                datepart="day",
                start_date="cast('2020-01-01' as date)",
                end_date="cast('2026-12-31' as date)"
        ) }}
    )

    , enriched_date_spine as (
        select
            *
            , year(date_day) as year_number
            , month(date_day) as month_number
            , day(date_day) as day_number
            , case
                when day(date_day) in (6, 7) then true
                else false
            end as is_weekend
            , case
                when
                    day(date_day) = 31
                    and month(date_day) = 10
                then true
                else false
            end as is_halloween
            , datediff('day', date(year_number || '-10-31'), date_day) as days_to_halloween
            , case
                when month(date_day) = 10 and day(date_day) = 31 then 'Halloween'
                when month(date_day) = 10 then 'October (Halloween Season)'
                when month(date_day) in (11, 12) then 'Holiday Season'
                when month(date_day) in (6, 7, 8) then 'Summer Season'
                when month(date_day) in (3, 4, 5) then 'Spring Season'
                else 'Off Season'
            end as business_season
        from date_spine
    )

select *
from enriched_date_spine