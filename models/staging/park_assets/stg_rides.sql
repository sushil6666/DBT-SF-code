{{ config(materialized='view') }}

with source as (
    select * from {{ source('external_haunted', 'haunted_houses') }}
),

renamed as (
    select
        ride_id::int                                as ride_id,
        ride_name,
        ride_type,
        nullif(min_height_cm::int, 0)               as min_height_cm,
        thrill_level,
        capacity_per_hour::int                      as capacity_per_hour,
        zone,
        is_haunted::boolean                         as is_haunted,
        opened_date::date                           as opened_date,
        status,
        avg_wait_minutes::int                       as avg_wait_minutes
    from source
)

select * from renamed
