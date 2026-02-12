with
    source as (
        select
            price
            , includes_fast_pass
            , description
            , updated_at
            , ticket_id
            , launch_date
            , ticket_type_name
            , includes_vip_benefits
            , created_at
        from {{ source('park_assets', 'ticket_types') }}
    )

select *
from source