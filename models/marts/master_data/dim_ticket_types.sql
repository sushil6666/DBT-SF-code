with
    ticket_types as (
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
        from {{ ref('stg_park_assets__ticket_types') }}
    )

select *
from ticket_types