-- models/marts/fct_orders.sql
-- models/marts/fct_orders.sql
with lineitem as (
    select
        order_id,
        sum(extended_price * (1 - discount))                   as revenue_net,
        sum(extended_price * (1 - discount) * (1 + tax))       as revenue_with_tax,
        sum(quantity)                                          as total_qty,
        count(*)                                               as line_count
    from {{ ref('stg_lineitem') }}
    group by 1
),

orders as (
    select
        order_id,
        customer_id,
        order_status,
        order_total,
        order_date,
        order_priority,
        clerk,
        ship_priority,
        comment
    from {{ ref('stg_orders') }}
)

select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_total,
    o.order_date,
    o.order_priority,
    l.revenue_net,
    l.revenue_with_tax,
    l.total_qty,
    l.line_count
from orders o
left join lineitem l
    on o.order_id = l.order_id