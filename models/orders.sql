with
    orders as (
        select * from {{ ref('stg_orders')}}
    ),
    order_payments as (
        select * from {{ ref('stg_payments')}}
    ),
    customers as (
        select * from {{ ref('stg_customers')}}
    )
select
    o.order_id,
    c.customer_id,
    sum(p.amount / 100) as amount
from
    orders o
    left join order_payments p on o.order_id = p.order_id
    left join customers c on o.order_id = c.customer_id
group by
    o.order_id,
    c.customer_id