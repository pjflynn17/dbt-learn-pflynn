with
order_payments as (
        select * from {{ ref('stg_payments')}}
),
customers as (
    select * from {{ ref('stg_customers')}}
),
orders as (
    select * from {{ ref('stg_orders')}}
),
customer_orders as (
    select
        order_id,
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
    from orders
    group by 1, 2
),
final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        sum(coalesce(customer_orders.number_of_orders, 0)) as number_of_orders,
        nvl(sum(order_payments.amount), 0) as lifetime_value
    from customers
    left join customer_orders using (customer_id)
    left join order_payments using (order_id)
    group by 1, 2, 3
    order by 1
)
select * from final
