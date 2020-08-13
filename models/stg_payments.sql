select 
    orderid as order_id,
    status,
    paymentmethod as payment_method,
    amount
from
    {{ source('stripe', 'payment')}}
where
    status = 'success'
