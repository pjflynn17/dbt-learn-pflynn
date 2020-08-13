select 
    orderid as order_id,
    status,
    amount
from
    {{ source('stripe', 'payment')}}
where
    status = 'success'
