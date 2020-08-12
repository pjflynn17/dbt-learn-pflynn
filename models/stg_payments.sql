select 
    orderid as order_id,
    status,
    amount
from
    raw.stripe.payment
where
    status = 'success'
