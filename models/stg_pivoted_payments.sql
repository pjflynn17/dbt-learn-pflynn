{% set payment_method = ['credit_card', 'bank_transfer', 'coupon', 'gift_card'] %}
select
    order_id,
    {% for payment in payment_method %}
        sum(case when payment_method = '{{ payment }}' then amount else 0 end) as {{ payment }}_amount
        {% if not loop.last %},
        {% endif %}
    {% endfor %}
from {{ ref('stg_payments') }}
group by 1