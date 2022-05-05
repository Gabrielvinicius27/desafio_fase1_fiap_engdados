--Total de pagamentos por método de pagamento.
select
      upper(op.payment_type) as payment_type,
      count(*) as total_payments,
      count(distinct op.order_id) as total_orders,
      round(count(*) / count(distinct op.order_id) ,1) as payments_X_orders
from
      db_olist.order_payments as op
where
      op.payment_type not like 'not_defined' and
      op.payment_value > 0
group by
      op.payment_type
order by
      total_payments desc