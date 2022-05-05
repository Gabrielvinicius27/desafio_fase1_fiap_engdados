--Vendedores X Vendas
select
      s.seller_id,
      upper(s.seller_city) as seller_city,
      s.seller_state,
      round(sum(oi.price),2) as total_amount,
      count(*) as total_itens_sold,
      round(avg(oi.price),2) as average_price,
      round(min(oi.price),2) as min_price,
      round(max(oi.price),2) as max_price
from
      db_olist.orders as o
inner join
      db_olist.order_items as oi on o.order_id = oi.order_id
inner join
      db_olist.products as p on oi.product_id = p.product_id
inner join
      db_olist.sellers as s on oi.seller_id = s.seller_id
group by
      1,2,3
order by
      4 desc
limit 100