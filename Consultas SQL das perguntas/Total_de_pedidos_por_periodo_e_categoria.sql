--Total de pedidos por período e categorias.
select
      year(o.order_purchase_timestamp) as order_year,
      coalesce(p.product_category_name,'sem_categoria') as product_category,
      count(distinct o.order_id) as total_orders,
      count(*) as total_itens_sold,
      count(distinct p.product_id) as total_products,
      round(sum(oi.price),2) as total_amount,
      round(avg(oi.price),2) as average_ticket
from
      db_olist.orders as o
inner join
      db_olist.order_items as oi on o.order_id = oi.order_id   
inner join
      db_olist.products as p on oi.product_id = p.product_id
group by
      1,2
order by
      order_year, product_category