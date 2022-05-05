--Top 5 Categorias de produtos mais vendidos
select
      coalesce(p.product_category_name,'sem_categoria') as product_category,
      count(*) as total_itens_sold,
      round(sum(oi.price),2) as total_amount,
      round(avg(oi.price),2) as average_price,
      round(min(oi.price),2) as min_price,
      round(max(oi.price),2) as max_price,
      round(max(oi.price) - min(oi.price),2) as price_variation
from
      db_olist.orders as o
inner join
      db_olist.order_items as oi on o.order_id = oi.order_id
inner join
      db_olist.products as p on oi.product_id = p.product_id
group by
      1
order by
      2 desc
LIMIT 5