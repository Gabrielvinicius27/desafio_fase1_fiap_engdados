--Média das avaliações dos pedidos por vendedor (com mínimo 5 reviews)
select
      s.seller_id,
      upper(s.seller_city) as seller_city,
      s.seller_state,
      round(avg(r.review_score),2) as average_review_score,
      count(r.review_id) as total_reviews
from
      db_olist.orders as o
inner join
      db_olist.order_items as oi on o.order_id = oi.order_id
inner join
      db_olist.products as p on oi.product_id = p.product_id
inner join
      db_olist.sellers as s on oi.seller_id = s.seller_id
inner join
      db_olist.order_reviews as r on o.order_id = r.order_id
group by
      1,2,3
having
      count(r.review_id) > 4
order by
      4 desc, 5 desc