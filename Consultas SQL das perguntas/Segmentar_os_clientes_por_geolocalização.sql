-- Clientes por estado
SELECT  "db_olist"."customers"."customer_state" AS "customer_state", 
        count("db_olist"."customers"."customer_unique_id") AS "count_customer_unique_id"
FROM "db_olist"."customers"
GROUP BY "db_olist"."customers"."customer_state";

