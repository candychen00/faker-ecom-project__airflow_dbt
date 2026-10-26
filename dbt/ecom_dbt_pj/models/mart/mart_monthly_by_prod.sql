WITH

fct_ord as (
    select * 
    from {{ ref("fct_orders") }}
), 

dim_prod as (
    select * 
    from {{ ref("dim_products") }}
)

SELECT
    FORMAT_DATETIME('%Y-%m', ord.order_at) AS year_month,
    prod.product,
    prod.prod_category,
    prod.prod_fruit,
    SUM(ord.ord_quantity) as ord_quantity,
    ROUND( SUM(ord.order_amt), 2) as order_amt
    

FROM fct_ord ord
LEFT JOIN dim_prod prod ON ord.product_id = prod.product_id
GROUP BY year_month, prod.product, prod.prod_category, prod.prod_fruit

ORDER BY year_month desc, prod.product, prod.prod_category, prod.prod_fruit
