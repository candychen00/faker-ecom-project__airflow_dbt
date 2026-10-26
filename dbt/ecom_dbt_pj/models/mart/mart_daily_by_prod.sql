with

fct_ord as (
    select
        order_at,
        product_id,
        ord_quantity,
        order_amt
    from {{ ref("fct_orders") }}
),

dim_prod as (
    select * 
    from {{ ref("dim_products") }}
)

SELECT
    DATE(ord.order_at) as order_date,
    prod.product,
    prod.prod_category,
    prod.prod_fruit,
    SUM(ord.ord_quantity) as ord_quantity,
    ROUND( SUM(ord.order_amt), 2) as order_amt
    

FROM fct_ord ord
LEFT JOIN dim_prod prod ON ord.product_id = prod.product_id
GROUP BY order_date, prod.product, prod.prod_category, prod.prod_fruit