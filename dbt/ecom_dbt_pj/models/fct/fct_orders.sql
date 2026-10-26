
WITH

raw_ord as (
    select * from {{ ref("src_orders") }}
),

dim_prod as (
    select * from {{ ref("dim_products") }}
)

SELECT
    row_number() over () as order_unique_id,
    ord.order_id,
    ord.order_at,
    ord.customer_id,
    ord.product_id,
    ord.quantity as ord_quantity,
    ( ord.quantity * prod.unit_price ) as order_amt

FROM raw_ord ord
LEFT JOIN dim_prod prod ON ord.product_id = prod.product_id

/*
{% if is_incremental%}


{% endif %}
*/