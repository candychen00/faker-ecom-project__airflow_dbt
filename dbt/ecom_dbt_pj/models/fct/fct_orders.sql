
{{
    config(
        materialized = 'incremental',
        unique_key = 'order_id',
        incremental_strategy = 'merge',
        on_schema_change = "fail",

        tags= ["ord"]
    )
}}

WITH

ord as (
    select * from {{ ref("int_orders") }}
),

prod as (
    select * from {{ ref("dim_products") }}
)

SELECT
    ord.order_sk,
    ord.order_id,
    cast( ord.order_at as datetime ) as order_at,
    ord.customer_id,
    ord.product_id,
    ord.quantity as ord_quantity,
    ( ord.quantity * prod.unit_price ) as order_amt,
    ord.updated_at,
    ord.created_at


FROM ord
LEFT JOIN prod ON ord.product_id = prod.product_id


{% if is_incremental() %}

    WHERE CAST(ord.updated_at AS DATETIME) >= TIMESTAMP_SUB(
        (SELECT MAX(updated_at) FROM {{ this }}),
        INTERVAL 3 HOUR
    )

{% endif %}

