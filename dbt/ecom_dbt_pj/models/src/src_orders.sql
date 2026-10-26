
WITH
dedup as (
    select distinct *
    from {{ source('ecom_raw', 'orders') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['order_id', 'order_at', 'customer_id', 'product_id', 'quantity']) }} as order_sk,
    *
FROM dedup