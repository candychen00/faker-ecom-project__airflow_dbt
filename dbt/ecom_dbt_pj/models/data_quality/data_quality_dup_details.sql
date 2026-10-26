SELECT
    format_timestamp('%Y-%m-%d %H:%M', current_timestamp(), 'Asia/Taipei') as run_time,
    'ecom_raw_orders' as table_name,
    CAST(order_id AS STRING) as id,
    COUNT(*) AS record_count
FROM {{ source('ecom_raw', 'orders') }}
GROUP BY order_id
HAVING COUNT(*) > 1


UNION ALL


SELECT
    format_timestamp('%Y-%m-%d %H:%M', current_timestamp(), 'Asia/Taipei') as run_time,
    'ecom_raw_customers' as table_name,
    CAST(customer_id AS STRING) as id,
    COUNT(*) AS record_count
FROM {{ source('ecom_raw', 'customers') }}
GROUP BY customer_id
HAVING COUNT(*) > 1


UNION ALL


SELECT
    format_timestamp('%Y-%m-%d %H:%M', current_timestamp(), 'Asia/Taipei') as run_time,
    'ecom_raw_products' as table_name,
    CAST(product_id AS STRING) as id,
    COUNT(*) AS record_count
FROM {{ source('ecom_raw', 'products') }}
GROUP BY product_id
HAVING COUNT(*) > 1