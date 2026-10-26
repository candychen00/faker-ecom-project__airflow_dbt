
WITH summary as (
        SELECT
            format_timestamp('%Y-%m-%d %H:%M', current_timestamp(), 'Asia/Taipei') as run_time,
            'ecom_raw_orders' AS table_name,
            SUM(record_count/2) AS dup_count
        FROM
        (
            SELECT
                order_id,
                COUNT(*) AS record_count
            FROM {{ source('ecom_raw', 'orders') }}
            GROUP BY order_id
            HAVING COUNT(*) > 1
        )


        UNION ALL


        SELECT
            format_timestamp('%Y-%m-%d %H:%M', current_timestamp(), 'Asia/Taipei') as run_time,
            'fct_orders' AS table_name,
            SUM(record_count/2) AS dup_count
        FROM
        (
            SELECT
                order_id,
                COUNT(*) AS record_count
            FROM {{ ref('fct_orders') }}
            GROUP BY order_id
            HAVING COUNT(*) > 1
        )


        UNION ALL


        SELECT
            format_timestamp('%Y-%m-%d %H:%M', current_timestamp(), 'Asia/Taipei') as run_time,
            'ecom_raw_products' AS table_name,
            SUM(record_count/2) AS dup_count
        FROM
        (
            SELECT
                product_id,
                COUNT(*) AS record_count
            FROM {{ source('ecom_raw', 'products') }}
            GROUP BY product_id
            HAVING COUNT(*) > 1
        )


        UNION ALL


        SELECT
            format_timestamp('%Y-%m-%d %H:%M', current_timestamp(), 'Asia/Taipei') as run_time,
            'dim_products' AS table_name,
            SUM(record_count/2) AS dup_count
        FROM
        (
            SELECT
                product_id,
                COUNT(*) AS record_count
            FROM {{ ref("dim_products") }}
            GROUP BY product_id
            HAVING COUNT(*) > 1
        )


        UNION ALL


        SELECT
            format_timestamp('%Y-%m-%d %H:%M', current_timestamp(), 'Asia/Taipei') as run_time,
            'ecom_raw_customers' AS table_name,
            SUM(record_count/2) AS dup_count
        FROM
        (
            SELECT
                customer_id,
                COUNT(*) AS record_count
            FROM {{ source('ecom_raw', 'customers') }}
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        )


        UNION ALL


        SELECT
            format_timestamp('%Y-%m-%d %H:%M', current_timestamp(), 'Asia/Taipei') as run_time,
            'dim_customers' AS table_name,
            SUM(record_count/2) AS dup_count
        FROM
        (
            SELECT
                customer_id,
                COUNT(*) AS record_count
            FROM {{ ref("dim_customers") }}
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        )
)

SELECT
    run_time,
    table_name,
    CASE WHEN dup_count IS NULL THEN 0 ELSE dup_count END as dup_count
FROM summary
ORDER BY table_name
