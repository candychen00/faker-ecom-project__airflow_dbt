{% macro daily_agg(group_col) %}

WITH
daily_by_prod AS (
    select * 
    from {{ ref("mart_daily_by_prod") }}
)

SELECT
    order_date,
    {{ group_col }},
    SUM(ord_quantity) as ord_quantity,
    ROUND(SUM(order_amt), 2) as order_amt

FROM daily_by_prod
GROUP BY order_date, {{ group_col }}

ORDER BY order_date desc, {{ group_col }}

{% endmacro %}