{% macro monthly_agg(group_col) %}

WITH
monthly_by_prod as (
    select * 
    from {{ ref("mart_monthly_by_prod") }}
)

SELECT
    year_month,
    {{ group_col }},
    SUM( ord_quantity ) as ord_quantity,
    ROUND( SUM( order_amt ), 2) as order_amt

FROM monthly_by_prod
GROUP BY year_month, {{ group_col }}

ORDER BY year_month desc, {{ group_col }}

{% endmacro %}