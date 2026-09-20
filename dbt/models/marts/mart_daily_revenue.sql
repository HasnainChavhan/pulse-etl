SELECT
    order_date,
    SUM(revenue) as daily_revenue
FROM {{ ref('stg_orders') }}
GROUP BY 1
