WITH customer_cohort AS (
    SELECT 
        customer_id, 
        DATE_TRUNC('month', MIN(order_date)) as cohort_month
    FROM fact_orders
    GROUP BY 1
)
SELECT 
    c.cohort_month,
    DATE_TRUNC('month', o.order_date) as order_month,
    COUNT(DISTINCT c.customer_id) as users
FROM customer_cohort c
JOIN fact_orders o ON c.customer_id = o.customer_id
GROUP BY 1, 2
ORDER BY 1, 2;
