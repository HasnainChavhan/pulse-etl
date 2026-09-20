WITH MonthlyRev AS (
    SELECT 
        DATE_TRUNC('month', order_date) as month,
        SUM(revenue) as revenue
    FROM fact_orders
    GROUP BY 1
)
SELECT * FROM MonthlyRev ORDER BY month;
