SELECT 
    c.customer_id, 
    c.name, 
    SUM(o.revenue) as total_revenue,
    RANK() OVER (ORDER BY SUM(o.revenue) DESC) as rank
FROM dim_customers c
JOIN fact_orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
ORDER BY total_revenue DESC
LIMIT 10;
