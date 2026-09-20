SELECT 
    p.product_id, 
    p.name, 
    SUM(o.quantity) as total_sold,
    RANK() OVER (ORDER BY SUM(o.quantity) DESC) as sales_rank
FROM dim_products p
JOIN fact_orders o ON p.product_id = o.product_id
GROUP BY p.product_id, p.name;
