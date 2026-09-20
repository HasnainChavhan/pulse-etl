SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    price,
    discount,
    order_date,
    status,
    revenue
FROM {{ source('raw', 'fact_orders') }}
