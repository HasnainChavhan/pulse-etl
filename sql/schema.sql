CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id UUID PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    city VARCHAR(100),
    country VARCHAR(100),
    signup_date DATE,
    segment VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id UUID PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    price DECIMAL(10, 2),
    cost DECIMAL(10, 2),
    supplier VARCHAR(255),
    profit_margin DECIMAL(10, 4)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date DATE PRIMARY KEY,
    year INT,
    month INT,
    quarter INT,
    day_of_week INT
);

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id UUID PRIMARY KEY,
    customer_id UUID REFERENCES dim_customers(customer_id),
    product_id UUID REFERENCES dim_products(product_id),
    quantity INT,
    price DECIMAL(10, 2),
    discount DECIMAL(10, 2),
    order_date DATE,
    status VARCHAR(50),
    payment_method VARCHAR(50),
    shipping_address TEXT,
    revenue DECIMAL(10, 2)
);
