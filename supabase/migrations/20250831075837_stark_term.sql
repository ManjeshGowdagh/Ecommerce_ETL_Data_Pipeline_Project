/*
  # Data Population for E-commerce Star Schema

  1. Data Loading Strategy
    - Populate dimension tables first to ensure referential integrity
    - Load fact tables with proper foreign key relationships
    - Handle incremental updates for ongoing data loads

  2. Data Transformations
    - Generate surrogate keys where needed
    - Apply business rules and calculations
    - Ensure data consistency across tables

  3. Performance Optimizations
    - Use appropriate partitioning for large fact tables
    - Implement proper indexing strategy
    - Optimize join operations for analytics queries
*/

-- Set database context
USE ecommerce_dw;

-- Populate Date Dimension (static reference data)
INSERT OVERWRITE TABLE dim_date
SELECT DISTINCT
    CAST(date_format(order_date, 'yyyyMMdd') AS INT) AS date_id,
    order_date AS full_date,
    day(order_date) AS day_of_month,
    date_format(order_date, 'EEEE') AS day_name,
    dayofweek(order_date) AS day_of_week,
    weekofyear(order_date) AS week_of_year,
    month(order_date) AS month_number,
    date_format(order_date, 'MMMM') AS month_name,
    quarter(order_date) AS quarter,
    year(order_date) AS year,
    CASE WHEN dayofweek(order_date) IN (1, 7) THEN true ELSE false END AS is_weekend,
    false AS is_holiday,  -- Can be enhanced with holiday calendar
    CASE 
        WHEN month(order_date) >= 4 THEN year(order_date)
        ELSE year(order_date) - 1
    END AS fiscal_year,
    CASE 
        WHEN month(order_date) BETWEEN 4 AND 6 THEN 1
        WHEN month(order_date) BETWEEN 7 AND 9 THEN 2
        WHEN month(order_date) BETWEEN 10 AND 12 THEN 3
        ELSE 4
    END AS fiscal_quarter
FROM (
    SELECT DISTINCT order_date
    FROM default.orders_clean
    WHERE order_date IS NOT NULL
) dates;

-- Populate Customer Dimension
INSERT OVERWRITE TABLE dim_customer
SELECT 
    customer_id,
    customer_name,
    email,
    phone,
    address,
    city,
    state,
    zip_code,
    age,
    age_group,
    registration_date,
    CASE 
        WHEN age BETWEEN 18 AND 25 THEN 'Young Adults'
        WHEN age BETWEEN 26 AND 35 THEN 'Millennials'
        WHEN age BETWEEN 36 AND 50 THEN 'Gen X'
        WHEN age > 50 THEN 'Baby Boomers'
        ELSE 'Unknown'
    END AS customer_segment,
    0.0 AS lifetime_value,  -- Will be updated with actual calculations
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
FROM default.customers_clean;

-- Calculate and update customer lifetime value
UPDATE dim_customer SET lifetime_value = (
    SELECT COALESCE(SUM(total_amount), 0)
    FROM default.orders_clean o
    WHERE o.customer_id = dim_customer.customer_id
);

-- Populate Product Dimension
INSERT OVERWRITE TABLE dim_product
SELECT 
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    price,
    cost,
    profit_margin,
    price_category,
    description,
    true AS is_active,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
FROM default.products_clean;

-- Populate Sales Fact Table
INSERT OVERWRITE TABLE fact_sales
PARTITION (year, month)
SELECT 
    monotonically_increasing_id() AS sales_id,
    o.order_id,
    o.customer_id,
    o.product_id,
    CAST(date_format(o.order_date, 'yyyyMMdd') AS INT) AS date_id,
    o.quantity,
    o.unit_price,
    o.total_amount,
    COALESCE(o.total_amount * 0.1, 0) AS discount_amount,  -- Assume 10% discount if applicable
    o.total_amount - COALESCE(o.total_amount * 0.1, 0) AS net_amount,
    o.payment_type,
    COALESCE(p.payment_status, 'Unknown') AS payment_status,
    'Completed' AS order_status,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at,
    year(o.order_date) AS year,
    month(o.order_date) AS month
FROM default.orders_clean o
LEFT JOIN default.payments_clean p ON o.order_id = p.order_id
WHERE o.order_date IS NOT NULL
  AND o.customer_id IS NOT NULL
  AND o.product_id IS NOT NULL;

-- Populate Payment Fact Table
INSERT OVERWRITE TABLE fact_payments
PARTITION (year, month)
SELECT 
    p.payment_id,
    p.order_id,
    o.customer_id,
    CAST(date_format(p.payment_date, 'yyyyMMdd') AS INT) AS date_id,
    p.payment_method,
    p.payment_amount,
    p.payment_date,
    p.payment_status,
    CASE 
        WHEN p.payment_method = 'Credit Card' THEN p.payment_amount * 0.029
        WHEN p.payment_method = 'PayPal' THEN p.payment_amount * 0.034
        ELSE 0
    END AS processing_fee,
    current_timestamp() AS created_at,
    year(p.payment_date) AS year,
    month(p.payment_date) AS month
FROM default.payments_clean p
JOIN default.orders_clean o ON p.order_id = o.order_id
WHERE p.payment_date IS NOT NULL;

-- Create materialized views for common analytics queries
CREATE VIEW IF NOT EXISTS vw_monthly_sales AS
SELECT 
    d.year,
    d.month_name,
    d.quarter,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS total_quantity,
    SUM(f.total_amount) AS total_revenue,
    SUM(f.net_amount) AS net_revenue,
    AVG(f.total_amount) AS avg_order_value
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month_number, d.month_name, d.quarter
ORDER BY d.year, d.month_number;

CREATE VIEW IF NOT EXISTS vw_customer_metrics AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.age_group,
    c.customer_segment,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.total_amount) AS total_spent,
    AVG(f.total_amount) AS avg_order_value,
    MAX(d.full_date) AS last_order_date,
    MIN(d.full_date) AS first_order_date
FROM dim_customer c
JOIN fact_sales f ON c.customer_id = f.customer_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY c.customer_id, c.customer_name, c.age_group, c.customer_segment;

CREATE VIEW IF NOT EXISTS vw_product_performance AS
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    COUNT(DISTINCT f.order_id) AS orders_count,
    SUM(f.quantity) AS total_quantity_sold,
    SUM(f.total_amount) AS total_revenue,
    AVG(f.unit_price) AS avg_selling_price,
    p.cost,
    (AVG(f.unit_price) - p.cost) AS avg_profit_per_unit
FROM dim_product p
JOIN fact_sales f ON p.product_id = f.product_id
GROUP BY p.product_id, p.product_name, p.category, p.brand, p.cost
ORDER BY total_revenue DESC;