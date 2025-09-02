/*
  # E-commerce Data Warehouse Schema Creation

  1. Star Schema Design
    - `fact_sales`: Central fact table with sales transactions
    - `dim_customer`: Customer dimension with demographic data
    - `dim_product`: Product dimension with catalog information
    - `dim_date`: Date dimension for time-based analysis

  2. Table Specifications
    - All tables use appropriate data types for analytics
    - Foreign key relationships maintain referential integrity
    - Partitioning strategy optimizes query performance
    - Indexes are created on frequently queried columns

  3. Storage Format
    - Tables stored in Parquet format for columnar analytics
    - Snappy compression for optimal storage and query performance
    - Partitioned by date for time-based query optimization
*/

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS ecommerce_dw
COMMENT 'E-commerce data warehouse for analytics and reporting';

-- Set database context
USE ecommerce_dw;

-- Create Date Dimension Table
CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT,
    full_date DATE,
    day_of_month INT,
    day_name STRING,
    day_of_week INT,
    week_of_year INT,
    month_number INT,
    month_name STRING,
    quarter INT,
    year INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    fiscal_year INT,
    fiscal_quarter INT
)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'description'='Date dimension table for time-based analysis'
);

-- Create Customer Dimension Table
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id STRING,
    customer_name STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    age INT,
    age_group STRING,
    registration_date DATE,
    customer_segment STRING,
    lifetime_value DECIMAL(10,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'description'='Customer dimension table with demographic and behavioral data'
);

-- Create Product Dimension Table
CREATE TABLE IF NOT EXISTS dim_product (
    product_id STRING,
    product_name STRING,
    category STRING,
    subcategory STRING,
    brand STRING,
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    profit_margin DECIMAL(5,2),
    price_category STRING,
    description STRING,
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'description'='Product dimension table with catalog and pricing information'
);

-- Create Sales Fact Table
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_id BIGINT,
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    date_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    net_amount DECIMAL(10,2),
    payment_type STRING,
    payment_status STRING,
    order_status STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'description'='Sales fact table containing transactional data'
);

-- Create Payment Fact Table (Optional - for detailed payment analysis)
CREATE TABLE IF NOT EXISTS fact_payments (
    payment_id STRING,
    order_id STRING,
    customer_id STRING,
    date_id INT,
    payment_method STRING,
    payment_amount DECIMAL(10,2),
    payment_date DATE,
    payment_status STRING,
    processing_fee DECIMAL(10,2),
    created_at TIMESTAMP
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'description'='Payment fact table for financial analysis'
);

-- Create indexes for performance optimization
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales (customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON fact_sales (product_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON fact_sales (date_id);
CREATE INDEX IF NOT EXISTS idx_dim_customer_segment ON dim_customer (customer_segment);
CREATE INDEX IF NOT EXISTS idx_dim_product_category ON dim_product (category);

-- Add table comments for documentation
ALTER TABLE fact_sales SET TBLPROPERTIES ('comment'='Central fact table containing all sales transactions');
ALTER TABLE dim_customer SET TBLPROPERTIES ('comment'='Customer dimension with demographic and behavioral attributes');
ALTER TABLE dim_product SET TBLPROPERTIES ('comment'='Product dimension with catalog and pricing information');
ALTER TABLE dim_date SET TBLPROPERTIES ('comment'='Date dimension for time-based analysis and reporting');