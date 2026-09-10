-- Create database if not exists
CREATE DATABASE IF NOT EXISTS ecommerce_dw;

USE ecommerce_dw;

-- External table mapped to HDFS Parquet location
CREATE EXTERNAL TABLE IF NOT EXISTS streaming_orders (
    order_id       INT,
    customer_id    INT,
    product_id     INT,
    quantity       INT,
    price          DOUBLE,
    total_amount   DOUBLE,
    order_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/ecommerce_dw.db/streaming_orders';

-- =========================================
-- Analytical OLAP Queries
-- =========================================

-- 1. Total number of processed orders
SELECT COUNT(*) AS total_orders
FROM streaming_orders;

-- 2. Total gross revenue
SELECT SUM(total_amount) AS total_revenue
FROM streaming_orders;

-- 3. Average order value (AOV)
SELECT AVG(total_amount) AS average_order_value
FROM streaming_orders;

-- 4. Total sales broken down by product_id
SELECT product_id, SUM(total_amount) AS product_revenue
FROM streaming_orders
GROUP BY product_id
ORDER BY product_revenue DESC;

-- 5. Total expenditure per customer_id
SELECT customer_id, SUM(total_amount) AS customer_spend
FROM streaming_orders
GROUP BY customer_id
ORDER BY customer_spend DESC;

-- 6. Top 5 selling products ranked by gross revenue
SELECT product_id, SUM(total_amount) AS revenue
FROM streaming_orders
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 5;
