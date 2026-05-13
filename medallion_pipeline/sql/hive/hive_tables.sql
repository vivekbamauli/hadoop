-- =============================================================
-- Medallion Pipeline – Hive External Table Definitions
-- =============================================================
-- Run: beeline -u jdbc:hive2://localhost:10000 -f hive_tables.sql
-- =============================================================

CREATE DATABASE IF NOT EXISTS medallion
  COMMENT 'Medallion Data Lake – Bronze / Silver / Gold'
  LOCATION '/user/hive/warehouse/medallion.db';

USE medallion;

-- =============================================================
-- BRONZE LAYER (raw, partitioned by load_date)
-- =============================================================

-- customers (bronze)
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_customers (
    _id             STRING,
    customer_id     STRING,
    first_name      STRING,
    last_name       STRING,
    email           STRING,
    phone           STRING,
    address         STRING,
    city            STRING,
    country         STRING,
    status          STRING,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    raw_document    STRING    COMMENT 'Full JSON document'
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '/data-lake/bronze/customers'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- orders (bronze)
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_orders (
    _id             STRING,
    order_id        STRING,
    customer_id     STRING,
    total_amount    DOUBLE,
    currency        STRING,
    status          STRING,
    items           STRING    COMMENT 'JSON array of line items',
    shipping_addr   STRING,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    raw_document    STRING
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '/data-lake/bronze/orders'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- products (bronze)
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_products (
    _id             STRING,
    product_id      STRING,
    name            STRING,
    category        STRING,
    sub_category    STRING,
    price           DOUBLE,
    stock_qty       INT,
    attributes      STRING    COMMENT 'JSON map of product attributes',
    status          STRING,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    raw_document    STRING
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '/data-lake/bronze/products'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- payments (bronze)
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_payments (
    _id             STRING,
    payment_id      STRING,
    order_id        STRING,
    customer_id     STRING,
    amount          DOUBLE,
    currency        STRING,
    method          STRING,
    gateway         STRING,
    status          STRING,
    created_at      TIMESTAMP,
    raw_document    STRING
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '/data-lake/bronze/payments'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- reviews (bronze)
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_reviews (
    _id             STRING,
    review_id       STRING,
    product_id      STRING,
    customer_id     STRING,
    rating          TINYINT,
    title           STRING,
    body            STRING,
    status          STRING,
    created_at      TIMESTAMP,
    raw_document    STRING
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '/data-lake/bronze/reviews'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- =============================================================
-- SILVER LAYER (cleansed, SCD-managed)
-- =============================================================

-- customers (silver – SCD Type 2)
CREATE EXTERNAL TABLE IF NOT EXISTS silver_customers (
    customer_id     STRING,
    first_name      STRING,
    last_name       STRING,
    full_name       STRING,
    email           STRING,
    phone           STRING,
    address         STRING,
    city            STRING,
    country         STRING,
    status          STRING,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    -- SCD2 columns
    effective_date  TIMESTAMP,
    expiry_date     TIMESTAMP,
    is_current      BOOLEAN,
    version_number  INT,
    record_hash     STRING,
    pipeline_load_ts TIMESTAMP
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '/data-lake/silver/customers'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- orders (silver – SCD Type 1)
CREATE EXTERNAL TABLE IF NOT EXISTS silver_orders (
    order_id        STRING,
    customer_id     STRING,
    total_amount    DOUBLE,
    currency        STRING,
    status          STRING,
    items_count     INT,
    total_items_qty INT,
    shipping_city   STRING,
    shipping_country STRING,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    record_hash     STRING,
    pipeline_load_ts TIMESTAMP
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '/data-lake/silver/orders'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- products (silver – SCD Type 2)
CREATE EXTERNAL TABLE IF NOT EXISTS silver_products (
    product_id      STRING,
    name            STRING,
    category        STRING,
    sub_category    STRING,
    price           DOUBLE,
    stock_qty       INT,
    status          STRING,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    effective_date  TIMESTAMP,
    expiry_date     TIMESTAMP,
    is_current      BOOLEAN,
    version_number  INT,
    record_hash     STRING,
    pipeline_load_ts TIMESTAMP
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '/data-lake/silver/products'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- =============================================================
-- GOLD LAYER (analytics-ready aggregations)
-- =============================================================

-- customer_sales_summary (gold)
CREATE EXTERNAL TABLE IF NOT EXISTS gold_customer_sales_summary (
    customer_id         STRING,
    full_name           STRING,
    email               STRING,
    country             STRING,
    total_orders        BIGINT,
    total_revenue       DOUBLE,
    avg_order_value     DOUBLE,
    first_order_date    TIMESTAMP,
    last_order_date     TIMESTAMP,
    customer_segment    STRING,
    lifetime_value_tier STRING,
    pipeline_load_ts    TIMESTAMP
)
PARTITIONED BY (report_date STRING)
STORED AS PARQUET
LOCATION '/data-lake/gold/customer_sales_summary'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- top_products (gold)
CREATE EXTERNAL TABLE IF NOT EXISTS gold_top_products (
    product_id          STRING,
    product_name        STRING,
    category            STRING,
    sub_category        STRING,
    total_units_sold    BIGINT,
    total_revenue       DOUBLE,
    avg_rating          DOUBLE,
    review_count        BIGINT,
    return_rate         DOUBLE,
    rank_in_category    INT,
    pipeline_load_ts    TIMESTAMP
)
PARTITIONED BY (report_date STRING)
STORED AS PARQUET
LOCATION '/data-lake/gold/top_products'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- order_analytics (gold)
CREATE EXTERNAL TABLE IF NOT EXISTS gold_order_analytics (
    order_date          STRING,
    country             STRING,
    category            STRING,
    total_orders        BIGINT,
    completed_orders    BIGINT,
    cancelled_orders    BIGINT,
    total_revenue       DOUBLE,
    avg_order_value     DOUBLE,
    completion_rate     DOUBLE,
    pipeline_load_ts    TIMESTAMP
)
PARTITIONED BY (report_date STRING)
STORED AS PARQUET
LOCATION '/data-lake/gold/order_analytics'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- =============================================================
-- Repair Partitions (run after each pipeline load)
-- =============================================================
MSCK REPAIR TABLE bronze_customers;
MSCK REPAIR TABLE bronze_orders;
MSCK REPAIR TABLE bronze_products;
MSCK REPAIR TABLE bronze_payments;
MSCK REPAIR TABLE bronze_reviews;

MSCK REPAIR TABLE silver_customers;
MSCK REPAIR TABLE silver_orders;
MSCK REPAIR TABLE silver_products;

MSCK REPAIR TABLE gold_customer_sales_summary;
MSCK REPAIR TABLE gold_top_products;
MSCK REPAIR TABLE gold_order_analytics;

-- =============================================================
-- Sample Analytical Queries
-- =============================================================

-- Top 10 customers by revenue
SELECT
    customer_id, full_name, country,
    total_orders, ROUND(total_revenue, 2) AS revenue,
    lifetime_value_tier
FROM gold_customer_sales_summary
WHERE report_date = (SELECT MAX(report_date) FROM gold_customer_sales_summary)
ORDER BY total_revenue DESC
LIMIT 10;

-- Daily order trend (last 30 days)
SELECT
    order_date, SUM(total_orders) AS orders,
    ROUND(SUM(total_revenue), 2)  AS revenue,
    ROUND(AVG(avg_order_value), 2) AS aov
FROM gold_order_analytics
WHERE report_date >= DATE_SUB(CURRENT_DATE, 30)
GROUP BY order_date
ORDER BY order_date;
