-- 示例SQL查询文件，包含各种SQL语句类型

-- 简单SELECT查询
SELECT * FROM users;

-- 带WHERE条件的查询
SELECT id, name, email FROM customers WHERE status = 'active';

-- JOIN查询
SELECT u.id, u.name, o.order_id, o.total
FROM users u
INNER JOIN orders o ON u.id = o.user_id;

-- 多表JOIN
SELECT 
    c.customer_name,
    o.order_date,
    p.product_name,
    od.quantity
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
INNER JOIN order_details od ON o.order_id = od.order_id
RIGHT JOIN products p ON od.product_id = p.product_id;

-- 子查询
SELECT * FROM employees 
WHERE department_id IN (SELECT id FROM departments WHERE location = 'Beijing');

-- INSERT语句
INSERT INTO users (name, email, created_at) 
VALUES ('张三', 'zhangsan@example.com', NOW());

-- UPDATE语句
UPDATE products SET price = price * 1.1 WHERE category = 'electronics';

-- DELETE语句
DELETE FROM temp_data WHERE created_at < '2023-01-01';

-- CREATE TABLE
CREATE TABLE IF NOT EXISTS user_logs (
    id INT PRIMARY KEY,
    user_id INT,
    action VARCHAR(100),
    created_at TIMESTAMP
);

-- WITH CTE (Common Table Expression)
WITH sales_summary AS (
    SELECT 
        product_id,
        SUM(quantity) as total_quantity
    FROM sales_records
    GROUP BY product_id
)
SELECT p.product_name, ss.total_quantity
FROM products p
JOIN sales_summary ss ON p.id = ss.product_id;

-- 复杂嵌套查询
SELECT t1.id, t1.name
FROM (
    SELECT id, name FROM employees WHERE salary > 50000
) t1
WHERE t1.id IN (
    SELECT employee_id FROM performance_reviews WHERE rating > 4
);

-- UNION查询
SELECT customer_id, name FROM customers_2023
UNION
SELECT customer_id, name FROM customers_2024;

-- 带schema的表名
SELECT * FROM db_schema.table_name;
SELECT * FROM `database_1`.`users_1`;
select * from "database_2"."users_2";

-- 多个INSERT
INSERT INTO logs (message) SELECT error_message FROM error_table;

-- TRUNCATE
TRUNCATE TABLE temporary_cache;

-- DROP TABLE
DROP TABLE IF EXISTS old_backup_table;

-- ALTER TABLE
ALTER TABLE users ADD COLUMN phone VARCHAR(20);

-- FROM子句中的子查询
SELECT * FROM (SELECT * FROM raw_data WHERE valid = 1) AS filtered_data;

-- ============================================================
-- Hive SQL 语法示例
-- ============================================================

-- Hive: INSERT OVERWRITE TABLE
INSERT OVERWRITE TABLE ods_user PARTITION(dt='2024-01-01')
SELECT * FROM src_user WHERE dt='2024-01-01';

-- Hive: INSERT INTO TABLE
INSERT INTO TABLE dwd_order
SELECT order_id, user_id, amount FROM ods_order;

-- Hive: CREATE EXTERNAL TABLE
CREATE EXTERNAL TABLE IF NOT EXISTS ext_log_data (
    log_id STRING,
    log_time TIMESTAMP,
    message STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/data/logs/';

-- Hive: LOAD DATA
LOAD DATA INPATH '/tmp/data.csv' INTO TABLE staging_table;
LOAD DATA LOCAL INPATH '/home/user/data.txt' OVERWRITE INTO TABLE local_import_table;

-- Hive: MSCK REPAIR TABLE
MSCK REPAIR TABLE partitioned_events;

-- Hive: ANALYZE TABLE
ANALYZE TABLE fact_sales COMPUTE STATISTICS;

-- Hive: DESCRIBE
DESCRIBE FORMATTED dim_product;
DESC EXTENDED dim_customer;

-- Hive: SHOW commands
SHOW PARTITIONS event_logs;
SHOW CREATE TABLE config_table;
SHOW COLUMNS FROM metadata_table;

-- Hive: EXPORT/IMPORT TABLE
EXPORT TABLE backup_source TO '/backup/2024/';
IMPORT TABLE restored_table FROM '/backup/2024/';

-- Hive: 复杂查询示例 (WITH + INSERT OVERWRITE)
WITH daily_agg AS (
    SELECT user_id, COUNT(*) as cnt
    FROM user_actions
    GROUP BY user_id
)
INSERT OVERWRITE TABLE user_daily_stats PARTITION(dt='2024-01-01')
SELECT user_id, cnt FROM daily_agg;
