-- ============================================================
-- 示例SQL文件 - 用于测试SQL提取
-- 包含Hive SQL, Spark SQL, Impala SQL的各种语法
-- ============================================================

-- ============================================================
-- 1. 基础查询
-- ============================================================

-- 简单SELECT
SELECT user_id, user_name, age
FROM db_user.users
WHERE status = 1
LIMIT 100;

-- 带聚合的查询
SELECT 
    category,
    COUNT(*) as product_count,
    AVG(price) as avg_price,
    MAX(price) as max_price,
    MIN(price) as min_price
FROM db_product.products
WHERE status = 1
GROUP BY category
HAVING COUNT(*) > 10
ORDER BY product_count DESC;

-- ============================================================
-- 2. 多表JOIN
-- ============================================================

SELECT 
    u.user_id,
    u.user_name,
    o.order_id,
    o.order_date,
    o.amount,
    p.product_name
FROM db_user.users u
INNER JOIN db_order.orders o ON u.user_id = o.user_id
LEFT JOIN db_order.order_items oi ON o.order_id = oi.order_id
LEFT JOIN db_product.products p ON oi.product_id = p.product_id
WHERE o.order_date >= '2024-01-01'
  AND u.status = 1
ORDER BY o.order_date DESC;

-- ============================================================
-- 3. CTE (Common Table Expression)
-- ============================================================

WITH monthly_sales AS (
    SELECT 
        DATE_FORMAT(order_date, 'yyyy-MM') as month,
        product_id,
        SUM(quantity) as total_qty,
        SUM(amount) as total_amount
    FROM db_order.orders o
    JOIN db_order.order_items oi ON o.order_id = oi.order_id
    WHERE order_date >= '2024-01-01'
    GROUP BY DATE_FORMAT(order_date, 'yyyy-MM'), product_id
),
product_ranking AS (
    SELECT 
        month,
        product_id,
        total_amount,
        ROW_NUMBER() OVER (PARTITION BY month ORDER BY total_amount DESC) as rank
    FROM monthly_sales
)
SELECT 
    pr.month,
    pr.product_id,
    p.product_name,
    pr.total_amount,
    pr.rank
FROM product_ranking pr
JOIN db_product.products p ON pr.product_id = p.product_id
WHERE pr.rank <= 10
ORDER BY pr.month, pr.rank;

-- ============================================================
-- 4. 子查询
-- ============================================================

SELECT *
FROM db_user.users
WHERE user_id IN (
    SELECT DISTINCT user_id
    FROM db_order.orders
    WHERE amount > 1000
      AND order_date >= date_sub(current_date(), 30)
);

SELECT 
    u.*,
    order_stats.order_count,
    order_stats.total_amount
FROM db_user.users u
JOIN (
    SELECT 
        user_id,
        COUNT(*) as order_count,
        SUM(amount) as total_amount
    FROM db_order.orders
    GROUP BY user_id
) order_stats ON u.user_id = order_stats.user_id
WHERE order_stats.order_count > 5;

-- ============================================================
-- 5. 窗口函数
-- ============================================================

SELECT 
    user_id,
    order_id,
    order_date,
    amount,
    SUM(amount) OVER (PARTITION BY user_id ORDER BY order_date) as cumulative_amount,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_date DESC) as order_rank,
    LAG(amount, 1, 0) OVER (PARTITION BY user_id ORDER BY order_date) as prev_amount,
    LEAD(amount, 1, 0) OVER (PARTITION BY user_id ORDER BY order_date) as next_amount
FROM db_order.orders
WHERE order_date >= '2024-01-01';

-- ============================================================
-- 6. CASE WHEN
-- ============================================================

SELECT 
    user_id,
    user_name,
    age,
    CASE 
        WHEN age < 18 THEN '未成年'
        WHEN age BETWEEN 18 AND 30 THEN '青年'
        WHEN age BETWEEN 31 AND 50 THEN '中年'
        ELSE '老年'
    END as age_group,
    CASE gender
        WHEN 'M' THEN '男'
        WHEN 'F' THEN '女'
        ELSE '未知'
    END as gender_name
FROM db_user.users;

-- ============================================================
-- 7. INSERT语句
-- ============================================================

-- INSERT INTO
INSERT INTO TABLE db_report.user_stats
PARTITION (dt = '2024-01-01')
SELECT 
    user_id,
    COUNT(*) as login_count,
    MAX(login_time) as last_login
FROM db_log.user_login
WHERE dt = '2024-01-01'
GROUP BY user_id;

-- INSERT OVERWRITE
INSERT OVERWRITE TABLE db_report.daily_summary
PARTITION (dt)
SELECT 
    category,
    COUNT(*) as pv,
    COUNT(DISTINCT user_id) as uv,
    SUM(amount) as total_amount,
    dt
FROM db_log.page_view
WHERE dt >= '2024-01-01'
GROUP BY category, dt;

-- ============================================================
-- 8. CREATE TABLE
-- ============================================================

-- 创建普通表
CREATE TABLE IF NOT EXISTS db_test.test_table (
    id BIGINT COMMENT '主键ID',
    name STRING COMMENT '名称',
    value DOUBLE COMMENT '值',
    create_time TIMESTAMP COMMENT '创建时间'
)
COMMENT '测试表'
PARTITIONED BY (dt STRING COMMENT '日期分区')
CLUSTERED BY (id) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 创建外部表
CREATE EXTERNAL TABLE IF NOT EXISTS db_ods.external_data (
    col1 STRING,
    col2 INT,
    col3 DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/hive/external/data';

-- CTAS (Create Table As Select)
CREATE TABLE db_temp.temp_result AS
SELECT 
    user_id,
    COUNT(*) as cnt
FROM db_log.events
WHERE dt = '2024-01-01'
GROUP BY user_id;

-- ============================================================
-- 9. DDL操作
-- ============================================================

-- 删除表
DROP TABLE IF EXISTS db_temp.temp_result;

-- 修改表
ALTER TABLE db_test.test_table ADD COLUMNS (
    extra_col1 STRING COMMENT '额外字段1',
    extra_col2 INT COMMENT '额外字段2'
);

ALTER TABLE db_test.test_table 
SET TBLPROPERTIES ('comment' = '更新后的表注释');

-- 添加分区
ALTER TABLE db_test.test_table 
ADD IF NOT EXISTS PARTITION (dt = '2024-01-01');

-- 删除分区
ALTER TABLE db_test.test_table 
DROP IF EXISTS PARTITION (dt = '2024-01-01');

-- ============================================================
-- 10. Hive特有语法
-- ============================================================

-- 修复分区
MSCK REPAIR TABLE db_test.test_table;

-- 分析表统计信息
ANALYZE TABLE db_test.test_table COMPUTE STATISTICS;
ANALYZE TABLE db_test.test_table COMPUTE STATISTICS FOR COLUMNS;

-- 查看表信息
DESCRIBE EXTENDED db_test.test_table;
DESCRIBE FORMATTED db_test.test_table;

-- 查看分区
SHOW PARTITIONS db_test.test_table;

-- 查看建表语句
SHOW CREATE TABLE db_test.test_table;

-- LOAD DATA
LOAD DATA LOCAL INPATH '/tmp/data.txt'
OVERWRITE INTO TABLE db_test.test_table
PARTITION (dt = '2024-01-01');

-- ============================================================
-- 11. Impala特有语法
-- ============================================================

-- 刷新元数据
INVALIDATE METADATA db_test.test_table;
REFRESH db_test.test_table;

-- 计算统计信息
COMPUTE STATS db_test.test_table;
COMPUTE INCREMENTAL STATS db_test.test_table PARTITION (dt = '2024-01-01');

-- ============================================================
-- 12. Spark SQL特有语法
-- ============================================================

-- 缓存表
CACHE TABLE cached_users AS
SELECT * FROM db_user.users WHERE status = 1;

-- 取消缓存
UNCACHE TABLE IF EXISTS cached_users;

-- 刷新表
REFRESH TABLE db_test.test_table;

-- ============================================================
-- 13. MERGE语句 (Hive 3.x / Spark 3.x)
-- ============================================================

MERGE INTO db_target.users AS target
USING db_source.user_updates AS source
ON target.user_id = source.user_id
WHEN MATCHED AND source.is_deleted = 1 THEN DELETE
WHEN MATCHED THEN UPDATE SET
    user_name = source.user_name,
    email = source.email,
    update_time = current_timestamp()
WHEN NOT MATCHED THEN INSERT (user_id, user_name, email, create_time)
VALUES (source.user_id, source.user_name, source.email, current_timestamp());

-- ============================================================
-- 14. UNION操作
-- ============================================================

SELECT user_id, 'order' as source, amount as value
FROM db_order.orders
WHERE order_date = '2024-01-01'

UNION ALL

SELECT user_id, 'refund' as source, -refund_amount as value
FROM db_order.refunds
WHERE refund_date = '2024-01-01'

UNION

SELECT user_id, 'bonus' as source, bonus_amount as value
FROM db_user.user_bonus
WHERE bonus_date = '2024-01-01';

-- ============================================================
-- 15. 复杂嵌套查询
-- ============================================================

WITH base_data AS (
    SELECT 
        user_id,
        product_id,
        order_date,
        amount
    FROM db_order.orders
    WHERE order_date >= date_sub(current_date(), 90)
),
user_product_matrix AS (
    SELECT 
        user_id,
        product_id,
        COUNT(*) as purchase_count,
        SUM(amount) as total_spend
    FROM base_data
    GROUP BY user_id, product_id
),
user_stats AS (
    SELECT 
        user_id,
        COUNT(DISTINCT product_id) as product_variety,
        SUM(purchase_count) as total_purchases,
        SUM(total_spend) as total_spend,
        MAX(total_spend) as max_single_product_spend
    FROM user_product_matrix
    GROUP BY user_id
)
SELECT 
    us.user_id,
    u.user_name,
    us.product_variety,
    us.total_purchases,
    us.total_spend,
    us.max_single_product_spend,
    CASE 
        WHEN us.total_spend >= 10000 AND us.product_variety >= 10 THEN 'Premium'
        WHEN us.total_spend >= 5000 THEN 'Gold'
        WHEN us.total_spend >= 1000 THEN 'Silver'
        ELSE 'Bronze'
    END as user_tier
FROM user_stats us
JOIN db_user.users u ON us.user_id = u.user_id
WHERE u.status = 1
ORDER BY us.total_spend DESC
LIMIT 1000;
