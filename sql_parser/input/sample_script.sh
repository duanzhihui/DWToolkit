#!/bin/bash
# 示例Shell脚本 - 用于测试SQL提取
# 包含各种在Shell中嵌入SQL的方式

set -e

# 配置变量
DATE=$(date +%Y-%m-%d)
HIVE_DB="db_test"
IMPALA_HOST="impala-server.example.com"

# ============================================================
# 示例1: hive -e 执行SQL
# ============================================================

echo "执行Hive查询..."
hive -e "SELECT user_id, user_name FROM ${HIVE_DB}.user_info WHERE dt = '${DATE}' LIMIT 10"

# 多行SQL使用引号
hive -e "
SELECT 
    category,
    COUNT(*) as cnt,
    SUM(amount) as total
FROM db_order.orders
WHERE order_date = '${DATE}'
GROUP BY category
ORDER BY total DESC
"

# ============================================================
# 示例2: beeline 执行SQL
# ============================================================

beeline -u "jdbc:hive2://hive-server:10000/default" -n hadoop -e "
INSERT OVERWRITE TABLE db_report.daily_stats
PARTITION (dt = '${DATE}')
SELECT 
    hour(create_time) as hour,
    COUNT(DISTINCT user_id) as uv,
    COUNT(*) as pv
FROM db_log.page_view
WHERE dt = '${DATE}'
GROUP BY hour(create_time)
"

# ============================================================
# 示例3: impala-shell 执行SQL
# ============================================================

impala-shell -i ${IMPALA_HOST} -q "
INVALIDATE METADATA db_test.user_info;
SELECT * FROM db_test.user_info WHERE dt = '${DATE}' LIMIT 100
"

impala-shell -i ${IMPALA_HOST} -q "COMPUTE STATS db_test.user_info"

# ============================================================
# 示例4: spark-sql 执行SQL
# ============================================================

spark-sql --master yarn --deploy-mode client -e "
CACHE TABLE cached_orders AS
SELECT order_id, user_id, amount
FROM db_order.orders
WHERE order_date >= date_sub(current_date(), 7);

SELECT 
    user_id,
    COUNT(*) as order_cnt,
    SUM(amount) as total_amount
FROM cached_orders
GROUP BY user_id
HAVING order_cnt > 5
"

# ============================================================
# 示例5: heredoc方式 (推荐用于复杂SQL)
# ============================================================

hive << EOF
-- 创建临时表
CREATE TABLE IF NOT EXISTS db_temp.temp_user_stats (
    user_id BIGINT,
    login_cnt INT,
    last_login_time TIMESTAMP
)
STORED AS PARQUET;

-- 插入数据
INSERT OVERWRITE TABLE db_temp.temp_user_stats
SELECT 
    user_id,
    COUNT(*) as login_cnt,
    MAX(login_time) as last_login_time
FROM db_log.user_login
WHERE dt >= date_sub('${DATE}', 30)
GROUP BY user_id;

-- 查询结果
SELECT * FROM db_temp.temp_user_stats ORDER BY login_cnt DESC LIMIT 100;
EOF

# ============================================================
# 示例6: heredoc with 缩进 (<<-)
# ============================================================

beeline -u "jdbc:hive2://hive-server:10000/default" -n hadoop <<- ENDSQL
    WITH user_summary AS (
        SELECT 
            user_id,
            SUM(amount) as total_spend,
            COUNT(*) as order_count
        FROM db_order.orders
        WHERE order_date >= '2024-01-01'
        GROUP BY user_id
    )
    SELECT 
        CASE 
            WHEN total_spend >= 10000 THEN 'VIP'
            WHEN total_spend >= 1000 THEN 'Regular'
            ELSE 'New'
        END as user_level,
        COUNT(*) as user_count,
        AVG(total_spend) as avg_spend
    FROM user_summary
    GROUP BY 
        CASE 
            WHEN total_spend >= 10000 THEN 'VIP'
            WHEN total_spend >= 1000 THEN 'Regular'
            ELSE 'New'
        END
ENDSQL

# ============================================================
# 示例7: SQL变量赋值
# ============================================================

SQL="
SELECT 
    product_id,
    product_name,
    category,
    price
FROM db_product.products
WHERE status = 1
  AND stock > 0
ORDER BY price DESC
LIMIT 50
"

echo "执行SQL: ${SQL}"
hive -e "${SQL}"

# 另一种变量赋值方式
HQL='
DROP TABLE IF EXISTS db_temp.temp_products;
CREATE TABLE db_temp.temp_products AS
SELECT * FROM db_product.products WHERE category = "electronics"
'

hive -e "${HQL}"

# ============================================================
# 示例8: 复杂的ETL脚本
# ============================================================

QUERY="
-- Step 1: 清理目标表分区
ALTER TABLE db_dw.fact_orders DROP IF EXISTS PARTITION (dt = '${DATE}');

-- Step 2: 插入新数据
INSERT INTO TABLE db_dw.fact_orders
PARTITION (dt = '${DATE}')
SELECT 
    o.order_id,
    o.user_id,
    u.user_name,
    o.product_id,
    p.product_name,
    p.category,
    o.quantity,
    o.unit_price,
    o.quantity * o.unit_price as total_amount,
    o.order_status,
    o.create_time,
    o.update_time
FROM db_ods.orders o
LEFT JOIN db_dim.users u ON o.user_id = u.user_id
LEFT JOIN db_dim.products p ON o.product_id = p.product_id
WHERE o.dt = '${DATE}'
"

hive -e "${QUERY}"

# ============================================================
# 示例9: Impala 刷新和统计
# ============================================================

impala-shell -i ${IMPALA_HOST} -q "
REFRESH db_dw.fact_orders PARTITION (dt = '${DATE}');
COMPUTE INCREMENTAL STATS db_dw.fact_orders PARTITION (dt = '${DATE}');
"

echo "ETL任务完成!"
