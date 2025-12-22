#!/bin/bash
# 示例Shell脚本，包含SQL语句
# 用于测试sql_table_parser解析.sh文件时能正确过滤shell注释
# 同时演示变量表名的使用场景

# 数据库配置
DB_HOST="localhost"
DB_USER="root"
DB_NAME="mydb"

# 变量表名配置 - 用于动态指定schema和表名
SCHEMA_DW="dw"
SCHEMA_ODS="ods"
TABLE_USERS="dim_users"
TABLE_ORDERS="fact_orders"
TABLE_PRODUCTS="dim_products"
DATE_SUFFIX=$(date +%Y%m%d)

# 查询用户数据
echo "查询用户数据..."
mysql -h $DB_HOST -u $DB_USER $DB_NAME -e "
SELECT u.id, u.name, u.email
FROM users u
WHERE u.status = 'active';
"

# 查询订单统计
echo "查询订单统计..."
mysql -h $DB_HOST -u $DB_USER $DB_NAME -e "
WITH order_stats AS (
    SELECT user_id, COUNT(*) as order_count, SUM(amount) as total_amount
    FROM orders
    GROUP BY user_id
)
SELECT u.name, os.order_count, os.total_amount
FROM users u
INNER JOIN order_stats os ON u.id = os.user_id
ORDER BY os.total_amount DESC;
"

# 使用变量表名 - Shell变量格式 ${var} 和 $var
echo "使用变量表名查询..."
mysql -h $DB_HOST -u $DB_USER $DB_NAME -e "
SELECT u.id, u.name, o.order_id, o.amount
FROM ${SCHEMA_DW}.${TABLE_USERS} u
INNER JOIN ${SCHEMA_DW}.${TABLE_ORDERS} o ON u.id = o.user_id
WHERE o.order_date >= '2024-01-01';
"

# 使用变量表名 - 混合格式
echo "跨schema查询..."
mysql -h $DB_HOST -u $DB_USER $DB_NAME -e "
SELECT s.*, t.*
FROM ${SCHEMA_ODS}.raw_users s
LEFT JOIN $SCHEMA_DW.$TABLE_USERS t ON s.id = t.source_id;
"

# 动态创建分区表
echo "创建分区表..."
mysql -h $DB_HOST -u $DB_USER $DB_NAME -e "
CREATE TABLE IF NOT EXISTS ${SCHEMA_DW}.${TABLE_ORDERS}_${DATE_SUFFIX} (
    id INT PRIMARY KEY,
    user_id INT,
    amount DECIMAL(10,2)
);
"

# 更新产品价格
echo "更新产品价格..."
mysql -h $DB_HOST -u $DB_USER $DB_NAME -e "
UPDATE products
SET price = price * 1.1
WHERE category = 'electronics';
"

# 使用变量表名更新
echo "更新变量表..."
mysql -h $DB_HOST -u $DB_USER $DB_NAME -e "
UPDATE ${SCHEMA_DW}.${TABLE_PRODUCTS}
SET updated_at = NOW()
WHERE status = 'active';
"

# 插入日志
echo "插入日志..."
mysql -h $DB_HOST -u $DB_USER $DB_NAME -e "
INSERT INTO operation_logs (operation, executed_at)
VALUES ('price_update', NOW());
"

# 使用变量表名插入
echo "插入到变量表..."
mysql -h $DB_HOST -u $DB_USER $DB_NAME -e "
INSERT INTO ${SCHEMA_DW}.audit_logs (action, table_name, executed_at)
SELECT 'sync', '${TABLE_USERS}', NOW()
FROM dual;
"

# 清理临时表
echo "清理临时表..."
mysql -h $DB_HOST -u $DB_USER $DB_NAME -e "
TRUNCATE TABLE temp_results;
"

# 删除过期数据
echo "删除过期数据..."
mysql -h $DB_HOST -u $DB_USER $DB_NAME -e "
DELETE FROM session_cache
WHERE expired_at < NOW();
"

# 删除变量表中的过期数据
echo "删除变量表过期数据..."
mysql -h $DB_HOST -u $DB_USER $DB_NAME -e "
DELETE FROM ${SCHEMA_DW}.${TABLE_ORDERS}
WHERE order_date < DATE_SUB(NOW(), INTERVAL 1 YEAR);
"

echo "脚本执行完成"
