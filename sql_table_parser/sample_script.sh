#!/bin/bash
# 示例Shell脚本，包含SQL语句
# 用于测试sql_table_parser解析.sh文件时能正确过滤shell注释

# 数据库配置
DB_HOST="localhost"
DB_USER="root"
DB_NAME="mydb"

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

# 更新产品价格
echo "更新产品价格..."
mysql -h $DB_HOST -u $DB_USER $DB_NAME -e "
UPDATE products
SET price = price * 1.1
WHERE category = 'electronics';
"

# 插入日志
echo "插入日志..."
mysql -h $DB_HOST -u $DB_USER $DB_NAME -e "
INSERT INTO operation_logs (operation, executed_at)
VALUES ('price_update', NOW());
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

echo "脚本执行完成"
