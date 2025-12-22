#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
示例Python文件 - 用于测试SQL提取
包含各种SQL嵌入方式
"""

from pyspark.sql import SparkSession

# 创建SparkSession
spark = SparkSession.builder.appName("SQLDemo").enableHiveSupport().getOrCreate()

# ============================================================
# 示例1: 三引号字符串中的SQL
# ============================================================

# Hive SQL - 创建表
create_table_sql = """
CREATE EXTERNAL TABLE IF NOT EXISTS db_test.user_info (
    user_id BIGINT COMMENT '用户ID',
    user_name STRING COMMENT '用户名',
    age INT COMMENT '年龄',
    create_time TIMESTAMP COMMENT '创建时间'
)
PARTITIONED BY (dt STRING COMMENT '日期分区')
STORED AS PARQUET
LOCATION '/user/hive/warehouse/db_test.db/user_info'
TBLPROPERTIES ('parquet.compression'='SNAPPY')
"""

# Hive SQL - 复杂查询 with CTE
query_sql = """
WITH active_users AS (
    SELECT user_id, user_name, age
    FROM db_test.user_info
    WHERE dt = '${date}'
      AND age >= 18
),
user_orders AS (
    SELECT 
        o.user_id,
        COUNT(*) as order_cnt,
        SUM(o.amount) as total_amount
    FROM db_order.orders o
    INNER JOIN active_users au ON o.user_id = au.user_id
    WHERE o.order_date >= date_sub('${date}', 30)
    GROUP BY o.user_id
)
SELECT 
    au.user_id,
    au.user_name,
    au.age,
    COALESCE(uo.order_cnt, 0) as order_cnt,
    COALESCE(uo.total_amount, 0) as total_amount
FROM active_users au
LEFT JOIN user_orders uo ON au.user_id = uo.user_id
ORDER BY total_amount DESC
LIMIT 1000
"""

# ============================================================
# 示例2: 单引号三引号
# ============================================================

insert_sql = '''
INSERT OVERWRITE TABLE db_report.daily_summary
PARTITION (dt = '${date}')
SELECT 
    category,
    COUNT(DISTINCT user_id) as uv,
    COUNT(*) as pv,
    SUM(amount) as total_amount
FROM db_log.page_view
WHERE dt = '${date}'
GROUP BY category
'''

# ============================================================
# 示例3: spark.sql() 方法调用
# ============================================================

df1 = spark.sql("""
SELECT 
    product_id,
    product_name,
    price,
    stock
FROM db_product.products
WHERE status = 1
  AND price > 0
""")

df2 = spark.sql("SELECT user_id, COUNT(*) as cnt FROM db_log.clicks GROUP BY user_id")

# ============================================================
# 示例4: f-string 格式化SQL
# ============================================================

table_name = "db_test.user_behavior"
date_str = "2024-01-01"

dynamic_sql = f"""
SELECT 
    user_id,
    behavior_type,
    COUNT(*) as cnt
FROM {table_name}
WHERE dt = '{date_str}'
GROUP BY user_id, behavior_type
HAVING cnt > 10
"""

# ============================================================
# 示例5: Impala SQL
# ============================================================

impala_sql = """
INVALIDATE METADATA db_test.user_info;

COMPUTE STATS db_test.user_info;

SELECT /* +STRAIGHT_JOIN */
    a.user_id,
    b.order_id,
    b.amount
FROM db_test.user_info a
INNER JOIN db_order.orders b ON a.user_id = b.user_id
WHERE a.dt = '2024-01-01'
"""

# ============================================================
# 示例6: Spark SQL 特有语法
# ============================================================

spark_cache_sql = """
CACHE TABLE cached_users AS
SELECT user_id, user_name
FROM db_test.user_info
WHERE age > 18;

REFRESH TABLE db_test.user_info;
"""

# ============================================================
# 示例7: 复杂的多表JOIN
# ============================================================

complex_join_sql = """
SELECT 
    u.user_id,
    u.user_name,
    o.order_id,
    o.order_date,
    p.product_name,
    oi.quantity,
    oi.unit_price,
    oi.quantity * oi.unit_price as item_total
FROM db_user.users u
INNER JOIN db_order.orders o ON u.user_id = o.user_id
INNER JOIN db_order.order_items oi ON o.order_id = oi.order_id
LEFT JOIN db_product.products p ON oi.product_id = p.product_id
LEFT OUTER JOIN db_user.user_address ua ON u.user_id = ua.user_id AND ua.is_default = 1
CROSS JOIN db_config.global_settings gs
WHERE o.order_date BETWEEN '2024-01-01' AND '2024-01-31'
  AND o.status IN (1, 2, 3)
  AND u.is_active = 1
ORDER BY o.order_date DESC, item_total DESC
"""

# ============================================================
# 示例8: DDL操作
# ============================================================

ddl_sql = """
-- 删除表
DROP TABLE IF EXISTS db_temp.temp_result;

-- 创建临时表
CREATE TABLE db_temp.temp_result AS
SELECT * FROM db_test.user_info WHERE dt = '2024-01-01';

-- 修改表
ALTER TABLE db_test.user_info ADD COLUMNS (email STRING COMMENT '邮箱');

-- 修复分区
MSCK REPAIR TABLE db_test.user_info;

-- 分析表
ANALYZE TABLE db_test.user_info COMPUTE STATISTICS;
"""

# ============================================================
# 示例9: LOAD DATA
# ============================================================

load_data_sql = """
LOAD DATA LOCAL INPATH '/tmp/data/user_info.txt'
OVERWRITE INTO TABLE db_test.user_info
PARTITION (dt = '2024-01-01')
"""

# ============================================================
# 示例10: MERGE语句 (Hive 3.x / Spark)
# ============================================================

merge_sql = """
MERGE INTO db_target.user_info AS target
USING db_source.user_updates AS source
ON target.user_id = source.user_id
WHEN MATCHED THEN
    UPDATE SET 
        user_name = source.user_name,
        age = source.age,
        update_time = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (user_id, user_name, age, create_time)
    VALUES (source.user_id, source.user_name, source.age, current_timestamp())
"""

# 执行SQL
def execute_sqls():
    """执行示例SQL"""
    spark.sql(create_table_sql)
    result = spark.sql(query_sql)
    result.show()

if __name__ == "__main__":
    execute_sqls()
