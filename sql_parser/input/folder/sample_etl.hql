-- ============================================================
-- 示例HQL文件 - Hive ETL脚本
-- 用于测试SQL提取
-- ============================================================

-- 设置Hive参数
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET mapreduce.job.queuename=etl_queue;

-- ============================================================
-- Step 1: 创建目标表
-- ============================================================

CREATE TABLE IF NOT EXISTS db_dw.dwd_user_behavior (
    user_id BIGINT COMMENT '用户ID',
    session_id STRING COMMENT '会话ID',
    page_id STRING COMMENT '页面ID',
    action_type STRING COMMENT '行为类型',
    action_time TIMESTAMP COMMENT '行为时间',
    duration INT COMMENT '停留时长(秒)',
    device_type STRING COMMENT '设备类型',
    os_type STRING COMMENT '操作系统',
    browser STRING COMMENT '浏览器',
    ip_address STRING COMMENT 'IP地址',
    city STRING COMMENT '城市',
    province STRING COMMENT '省份'
)
COMMENT '用户行为明细表'
PARTITIONED BY (dt STRING COMMENT '日期分区')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ============================================================
-- Step 2: 清理目标分区
-- ============================================================

ALTER TABLE db_dw.dwd_user_behavior 
DROP IF EXISTS PARTITION (dt = '${bizdate}');

-- ============================================================
-- Step 3: 数据加工
-- ============================================================

INSERT INTO TABLE db_dw.dwd_user_behavior
PARTITION (dt = '${bizdate}')
SELECT 
    -- 用户信息
    COALESCE(ub.user_id, 0) as user_id,
    ub.session_id,
    
    -- 页面信息
    ub.page_id,
    ub.action_type,
    ub.action_time,
    
    -- 计算停留时长
    CAST(
        UNIX_TIMESTAMP(
            LEAD(ub.action_time, 1, ub.action_time) 
            OVER (PARTITION BY ub.session_id ORDER BY ub.action_time)
        ) - UNIX_TIMESTAMP(ub.action_time) 
        AS INT
    ) as duration,
    
    -- 设备信息
    CASE 
        WHEN ub.user_agent LIKE '%Mobile%' THEN 'Mobile'
        WHEN ub.user_agent LIKE '%Tablet%' THEN 'Tablet'
        ELSE 'PC'
    END as device_type,
    
    -- 解析操作系统
    CASE 
        WHEN ub.user_agent LIKE '%Windows%' THEN 'Windows'
        WHEN ub.user_agent LIKE '%Mac OS%' THEN 'MacOS'
        WHEN ub.user_agent LIKE '%Android%' THEN 'Android'
        WHEN ub.user_agent LIKE '%iOS%' THEN 'iOS'
        WHEN ub.user_agent LIKE '%Linux%' THEN 'Linux'
        ELSE 'Other'
    END as os_type,
    
    -- 解析浏览器
    CASE 
        WHEN ub.user_agent LIKE '%Chrome%' THEN 'Chrome'
        WHEN ub.user_agent LIKE '%Firefox%' THEN 'Firefox'
        WHEN ub.user_agent LIKE '%Safari%' AND ub.user_agent NOT LIKE '%Chrome%' THEN 'Safari'
        WHEN ub.user_agent LIKE '%Edge%' THEN 'Edge'
        WHEN ub.user_agent LIKE '%MSIE%' OR ub.user_agent LIKE '%Trident%' THEN 'IE'
        ELSE 'Other'
    END as browser,
    
    -- IP和地理信息
    ub.ip_address,
    COALESCE(ip.city, '未知') as city,
    COALESCE(ip.province, '未知') as province

FROM db_ods.ods_user_behavior ub
LEFT JOIN db_dim.dim_ip_location ip 
    ON ub.ip_address = ip.ip_address
WHERE ub.dt = '${bizdate}'
  AND ub.action_type IS NOT NULL
  AND ub.session_id IS NOT NULL;

-- ============================================================
-- Step 4: 数据质量检查
-- ============================================================

-- 检查数据量
SELECT 
    '${bizdate}' as check_date,
    'dwd_user_behavior' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT user_id) as user_count,
    COUNT(DISTINCT session_id) as session_count
FROM db_dw.dwd_user_behavior
WHERE dt = '${bizdate}';

-- 检查空值率
SELECT 
    '${bizdate}' as check_date,
    SUM(CASE WHEN user_id = 0 THEN 1 ELSE 0 END) / COUNT(*) as null_user_rate,
    SUM(CASE WHEN city = '未知' THEN 1 ELSE 0 END) / COUNT(*) as unknown_city_rate
FROM db_dw.dwd_user_behavior
WHERE dt = '${bizdate}';

-- ============================================================
-- Step 5: 更新统计信息
-- ============================================================

ANALYZE TABLE db_dw.dwd_user_behavior PARTITION (dt = '${bizdate}') COMPUTE STATISTICS;

-- ============================================================
-- Step 6: 构建汇总表
-- ============================================================

INSERT OVERWRITE TABLE db_dw.dws_user_behavior_daily
PARTITION (dt = '${bizdate}')
SELECT 
    user_id,
    COUNT(DISTINCT session_id) as session_count,
    COUNT(*) as action_count,
    SUM(duration) as total_duration,
    AVG(duration) as avg_duration,
    COUNT(DISTINCT page_id) as page_count,
    MIN(action_time) as first_action_time,
    MAX(action_time) as last_action_time,
    COLLECT_SET(device_type)[0] as main_device,
    COLLECT_SET(city)[0] as main_city
FROM db_dw.dwd_user_behavior
WHERE dt = '${bizdate}'
  AND user_id > 0
GROUP BY user_id;

-- ============================================================
-- Step 7: 构建用户画像标签
-- ============================================================

WITH user_behavior_stats AS (
    SELECT 
        user_id,
        SUM(session_count) as total_sessions,
        SUM(action_count) as total_actions,
        SUM(total_duration) as total_duration,
        COUNT(DISTINCT dt) as active_days
    FROM db_dw.dws_user_behavior_daily
    WHERE dt >= date_sub('${bizdate}', 30)
    GROUP BY user_id
),
user_order_stats AS (
    SELECT 
        user_id,
        COUNT(*) as order_count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM db_dw.dwd_order_detail
    WHERE dt >= date_sub('${bizdate}', 30)
    GROUP BY user_id
)
INSERT OVERWRITE TABLE db_dm.dm_user_profile
PARTITION (dt = '${bizdate}')
SELECT 
    ubs.user_id,
    
    -- 活跃度标签
    CASE 
        WHEN ubs.active_days >= 20 THEN '高活跃'
        WHEN ubs.active_days >= 10 THEN '中活跃'
        WHEN ubs.active_days >= 3 THEN '低活跃'
        ELSE '沉默'
    END as activity_level,
    
    -- 消费能力标签
    CASE 
        WHEN uos.total_amount >= 10000 THEN '高消费'
        WHEN uos.total_amount >= 1000 THEN '中消费'
        WHEN uos.total_amount > 0 THEN '低消费'
        ELSE '未消费'
    END as consumption_level,
    
    -- 用户价值标签
    CASE 
        WHEN ubs.active_days >= 20 AND uos.total_amount >= 5000 THEN '高价值'
        WHEN ubs.active_days >= 10 AND uos.total_amount >= 1000 THEN '中价值'
        WHEN uos.total_amount > 0 THEN '低价值'
        ELSE '潜在用户'
    END as user_value,
    
    -- 统计指标
    ubs.total_sessions,
    ubs.total_actions,
    ubs.total_duration,
    ubs.active_days,
    COALESCE(uos.order_count, 0) as order_count,
    COALESCE(uos.total_amount, 0) as total_amount,
    COALESCE(uos.avg_amount, 0) as avg_amount

FROM user_behavior_stats ubs
LEFT JOIN user_order_stats uos ON ubs.user_id = uos.user_id;

-- 完成
SELECT 'ETL任务完成' as status, current_timestamp() as finish_time;
