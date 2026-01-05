-- Hive SQL 示例代码
-- 演示Hive的各种SQL语法和特性

-- 设置Hive参数
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;

-- 使用数据库
USE data_warehouse;

-- 创建外部表
CREATE EXTERNAL TABLE IF NOT EXISTS ods_user_info (
    user_id         STRING COMMENT '用户ID',
    user_name       STRING COMMENT '用户名',
    age             INT COMMENT '年龄',
    gender          STRING COMMENT '性别',
    register_time   STRING COMMENT '注册时间',
    extra_info      STRING COMMENT 'JSON格式的额外信息'
)
PARTITIONED BY (dt STRING COMMENT '日期分区')
STORED AS ORC
LOCATION 'hdfs://namenode:8020/data/ods/user_info'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 加载数据
LOAD DATA INPATH '/tmp/user_data.txt' INTO TABLE ods_user_info PARTITION (dt='2024-01-01');

-- 修复分区
MSCK REPAIR TABLE ods_user_info;

-- 添加分区
ALTER TABLE ods_user_info ADD PARTITION (dt='2024-01-02') 
LOCATION 'hdfs://namenode:8020/data/ods/user_info/dt=2024-01-02';

-- 使用Hive特有函数
SELECT 
    user_id,
    user_name,
    get_json_object(extra_info, '$.city') AS city,
    get_json_object(extra_info, '$.phone') AS phone
FROM ods_user_info
WHERE dt = '2024-01-01';

-- lateral view explode 示例
SELECT 
    user_id,
    tag
FROM ods_user_info
LATERAL VIEW explode(split(get_json_object(extra_info, '$.tags'), ',')) t AS tag
WHERE dt = '2024-01-01';

-- collect_set 和 collect_list 示例
SELECT 
    gender,
    collect_set(user_name) AS unique_names,
    collect_list(age) AS all_ages
FROM ods_user_info
WHERE dt = '2024-01-01'
GROUP BY gender;

-- 插入数据到目标表
INSERT OVERWRITE TABLE dwd_user_profile PARTITION (dt)
SELECT 
    user_id,
    user_name,
    age,
    gender,
    get_json_object(extra_info, '$.city') AS city,
    dt
FROM ods_user_info
WHERE dt >= '2024-01-01';

-- 使用map函数
SELECT 
    user_id,
    map_keys(str_to_map(extra_info, ',', ':')) AS keys,
    map_values(str_to_map(extra_info, ',', ':')) AS values
FROM ods_user_info
LIMIT 10;

-- posexplode 示例
SELECT 
    user_id,
    pos,
    tag
FROM ods_user_info
LATERAL VIEW posexplode(split(get_json_object(extra_info, '$.tags'), ',')) t AS pos, tag
WHERE dt = '2024-01-01';
