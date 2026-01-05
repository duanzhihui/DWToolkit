# -*- coding: utf-8 -*-
"""
混合代码示例
演示同时使用Spark和Hive的场景
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


def create_spark_with_hive():
    """创建支持Hive的SparkSession"""
    spark = SparkSession.builder \
        .appName("SparkHiveExample") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def read_hive_table(spark):
    """通过Spark读取Hive表"""
    # 直接读取Hive表
    df = spark.sql("""
        SELECT user_id, user_name, age
        FROM data_warehouse.ods_user_info
        WHERE dt = '2024-01-01'
    """)
    return df


def write_to_hive(spark, df):
    """通过Spark写入Hive表"""
    # 创建临时视图
    df.createOrReplaceTempView("temp_result")
    
    # 使用INSERT OVERWRITE TABLE写入Hive
    spark.sql("""
        INSERT OVERWRITE TABLE data_warehouse.dwd_user_profile
        PARTITION (dt='2024-01-01')
        SELECT user_id, user_name, age, 'processed' as status
        FROM temp_result
    """)


def main():
    spark = create_spark_with_hive()
    
    # 读取Hive数据
    df = read_hive_table(spark)
    
    # Spark处理
    df_processed = df.filter(col("age") > 18) \
        .withColumn("age_group", 
                    lit("adult") if col("age") >= 18 else lit("minor"))
    
    # 写回Hive
    write_to_hive(spark, df_processed)
    
    spark.stop()


if __name__ == "__main__":
    main()
