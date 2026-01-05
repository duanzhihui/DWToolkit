# -*- coding: utf-8 -*-
"""
Spark/PySpark 示例代码
演示Spark的各种API和使用方式
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, count, sum as spark_sum, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression


def create_spark_session():
    """创建SparkSession"""
    spark = SparkSession.builder \
        .appName("SparkExample") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def dataframe_operations(spark):
    """DataFrame API 操作示例"""
    # 读取数据
    df = spark.read.parquet("hdfs://namenode:8020/data/user_info")
    
    # 基本操作
    df_filtered = df.filter(col("age") > 18) \
        .select("user_id", "user_name", "age", "gender") \
        .withColumn("age_group", when(col("age") < 30, "young")
                    .when(col("age") < 50, "middle")
                    .otherwise("senior")) \
        .dropDuplicates(["user_id"]) \
        .orderBy(col("age").desc())
    
    # 聚合操作
    df_agg = df.groupBy("gender") \
        .agg(
            count("*").alias("total_count"),
            avg("age").alias("avg_age"),
            spark_sum("score").alias("total_score")
        )
    
    # Join操作
    df_orders = spark.read.json("hdfs://namenode:8020/data/orders")
    df_joined = df.join(df_orders, df.user_id == df_orders.user_id, "left")
    
    # 缓存
    df_filtered.cache()
    df_filtered.persist()
    
    # 写入数据
    df_filtered.write \
        .mode("overwrite") \
        .partitionBy("age_group") \
        .parquet("hdfs://namenode:8020/output/user_processed")
    
    return df_filtered


def rdd_operations(spark):
    """RDD API 操作示例"""
    sc = spark.sparkContext
    
    # 创建RDD
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    rdd = sc.parallelize(data)
    
    # RDD转换
    rdd_mapped = rdd.map(lambda x: (x[0], x[1] * 2))
    rdd_filtered = rdd_mapped.filter(lambda x: x[1] > 50)
    
    # flatMap
    rdd_flat = rdd.flatMap(lambda x: [x[0], str(x[1])])
    
    # reduceByKey
    word_counts = sc.parallelize(["a", "b", "a", "c", "b", "a"]) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b)
    
    # 收集结果
    results = rdd_filtered.collect()
    sample = rdd.take(10)
    total = rdd.count()
    
    # 保存
    rdd_mapped.saveAsTextFile("hdfs://namenode:8020/output/rdd_result")
    
    return results


def spark_sql_operations(spark):
    """Spark SQL 操作示例"""
    # 读取数据并注册临时视图
    df = spark.read.csv("hdfs://namenode:8020/data/sales.csv", header=True)
    df.createOrReplaceTempView("sales")
    df.createTempView("sales_temp")
    
    # 执行SQL查询
    result = spark.sql("""
        SELECT 
            product_id,
            SUM(amount) as total_amount,
            COUNT(*) as order_count
        FROM sales
        WHERE sale_date >= '2024-01-01'
        GROUP BY product_id
        HAVING SUM(amount) > 1000
        ORDER BY total_amount DESC
    """)
    
    return result


def ml_example(spark):
    """Spark ML 示例"""
    # 准备数据
    data = spark.read.parquet("hdfs://namenode:8020/data/ml_data")
    
    # 特征工程
    assembler = VectorAssembler(
        inputCols=["feature1", "feature2", "feature3"],
        outputCol="features"
    )
    data_assembled = assembler.transform(data)
    
    # 训练模型
    lr = LogisticRegression(featuresCol="features", labelCol="label")
    model = lr.fit(data_assembled)
    
    # 预测
    predictions = model.transform(data_assembled)
    
    return predictions


def main():
    # 创建SparkSession
    spark = create_spark_session()
    
    try:
        # DataFrame操作
        df_result = dataframe_operations(spark)
        df_result.show()
        
        # RDD操作
        rdd_result = rdd_operations(spark)
        print(rdd_result)
        
        # Spark SQL
        sql_result = spark_sql_operations(spark)
        sql_result.show()
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
