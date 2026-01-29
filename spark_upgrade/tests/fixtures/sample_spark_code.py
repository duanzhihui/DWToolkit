"""
示例 Spark 代码 - 用于测试迁移功能

这个文件包含各种需要升级的 Spark 2.x 代码模式
"""

from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("SampleApp") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# 获取 SparkContext（旧方式）
sc = spark.sparkContext

# 读取数据
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data.csv")

# 使用弃用的 registerTempTable
df.registerTempTable("temp_table")

# 使用弃用的 unionAll
df2 = spark.read.csv("data2.csv")
combined = df.unionAll(df2)

# 使用 na.fill（可简化）
filled = df.na.fill(0)

# 使用 na.drop（可简化）
cleaned = df.na.drop()

# RDD 操作（不推荐）
rdd_result = df.rdd.map(lambda x: x.col1)

# 使用 import * 导入的函数
result = df.select(
    col("name"),
    lit("constant"),
    when(col("value") > 0, "positive").otherwise("negative")
)

# 类型转换（字符串方式）
typed = df.withColumn("str_col", col("int_col").cast("string"))

# 配置设置
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# 输出结果
result.show()
