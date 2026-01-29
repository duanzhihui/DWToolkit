"""
示例 Spark 代码 - 升级后的版本

这个文件展示了升级到 Spark 3.x 后的代码应该是什么样子
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("SampleApp") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# 获取 SparkContext（通过 SparkSession）
sc = spark.sparkContext

# 读取数据
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("data.csv")

# 使用 createOrReplaceTempView 替代 registerTempTable
df.createOrReplaceTempView("temp_table")

# 使用 union 替代 unionAll
df2 = spark.read.csv("data2.csv")
combined = df.union(df2)

# 使用 fillna 替代 na.fill
filled = df.fillna(0)

# 使用 dropna 替代 na.drop
cleaned = df.dropna()

# 推荐使用 DataFrame API 替代 RDD 操作
# rdd_result = df.rdd.map(lambda x: x.col1)
# 替换为:
rdd_result = df.select(F.col("col1"))

# 使用 F. 前缀的函数
result = df.select(
    F.col("name"),
    F.lit("constant"),
    F.when(F.col("value") > 0, "positive").otherwise("negative")
)

# 类型转换（使用类型对象）
typed = df.withColumn("str_col", F.col("int_col").cast(StringType()))

# 配置设置（更新后的配置名）
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# 输出结果
result.show()
