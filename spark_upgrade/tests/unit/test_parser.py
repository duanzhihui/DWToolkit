"""解析器单元测试"""

import pytest

from spark_upgrade.core.parser import SparkParser, SparkCodeStructure


class TestSparkParser:
    """SparkParser 测试类"""

    def setup_method(self):
        """测试前置"""
        self.parser = SparkParser()

    def test_parse_spark_session(self):
        """测试解析 SparkSession"""
        code = '''
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()
'''
        result = self.parser.parse_code(code, "test.py")

        assert isinstance(result, SparkCodeStructure)
        assert len(result.spark_session_usage) >= 1
        assert result.file_path == "test.py"

    def test_parse_spark_imports(self):
        """测试解析 Spark 导入"""
        code = '''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, IntegerType
import pyspark.sql.functions as F
'''
        result = self.parser.parse_code(code, "test.py")

        spark_imports = self.parser.get_spark_imports(result)
        assert len(spark_imports) == 4

    def test_parse_dataframe_operations(self):
        """测试解析 DataFrame 操作"""
        code = '''
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("data.csv")
df.select("col1", "col2").filter("col1 > 10").show()
'''
        result = self.parser.parse_code(code, "test.py")

        assert len(result.dataframe_operations) >= 2

    def test_parse_rdd_operations(self):
        """测试解析 RDD 操作"""
        code = '''
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("data.csv")
rdd_result = df.rdd.map(lambda x: x.col1)
'''
        result = self.parser.parse_code(code, "test.py")

        assert len(result.rdd_operations) >= 1

    def test_parse_configurations(self):
        """测试解析配置"""
        code = '''
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "200")
'''
        result = self.parser.parse_code(code, "test.py")

        assert len(result.configurations) >= 1

    def test_parse_deprecated_apis(self):
        """测试解析弃用 API"""
        code = '''
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("data.csv")
df.registerTempTable("temp_table")
'''
        result = self.parser.parse_code(code, "test.py")

        deprecated = self.parser.get_deprecated_usage(result)
        assert len(deprecated) >= 1
        assert any("registerTempTable" in d["deprecated_api"] for d in deprecated)

    def test_has_spark_code_true(self):
        """测试检测包含 Spark 代码"""
        code = '''
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
'''
        result = self.parser.parse_code(code, "test.py")

        assert self.parser.has_spark_code(result) is True

    def test_has_spark_code_false(self):
        """测试检测不包含 Spark 代码"""
        code = '''
import pandas as pd
df = pd.read_csv("data.csv")
print(df.head())
'''
        result = self.parser.parse_code(code, "test.py")

        assert self.parser.has_spark_code(result) is False

    def test_parse_syntax_error(self):
        """测试解析语法错误"""
        code = '''
def broken_function(
    print("missing closing paren"
'''
        with pytest.raises(SyntaxError):
            self.parser.parse_code(code, "test.py")

    def test_parse_empty_code(self):
        """测试解析空代码"""
        code = ""
        result = self.parser.parse_code(code, "test.py")

        assert result.file_path == "test.py"
        assert len(result.imports) == 0

    def test_parse_complex_spark_code(self):
        """测试解析复杂 Spark 代码"""
        code = '''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg
from pyspark.sql.window import Window

spark = SparkSession.builder \\
    .appName("ComplexApp") \\
    .config("spark.sql.shuffle.partitions", "200") \\
    .getOrCreate()

df = spark.read.parquet("data.parquet")

window_spec = Window.partitionBy("category").orderBy("date")

result = df \\
    .filter(col("value") > 0) \\
    .withColumn("running_sum", sum("value").over(window_spec)) \\
    .groupBy("category") \\
    .agg(avg("value").alias("avg_value"))

result.write.mode("overwrite").parquet("output.parquet")
'''
        result = self.parser.parse_code(code, "test.py")

        assert self.parser.has_spark_code(result) is True
        assert len(result.dataframe_operations) > 0
        assert len(result.spark_session_usage) > 0


class TestSparkCodeStructure:
    """SparkCodeStructure 测试类"""

    def test_structure_fields(self):
        """测试结构字段"""
        structure = SparkCodeStructure(
            file_path="test.py",
            imports=[{"module": "pyspark"}],
            spark_session_usage=[{"name": "SparkSession"}],
        )

        assert structure.file_path == "test.py"
        assert len(structure.imports) == 1
        assert len(structure.spark_session_usage) == 1
        assert len(structure.dataframe_operations) == 0

    def test_structure_default_values(self):
        """测试结构默认值"""
        structure = SparkCodeStructure(file_path="test.py")

        assert structure.imports == []
        assert structure.spark_session_usage == []
        assert structure.dataframe_operations == []
        assert structure.rdd_operations == []
        assert structure.configurations == []
        assert structure.deprecated_apis == []
        assert structure.raw_code == ""
        assert structure.ast_tree is None
