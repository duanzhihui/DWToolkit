"""转换器单元测试"""

import pytest

from spark_upgrade.core.parser import SparkParser
from spark_upgrade.core.transformer import (
    SparkTransformer,
    TransformationRule,
    TransformationResult,
)


class TestSparkTransformer:
    """SparkTransformer 测试类"""

    def setup_method(self):
        """测试前置"""
        self.parser = SparkParser()
        self.transformer = SparkTransformer(target_version="3.2")

    def test_transform_register_temp_table(self):
        """测试转换 registerTempTable"""
        code = '''
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("data.csv")
df.registerTempTable("temp_table")
'''
        structure = self.parser.parse_code(code, "test.py")
        result = self.transformer.transform(structure)

        assert result.success is True
        assert ".createOrReplaceTempView(" in result.transformed_code
        assert ".registerTempTable(" not in result.transformed_code

    def test_transform_union_all(self):
        """测试转换 unionAll"""
        code = '''
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df1 = spark.read.csv("data1.csv")
df2 = spark.read.csv("data2.csv")
df3 = df1.unionAll(df2)
'''
        structure = self.parser.parse_code(code, "test.py")
        result = self.transformer.transform(structure)

        assert result.success is True
        assert ".union(" in result.transformed_code
        assert ".unionAll(" not in result.transformed_code

    def test_transform_import_star(self):
        """测试转换 import *"""
        code = '''
from pyspark.sql.functions import *
df.select(col("name"))
'''
        structure = self.parser.parse_code(code, "test.py")
        result = self.transformer.transform(structure)

        assert result.success is True
        assert "import functions as F" in result.transformed_code

    def test_transform_sql_context(self):
        """测试转换 SQLContext"""
        code = '''
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
'''
        structure = self.parser.parse_code(code, "test.py")
        result = self.transformer.transform(structure)

        assert result.success is True
        assert "SparkSession" in result.transformed_code

    def test_transform_preserves_valid_code(self):
        """测试保留有效代码"""
        code = '''
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("data.csv")
df.select(F.col("name")).show()
'''
        structure = self.parser.parse_code(code, "test.py")
        result = self.transformer.transform(structure)

        assert result.success is True
        # 代码应该基本保持不变
        assert "SparkSession" in result.transformed_code
        assert "F.col" in result.transformed_code

    def test_transform_result_has_changes(self):
        """测试转换结果包含变更"""
        code = '''
df.registerTempTable("test")
df.unionAll(df2)
'''
        structure = self.parser.parse_code(code, "test.py")
        result = self.transformer.transform(structure)

        assert result.has_changes is True
        assert len(result.changes_applied) >= 2

    def test_transform_result_no_changes(self):
        """测试转换结果无变更"""
        code = '''
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("data.csv")
df.show()
'''
        structure = self.parser.parse_code(code, "test.py")
        result = self.transformer.transform(structure)

        assert result.success is True
        # 可能有警告但没有实际变更

    def test_transform_code_directly(self):
        """测试直接转换代码"""
        code = '''
df.registerTempTable("test")
'''
        result = self.transformer.transform_code(code)

        assert isinstance(result, TransformationResult)
        assert result.success is True

    def test_add_custom_rule(self):
        """测试添加自定义规则"""
        custom_rule = TransformationRule(
            name="custom_rule",
            description="自定义规则测试",
            pattern="old_function(",
            replacement="new_function(",
            priority=100,
        )
        self.transformer.add_custom_rule(custom_rule)

        code = "result = old_function(x)"
        result = self.transformer.transform_code(code)

        assert "new_function(" in result.transformed_code

    def test_preview_changes(self):
        """测试预览变更"""
        code = '''
df.registerTempTable("test")
df.unionAll(df2)
'''
        structure = self.parser.parse_code(code, "test.py")
        preview = self.transformer.preview_changes(structure)

        assert "total_changes" in preview
        assert "changes" in preview
        assert preview["total_changes"] >= 2

    def test_get_rules_for_version(self):
        """测试获取指定版本规则"""
        rules_30 = self.transformer.get_rules_for_version("3.0")
        rules_32 = self.transformer.get_rules_for_version("3.2")

        assert len(rules_30) > 0
        assert len(rules_32) >= len(rules_30)


class TestTransformationRule:
    """TransformationRule 测试类"""

    def test_rule_creation(self):
        """测试规则创建"""
        rule = TransformationRule(
            name="test_rule",
            description="测试规则",
            pattern="old",
            replacement="new",
            priority=10,
            version="3.0",
        )

        assert rule.name == "test_rule"
        assert rule.description == "测试规则"
        assert rule.pattern == "old"
        assert rule.replacement == "new"
        assert rule.priority == 10
        assert rule.version == "3.0"
        assert rule.auto_fixable is True
        assert rule.regex is False

    def test_rule_with_regex(self):
        """测试正则规则"""
        rule = TransformationRule(
            name="regex_rule",
            description="正则规则",
            pattern=r"\.cast\(['\"]string['\"]\)",
            replacement=".cast(StringType())",
            regex=True,
        )

        assert rule.regex is True


class TestTransformationResult:
    """TransformationResult 测试类"""

    def test_result_success(self):
        """测试成功结果"""
        result = TransformationResult(
            success=True,
            original_code="old code",
            transformed_code="new code",
        )

        assert result.success is True
        assert result.original_code == "old code"
        assert result.transformed_code == "new code"

    def test_result_has_changes(self):
        """测试结果有变更"""
        result = TransformationResult(
            success=True,
            original_code="old",
            transformed_code="new",
            changes_applied=[{"rule": "test"}],
        )

        assert result.has_changes is True

    def test_result_no_changes(self):
        """测试结果无变更"""
        result = TransformationResult(
            success=True,
            original_code="code",
            transformed_code="code",
        )

        assert result.has_changes is False
