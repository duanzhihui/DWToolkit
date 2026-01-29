"""验证器单元测试"""

import pytest

from spark_upgrade.core.validator import SparkValidator, ValidationResult, ValidationIssue


class TestSparkValidator:
    """SparkValidator 测试类"""

    def setup_method(self):
        """测试前置"""
        self.validator = SparkValidator(target_version="3.2")

    def test_validate_valid_code(self):
        """测试验证有效代码"""
        code = '''
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("data.csv")
df.show()
'''
        result = self.validator.validate(code, "test.py")

        assert result.syntax_valid is True
        assert result.valid is True

    def test_validate_syntax_error(self):
        """测试验证语法错误"""
        code = '''
def broken(
    print("missing paren"
'''
        result = self.validator.validate(code, "test.py")

        assert result.syntax_valid is False
        assert result.valid is False
        assert result.error_count > 0

    def test_validate_deprecated_api(self):
        """测试验证弃用 API"""
        code = '''
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("data.csv")
df.registerTempTable("test")
'''
        result = self.validator.validate(code, "test.py")

        assert result.syntax_valid is True
        assert result.warning_count > 0

    def test_validate_spark_context(self):
        """测试验证 SparkContext"""
        code = '''
from pyspark import SparkContext
sc = SparkContext()
'''
        result = self.validator.validate(code, "test.py")

        assert result.syntax_valid is True
        assert result.warning_count > 0

    def test_validate_import_star(self):
        """测试验证 import *"""
        code = '''
from pyspark.sql.functions import *
'''
        result = self.validator.validate(code, "test.py")

        assert result.syntax_valid is True
        # 应该有 info 级别的问题

    def test_validate_rdd_operations(self):
        """测试验证 RDD 操作"""
        code = '''
df.rdd.map(lambda x: x.col1)
'''
        result = self.validator.validate(code, "test.py")

        assert result.syntax_valid is True
        # 应该有 info 级别的问题

    def test_compatibility_score_high(self):
        """测试高兼容性分数"""
        code = '''
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("data.csv")
df.select(F.col("name")).show()
'''
        result = self.validator.validate(code, "test.py")

        assert result.compatibility_score >= 0.8

    def test_compatibility_score_low(self):
        """测试低兼容性分数"""
        code = '''
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *

sc = SparkContext()
sqlContext = SQLContext(sc)
df = sqlContext.read.csv("data.csv")
df.registerTempTable("test")
df.unionAll(df2)
df.rdd.map(lambda x: x)
'''
        result = self.validator.validate(code, "test.py")

        assert result.compatibility_score < 0.8

    def test_check_imports(self):
        """测试检查导入"""
        code = '''
from pyspark.sql.functions import *
from pyspark.sql.types import *
'''
        issues = self.validator.check_imports(code)

        assert len(issues) >= 2

    def test_get_summary(self):
        """测试获取摘要"""
        code = '''
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
'''
        result = self.validator.validate(code, "test.py")
        summary = self.validator.get_summary(result)

        assert "valid" in summary
        assert "syntax_valid" in summary
        assert "compatibility_score" in summary
        assert "error_count" in summary
        assert "warning_count" in summary


class TestValidationResult:
    """ValidationResult 测试类"""

    def test_result_valid(self):
        """测试有效结果"""
        result = ValidationResult(
            valid=True,
            syntax_valid=True,
            compatibility_score=0.95,
        )

        assert result.valid is True
        assert result.syntax_valid is True
        assert result.error_count == 0
        assert result.warning_count == 0

    def test_result_with_issues(self):
        """测试带问题的结果"""
        result = ValidationResult(
            valid=False,
            syntax_valid=True,
            issues=[
                ValidationIssue(severity="error", message="错误1"),
                ValidationIssue(severity="warning", message="警告1"),
                ValidationIssue(severity="warning", message="警告2"),
                ValidationIssue(severity="info", message="信息1"),
            ],
        )

        assert result.error_count == 1
        assert result.warning_count == 2


class TestValidationIssue:
    """ValidationIssue 测试类"""

    def test_issue_creation(self):
        """测试问题创建"""
        issue = ValidationIssue(
            severity="error",
            message="测试错误",
            line=10,
            col=5,
            code="E001",
        )

        assert issue.severity == "error"
        assert issue.message == "测试错误"
        assert issue.line == 10
        assert issue.col == 5
        assert issue.code == "E001"

    def test_issue_defaults(self):
        """测试问题默认值"""
        issue = ValidationIssue(
            severity="warning",
            message="测试警告",
        )

        assert issue.line is None
        assert issue.col is None
        assert issue.code is None
