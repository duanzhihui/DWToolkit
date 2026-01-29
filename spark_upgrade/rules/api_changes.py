"""API 变更规则 - Spark 2.x 到 3.x 的 API 变更"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class APIChange:
    """API 变更定义"""
    old_api: str
    new_api: str
    version: str
    description: str
    auto_fixable: bool = True
    category: str = "general"
    notes: Optional[str] = None


# SparkSession 相关变更
SPARK_SESSION_CHANGES: List[APIChange] = [
    APIChange(
        old_api="SparkContext()",
        new_api="SparkSession.builder.getOrCreate().sparkContext",
        version="3.0",
        description="SparkContext 应通过 SparkSession 获取",
        category="session",
    ),
    APIChange(
        old_api="SQLContext(sc)",
        new_api="SparkSession.builder.getOrCreate()",
        version="3.0",
        description="SQLContext 已弃用，使用 SparkSession",
        category="session",
    ),
    APIChange(
        old_api="HiveContext(sc)",
        new_api="SparkSession.builder.enableHiveSupport().getOrCreate()",
        version="3.0",
        description="HiveContext 已弃用，使用 SparkSession",
        category="session",
    ),
]

# DataFrame API 变更
DATAFRAME_API_CHANGES: List[APIChange] = [
    APIChange(
        old_api=".registerTempTable(",
        new_api=".createOrReplaceTempView(",
        version="3.0",
        description="registerTempTable 已弃用",
        category="dataframe",
    ),
    APIChange(
        old_api=".unionAll(",
        new_api=".union(",
        version="3.0",
        description="unionAll 已弃用，使用 union",
        category="dataframe",
    ),
    APIChange(
        old_api=".na.fill(",
        new_api=".fillna(",
        version="3.0",
        description="na.fill 可以简化为 fillna",
        category="dataframe",
        auto_fixable=True,
    ),
    APIChange(
        old_api=".na.drop(",
        new_api=".dropna(",
        version="3.0",
        description="na.drop 可以简化为 dropna",
        category="dataframe",
        auto_fixable=True,
    ),
    APIChange(
        old_api=".toDF(*cols)",
        new_api=".toDF(*cols)",
        version="3.0",
        description="toDF 在 Spark 3.x 中行为可能不同",
        auto_fixable=False,
        category="dataframe",
        notes="检查列名处理是否符合预期",
    ),
]

# 函数 API 变更
FUNCTION_API_CHANGES: List[APIChange] = [
    APIChange(
        old_api="from pyspark.sql.functions import *",
        new_api="from pyspark.sql import functions as F",
        version="3.0",
        description="推荐使用别名导入而非 import *",
        category="import",
    ),
    APIChange(
        old_api="col('col').cast('string')",
        new_api="col('col').cast(StringType())",
        version="3.0",
        description="推荐使用类型对象而非字符串",
        auto_fixable=False,
        category="types",
        notes="需要导入对应的类型类",
    ),
    APIChange(
        old_api="col('col').cast('int')",
        new_api="col('col').cast(IntegerType())",
        version="3.0",
        description="推荐使用类型对象而非字符串",
        auto_fixable=False,
        category="types",
    ),
    APIChange(
        old_api="col('col').cast('double')",
        new_api="col('col').cast(DoubleType())",
        version="3.0",
        description="推荐使用类型对象而非字符串",
        auto_fixable=False,
        category="types",
    ),
]

# UDF 相关变更
UDF_API_CHANGES: List[APIChange] = [
    APIChange(
        old_api="@udf()",
        new_api="@udf(returnType=StringType())",
        version="3.0",
        description="UDF 需要显式指定返回类型",
        auto_fixable=False,
        category="udf",
        notes="根据实际返回类型修改",
    ),
    APIChange(
        old_api="PandasUDFType.SCALAR",
        new_api="'scalar'",
        version="3.0",
        description="pandas_udf 类型参数变更",
        category="udf",
    ),
    APIChange(
        old_api="PandasUDFType.GROUPED_MAP",
        new_api="'grouped_map'",
        version="3.0",
        description="pandas_udf GROUPED_MAP 变更",
        category="udf",
    ),
    APIChange(
        old_api="PandasUDFType.GROUPED_AGG",
        new_api="'grouped_agg'",
        version="3.0",
        description="pandas_udf GROUPED_AGG 变更",
        category="udf",
    ),
]

# 读写 API 变更
IO_API_CHANGES: List[APIChange] = [
    APIChange(
        old_api=".option('header', 'true')",
        new_api=".option('header', True)",
        version="3.0",
        description="布尔选项推荐使用布尔值",
        auto_fixable=False,
        category="io",
    ),
    APIChange(
        old_api=".option('inferSchema', 'true')",
        new_api=".option('inferSchema', True)",
        version="3.0",
        description="布尔选项推荐使用布尔值",
        auto_fixable=False,
        category="io",
    ),
    APIChange(
        old_api=".save(path)",
        new_api=".save(path)",
        version="3.0",
        description="save 方法在 Spark 3.x 中默认行为可能不同",
        auto_fixable=False,
        category="io",
        notes="检查 mode 参数是否正确设置",
    ),
]

# 窗口函数变更
WINDOW_API_CHANGES: List[APIChange] = [
    APIChange(
        old_api="Window.partitionBy(",
        new_api="Window.partitionBy(",
        version="3.0",
        description="窗口函数在 Spark 3.x 中行为一致",
        auto_fixable=False,
        category="window",
        notes="检查窗口边界定义",
    ),
]

# 汇总所有 API 变更
API_CHANGES: List[APIChange] = (
    SPARK_SESSION_CHANGES
    + DATAFRAME_API_CHANGES
    + FUNCTION_API_CHANGES
    + UDF_API_CHANGES
    + IO_API_CHANGES
    + WINDOW_API_CHANGES
)


def get_changes_by_category(category: str) -> List[APIChange]:
    """按类别获取 API 变更"""
    return [c for c in API_CHANGES if c.category == category]


def get_changes_by_version(version: str) -> List[APIChange]:
    """按版本获取 API 变更"""
    return [c for c in API_CHANGES if c.version <= version]


def get_auto_fixable_changes() -> List[APIChange]:
    """获取可自动修复的变更"""
    return [c for c in API_CHANGES if c.auto_fixable]
