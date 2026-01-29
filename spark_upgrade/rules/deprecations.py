"""弃用 API 处理规则"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class DeprecatedAPI:
    """弃用 API 定义"""
    pattern: str
    replacement: str
    version: str
    description: str
    severity: str = "warning"  # warning, error, info
    auto_fixable: bool = False
    category: str = "general"
    migration_guide: Optional[str] = None


# SparkContext 相关弃用
SPARK_CONTEXT_DEPRECATIONS: List[DeprecatedAPI] = [
    DeprecatedAPI(
        pattern=r"SparkContext\(\)",
        replacement="SparkSession.builder.getOrCreate()",
        version="3.0",
        description="SparkContext 直接创建已弃用，使用 SparkSession",
        severity="warning",
        category="context",
        migration_guide="""
将:
    sc = SparkContext()
替换为:
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
""",
    ),
    DeprecatedAPI(
        pattern=r"SQLContext\(",
        replacement="SparkSession.builder.getOrCreate()",
        version="3.0",
        description="SQLContext 已弃用，使用 SparkSession",
        severity="warning",
        category="context",
        migration_guide="""
将:
    sqlContext = SQLContext(sc)
替换为:
    spark = SparkSession.builder.getOrCreate()
""",
    ),
    DeprecatedAPI(
        pattern=r"HiveContext\(",
        replacement="SparkSession.builder.enableHiveSupport().getOrCreate()",
        version="3.0",
        description="HiveContext 已弃用，使用 SparkSession",
        severity="warning",
        category="context",
        migration_guide="""
将:
    hiveContext = HiveContext(sc)
替换为:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
""",
    ),
]

# RDD 相关弃用
RDD_DEPRECATIONS: List[DeprecatedAPI] = [
    DeprecatedAPI(
        pattern=r"\.rdd\.map\(",
        replacement=".select(F.expr(...))",
        version="3.0",
        description="RDD map 操作性能较低，推荐使用 DataFrame API",
        severity="info",
        category="rdd",
        migration_guide="""
将 RDD map 操作替换为 DataFrame 操作:
    df.rdd.map(lambda x: x.col1 + x.col2)
替换为:
    df.select(F.col('col1') + F.col('col2'))
""",
    ),
    DeprecatedAPI(
        pattern=r"\.rdd\.flatMap\(",
        replacement=".select(F.explode(...))",
        version="3.0",
        description="RDD flatMap 操作性能较低，推荐使用 DataFrame API",
        severity="info",
        category="rdd",
        migration_guide="""
将 RDD flatMap 操作替换为 DataFrame 操作:
    df.rdd.flatMap(lambda x: x.array_col)
替换为:
    df.select(F.explode('array_col'))
""",
    ),
    DeprecatedAPI(
        pattern=r"\.rdd\.filter\(",
        replacement=".filter(...)",
        version="3.0",
        description="RDD filter 操作性能较低，推荐使用 DataFrame API",
        severity="info",
        category="rdd",
        migration_guide="""
将 RDD filter 操作替换为 DataFrame 操作:
    df.rdd.filter(lambda x: x.col1 > 10)
替换为:
    df.filter(F.col('col1') > 10)
""",
    ),
    DeprecatedAPI(
        pattern=r"\.rdd\.reduce\(",
        replacement=".agg(...)",
        version="3.0",
        description="RDD reduce 操作性能较低，推荐使用 DataFrame API",
        severity="info",
        category="rdd",
    ),
]

# DataFrame API 弃用
DATAFRAME_DEPRECATIONS: List[DeprecatedAPI] = [
    DeprecatedAPI(
        pattern=r"\.registerTempTable\(",
        replacement=".createOrReplaceTempView(",
        version="3.0",
        description="registerTempTable 已弃用",
        severity="warning",
        auto_fixable=True,
        category="dataframe",
    ),
    DeprecatedAPI(
        pattern=r"\.unionAll\(",
        replacement=".union(",
        version="3.0",
        description="unionAll 已弃用，使用 union",
        severity="warning",
        auto_fixable=True,
        category="dataframe",
    ),
    DeprecatedAPI(
        pattern=r"\.toPandas\(\)",
        replacement=".toPandas()",
        version="3.0",
        description="toPandas() 可能导致内存问题，确保数据量可控",
        severity="info",
        auto_fixable=False,
        category="dataframe",
        migration_guide="""
使用 toPandas() 时注意:
1. 确保数据量足够小，可以放入驱动内存
2. 考虑使用 spark.sql.execution.arrow.pyspark.enabled=true 提升性能
3. 对于大数据集，考虑使用采样或分批处理
""",
    ),
]

# UDF 相关弃用
UDF_DEPRECATIONS: List[DeprecatedAPI] = [
    DeprecatedAPI(
        pattern=r"PandasUDFType\.SCALAR",
        replacement="'scalar'",
        version="3.0",
        description="PandasUDFType.SCALAR 已弃用",
        severity="warning",
        auto_fixable=True,
        category="udf",
    ),
    DeprecatedAPI(
        pattern=r"PandasUDFType\.GROUPED_MAP",
        replacement="'grouped_map'",
        version="3.0",
        description="PandasUDFType.GROUPED_MAP 已弃用",
        severity="warning",
        auto_fixable=True,
        category="udf",
    ),
    DeprecatedAPI(
        pattern=r"PandasUDFType\.GROUPED_AGG",
        replacement="'grouped_agg'",
        version="3.0",
        description="PandasUDFType.GROUPED_AGG 已弃用",
        severity="warning",
        auto_fixable=True,
        category="udf",
    ),
    DeprecatedAPI(
        pattern=r"@udf\(\s*\)",
        replacement="@udf(returnType=...)",
        version="3.0",
        description="UDF 需要显式指定返回类型",
        severity="warning",
        auto_fixable=False,
        category="udf",
        migration_guide="""
UDF 必须指定返回类型:
    @udf()
    def my_udf(x):
        return str(x)
替换为:
    @udf(returnType=StringType())
    def my_udf(x):
        return str(x)
""",
    ),
]

# 导入相关弃用
IMPORT_DEPRECATIONS: List[DeprecatedAPI] = [
    DeprecatedAPI(
        pattern=r"from pyspark\.sql\.functions import \*",
        replacement="from pyspark.sql import functions as F",
        version="3.0",
        description="不推荐使用 import *",
        severity="info",
        auto_fixable=True,
        category="import",
        migration_guide="""
将:
    from pyspark.sql.functions import *
替换为:
    from pyspark.sql import functions as F
    
然后将函数调用从:
    col('name')
改为:
    F.col('name')
""",
    ),
    DeprecatedAPI(
        pattern=r"from pyspark import SparkContext",
        replacement="from pyspark.sql import SparkSession",
        version="3.0",
        description="推荐使用 SparkSession",
        severity="info",
        category="import",
    ),
]

# 汇总所有弃用 API
DEPRECATED_APIS: List[DeprecatedAPI] = (
    SPARK_CONTEXT_DEPRECATIONS
    + RDD_DEPRECATIONS
    + DATAFRAME_DEPRECATIONS
    + UDF_DEPRECATIONS
    + IMPORT_DEPRECATIONS
)


def get_deprecations_by_category(category: str) -> List[DeprecatedAPI]:
    """按类别获取弃用 API"""
    return [d for d in DEPRECATED_APIS if d.category == category]


def get_deprecations_by_severity(severity: str) -> List[DeprecatedAPI]:
    """按严重程度获取弃用 API"""
    return [d for d in DEPRECATED_APIS if d.severity == severity]


def get_auto_fixable_deprecations() -> List[DeprecatedAPI]:
    """获取可自动修复的弃用 API"""
    return [d for d in DEPRECATED_APIS if d.auto_fixable]
