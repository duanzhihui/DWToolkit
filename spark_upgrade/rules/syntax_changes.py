"""语法变更规则 - Spark 2.x 到 3.x 的语法变更"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class SyntaxChange:
    """语法变更定义"""
    pattern: str
    replacement: str
    version: str
    description: str
    is_regex: bool = False
    auto_fixable: bool = True
    category: str = "general"
    notes: Optional[str] = None


# 类型转换语法变更
TYPE_CAST_CHANGES: List[SyntaxChange] = [
    SyntaxChange(
        pattern=r"\.cast\(['\"]string['\"]\)",
        replacement=".cast(StringType())",
        version="3.0",
        description="推荐使用类型对象进行类型转换",
        is_regex=True,
        auto_fixable=False,
        category="types",
        notes="需要导入 from pyspark.sql.types import StringType",
    ),
    SyntaxChange(
        pattern=r"\.cast\(['\"]int['\"]\)",
        replacement=".cast(IntegerType())",
        version="3.0",
        description="推荐使用类型对象进行类型转换",
        is_regex=True,
        auto_fixable=False,
        category="types",
        notes="需要导入 from pyspark.sql.types import IntegerType",
    ),
    SyntaxChange(
        pattern=r"\.cast\(['\"]integer['\"]\)",
        replacement=".cast(IntegerType())",
        version="3.0",
        description="推荐使用类型对象进行类型转换",
        is_regex=True,
        auto_fixable=False,
        category="types",
    ),
    SyntaxChange(
        pattern=r"\.cast\(['\"]long['\"]\)",
        replacement=".cast(LongType())",
        version="3.0",
        description="推荐使用类型对象进行类型转换",
        is_regex=True,
        auto_fixable=False,
        category="types",
    ),
    SyntaxChange(
        pattern=r"\.cast\(['\"]double['\"]\)",
        replacement=".cast(DoubleType())",
        version="3.0",
        description="推荐使用类型对象进行类型转换",
        is_regex=True,
        auto_fixable=False,
        category="types",
    ),
    SyntaxChange(
        pattern=r"\.cast\(['\"]float['\"]\)",
        replacement=".cast(FloatType())",
        version="3.0",
        description="推荐使用类型对象进行类型转换",
        is_regex=True,
        auto_fixable=False,
        category="types",
    ),
    SyntaxChange(
        pattern=r"\.cast\(['\"]boolean['\"]\)",
        replacement=".cast(BooleanType())",
        version="3.0",
        description="推荐使用类型对象进行类型转换",
        is_regex=True,
        auto_fixable=False,
        category="types",
    ),
    SyntaxChange(
        pattern=r"\.cast\(['\"]timestamp['\"]\)",
        replacement=".cast(TimestampType())",
        version="3.0",
        description="推荐使用类型对象进行类型转换",
        is_regex=True,
        auto_fixable=False,
        category="types",
    ),
    SyntaxChange(
        pattern=r"\.cast\(['\"]date['\"]\)",
        replacement=".cast(DateType())",
        version="3.0",
        description="推荐使用类型对象进行类型转换",
        is_regex=True,
        auto_fixable=False,
        category="types",
    ),
]

# 函数调用语法变更
FUNCTION_CALL_CHANGES: List[SyntaxChange] = [
    SyntaxChange(
        pattern="col(",
        replacement="F.col(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="functions",
        notes="需要先将导入改为 import functions as F",
    ),
    SyntaxChange(
        pattern="lit(",
        replacement="F.lit(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="functions",
    ),
    SyntaxChange(
        pattern="when(",
        replacement="F.when(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="functions",
    ),
    SyntaxChange(
        pattern="sum(",
        replacement="F.sum(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="functions",
        notes="注意不要替换 Python 内置的 sum()",
    ),
    SyntaxChange(
        pattern="count(",
        replacement="F.count(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="functions",
    ),
]

# 日期时间函数变更
DATETIME_FUNCTION_CHANGES: List[SyntaxChange] = [
    SyntaxChange(
        pattern="from_unixtime(",
        replacement="F.from_unixtime(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="datetime",
    ),
    SyntaxChange(
        pattern="unix_timestamp(",
        replacement="F.unix_timestamp(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="datetime",
    ),
    SyntaxChange(
        pattern="to_date(",
        replacement="F.to_date(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="datetime",
    ),
    SyntaxChange(
        pattern="to_timestamp(",
        replacement="F.to_timestamp(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="datetime",
    ),
    SyntaxChange(
        pattern="date_format(",
        replacement="F.date_format(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="datetime",
    ),
]

# 字符串函数变更
STRING_FUNCTION_CHANGES: List[SyntaxChange] = [
    SyntaxChange(
        pattern="concat(",
        replacement="F.concat(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="string",
    ),
    SyntaxChange(
        pattern="concat_ws(",
        replacement="F.concat_ws(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="string",
    ),
    SyntaxChange(
        pattern="substring(",
        replacement="F.substring(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="string",
    ),
    SyntaxChange(
        pattern="trim(",
        replacement="F.trim(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="string",
    ),
    SyntaxChange(
        pattern="lower(",
        replacement="F.lower(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="string",
    ),
    SyntaxChange(
        pattern="upper(",
        replacement="F.upper(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="string",
    ),
]

# 聚合函数变更
AGGREGATE_FUNCTION_CHANGES: List[SyntaxChange] = [
    SyntaxChange(
        pattern="avg(",
        replacement="F.avg(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="aggregate",
    ),
    SyntaxChange(
        pattern="max(",
        replacement="F.max(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="aggregate",
        notes="注意不要替换 Python 内置的 max()",
    ),
    SyntaxChange(
        pattern="min(",
        replacement="F.min(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="aggregate",
        notes="注意不要替换 Python 内置的 min()",
    ),
    SyntaxChange(
        pattern="first(",
        replacement="F.first(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="aggregate",
    ),
    SyntaxChange(
        pattern="last(",
        replacement="F.last(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="aggregate",
    ),
]

# 窗口函数语法变更
WINDOW_FUNCTION_CHANGES: List[SyntaxChange] = [
    SyntaxChange(
        pattern="row_number(",
        replacement="F.row_number(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="window",
    ),
    SyntaxChange(
        pattern="rank(",
        replacement="F.rank(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="window",
    ),
    SyntaxChange(
        pattern="dense_rank(",
        replacement="F.dense_rank(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="window",
    ),
    SyntaxChange(
        pattern="lag(",
        replacement="F.lag(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="window",
    ),
    SyntaxChange(
        pattern="lead(",
        replacement="F.lead(",
        version="3.0",
        description="使用完整的函数引用",
        auto_fixable=False,
        category="window",
    ),
]

# 汇总所有语法变更
SYNTAX_CHANGES: List[SyntaxChange] = (
    TYPE_CAST_CHANGES
    + FUNCTION_CALL_CHANGES
    + DATETIME_FUNCTION_CHANGES
    + STRING_FUNCTION_CHANGES
    + AGGREGATE_FUNCTION_CHANGES
    + WINDOW_FUNCTION_CHANGES
)


def get_changes_by_category(category: str) -> List[SyntaxChange]:
    """按类别获取语法变更"""
    return [c for c in SYNTAX_CHANGES if c.category == category]


def get_auto_fixable_changes() -> List[SyntaxChange]:
    """获取可自动修复的语法变更"""
    return [c for c in SYNTAX_CHANGES if c.auto_fixable]


def get_regex_changes() -> List[SyntaxChange]:
    """获取使用正则表达式的语法变更"""
    return [c for c in SYNTAX_CHANGES if c.is_regex]
