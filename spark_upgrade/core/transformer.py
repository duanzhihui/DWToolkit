"""代码转换器 - 应用转换规则将 Spark 2.x 代码升级到 3.x"""

import ast
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from .parser import SparkCodeStructure


@dataclass
class TransformationRule:
    """转换规则"""
    name: str
    description: str
    pattern: str
    replacement: str
    priority: int = 0
    version: str = "3.0"
    auto_fixable: bool = True
    regex: bool = False
    condition: Optional[Callable[[str], bool]] = None


@dataclass
class TransformationChange:
    """单个转换变更"""
    rule_name: str
    line: int
    col: int
    original: str
    transformed: str
    description: str


@dataclass
class TransformationResult:
    """转换结果"""
    success: bool
    original_code: str
    transformed_code: str
    changes_applied: List[TransformationChange] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return len(self.changes_applied) > 0


class SparkTransformer:
    """Spark 代码转换器"""

    def __init__(self, target_version: str = "3.2"):
        self.target_version = target_version
        self.rules = self._load_rules()
        self._custom_rules: List[TransformationRule] = []

    def add_custom_rule(self, rule: TransformationRule) -> None:
        """添加自定义规则"""
        self._custom_rules.append(rule)

    def transform(self, code_structure: SparkCodeStructure) -> TransformationResult:
        """
        应用转换规则
        
        Args:
            code_structure: 解析后的代码结构
            
        Returns:
            TransformationResult 转换结果
        """
        code = code_structure.raw_code
        changes: List[TransformationChange] = []
        warnings: List[str] = []
        errors: List[str] = []

        # 合并内置规则和自定义规则
        all_rules = self.rules + self._custom_rules
        # 按优先级排序
        all_rules.sort(key=lambda r: r.priority, reverse=True)

        transformed_code = code

        for rule in all_rules:
            if not rule.auto_fixable:
                # 只记录警告，不自动修复
                if self._pattern_exists(transformed_code, rule):
                    warnings.append(
                        f"[{rule.name}] {rule.description} (需要手动处理)"
                    )
                continue

            try:
                new_code, rule_changes = self._apply_rule(
                    transformed_code, rule, code_structure
                )
                if rule_changes:
                    transformed_code = new_code
                    changes.extend(rule_changes)
            except Exception as e:
                errors.append(f"应用规则 '{rule.name}' 时出错: {str(e)}")

        # 处理弃用 API 警告
        for deprecated in code_structure.deprecated_apis:
            warnings.append(
                f"第 {deprecated['line']} 行: 使用了弃用的 API '{deprecated['deprecated_api']}', "
                f"建议替换为 '{deprecated['replacement']}'"
            )

        return TransformationResult(
            success=len(errors) == 0,
            original_code=code,
            transformed_code=transformed_code,
            changes_applied=changes,
            warnings=warnings,
            errors=errors,
        )

    def transform_code(self, code: str) -> TransformationResult:
        """
        直接转换代码字符串
        
        Args:
            code: Python 代码字符串
            
        Returns:
            TransformationResult 转换结果
        """
        from .parser import SparkParser

        parser = SparkParser()
        structure = parser.parse_code(code)
        return self.transform(structure)

    def _apply_rule(
        self,
        code: str,
        rule: TransformationRule,
        structure: SparkCodeStructure,
    ) -> Tuple[str, List[TransformationChange]]:
        """应用单个规则"""
        changes: List[TransformationChange] = []

        if rule.condition and not rule.condition(code):
            return code, changes

        if rule.regex:
            # 使用正则表达式替换
            new_code, count = self._regex_replace(code, rule)
            if count > 0:
                changes.append(TransformationChange(
                    rule_name=rule.name,
                    line=0,  # 正则替换可能跨多行
                    col=0,
                    original=rule.pattern,
                    transformed=rule.replacement,
                    description=f"{rule.description} ({count} 处)",
                ))
        else:
            # 使用字符串替换
            new_code, count = self._string_replace(code, rule)
            if count > 0:
                changes.append(TransformationChange(
                    rule_name=rule.name,
                    line=0,
                    col=0,
                    original=rule.pattern,
                    transformed=rule.replacement,
                    description=f"{rule.description} ({count} 处)",
                ))

        return new_code, changes

    def _regex_replace(
        self, code: str, rule: TransformationRule
    ) -> Tuple[str, int]:
        """使用正则表达式替换"""
        pattern = re.compile(rule.pattern)
        new_code, count = pattern.subn(rule.replacement, code)
        return new_code, count

    def _string_replace(
        self, code: str, rule: TransformationRule
    ) -> Tuple[str, int]:
        """使用字符串替换"""
        count = code.count(rule.pattern)
        if count > 0:
            new_code = code.replace(rule.pattern, rule.replacement)
            return new_code, count
        return code, 0

    def _pattern_exists(self, code: str, rule: TransformationRule) -> bool:
        """检查模式是否存在于代码中"""
        if rule.regex:
            return bool(re.search(rule.pattern, code))
        return rule.pattern in code

    def _load_rules(self) -> List[TransformationRule]:
        """加载转换规则"""
        rules = []

        # === 导入相关规则 ===
        rules.extend([
            TransformationRule(
                name="import_functions_alias",
                description="规范化 pyspark.sql.functions 导入",
                pattern="from pyspark.sql.functions import *",
                replacement="from pyspark.sql import functions as F",
                priority=100,
                version="3.0",
            ),
            TransformationRule(
                name="import_spark_context_deprecated",
                description="SparkContext 导入提示",
                pattern="from pyspark import SparkContext",
                replacement="from pyspark.sql import SparkSession",
                priority=100,
                version="3.0",
            ),
            TransformationRule(
                name="import_sql_context_deprecated",
                description="SQLContext 已弃用，使用 SparkSession",
                pattern="from pyspark.sql import SQLContext",
                replacement="from pyspark.sql import SparkSession",
                priority=100,
                version="3.0",
            ),
            TransformationRule(
                name="import_hive_context_deprecated",
                description="HiveContext 已弃用，使用 SparkSession",
                pattern="from pyspark.sql import HiveContext",
                replacement="from pyspark.sql import SparkSession",
                priority=100,
                version="3.0",
            ),
        ])

        # === SparkSession 相关规则 ===
        rules.extend([
            TransformationRule(
                name="spark_context_to_session",
                description="SparkContext() 替换为 SparkSession",
                pattern=r"sc\s*=\s*SparkContext\(\)",
                replacement="spark = SparkSession.builder.getOrCreate()\nsc = spark.sparkContext",
                priority=90,
                version="3.0",
                regex=True,
            ),
            TransformationRule(
                name="sql_context_to_session",
                description="SQLContext 替换为 SparkSession",
                pattern=r"SQLContext\(sc\)",
                replacement="SparkSession.builder.getOrCreate()",
                priority=90,
                version="3.0",
                regex=True,
            ),
            TransformationRule(
                name="hive_context_to_session",
                description="HiveContext 替换为 SparkSession",
                pattern=r"HiveContext\(sc\)",
                replacement="SparkSession.builder.enableHiveSupport().getOrCreate()",
                priority=90,
                version="3.0",
                regex=True,
            ),
        ])

        # === DataFrame API 变更 ===
        rules.extend([
            TransformationRule(
                name="register_temp_table",
                description="registerTempTable 已弃用",
                pattern=".registerTempTable(",
                replacement=".createOrReplaceTempView(",
                priority=80,
                version="3.0",
            ),
            TransformationRule(
                name="union_all_deprecated",
                description="unionAll 已弃用，使用 union",
                pattern=".unionAll(",
                replacement=".union(",
                priority=80,
                version="3.0",
            ),
            TransformationRule(
                name="na_fill_to_fillna",
                description="na.fill 替换为 fillna",
                pattern=".na.fill(",
                replacement=".fillna(",
                priority=70,
                version="3.0",
            ),
            TransformationRule(
                name="na_drop_to_dropna",
                description="na.drop 替换为 dropna",
                pattern=".na.drop(",
                replacement=".dropna(",
                priority=70,
                version="3.0",
            ),
        ])

        # === 配置相关规则 ===
        rules.extend([
            TransformationRule(
                name="shuffle_partitions_aqe",
                description="推荐启用自适应查询执行",
                pattern=r"\.conf\.set\(['\"]spark\.sql\.shuffle\.partitions['\"],\s*['\"]?\d+['\"]?\)",
                replacement=".conf.set('spark.sql.adaptive.enabled', 'true')",
                priority=60,
                version="3.0",
                regex=True,
                auto_fixable=False,  # 需要手动确认
            ),
            TransformationRule(
                name="pandas_arrow_config",
                description="启用 Arrow 优化 Pandas 转换",
                pattern="spark.conf.set('spark.sql.execution.arrow.enabled', 'true')",
                replacement="spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', 'true')",
                priority=60,
                version="3.0",
            ),
        ])

        # === 类型转换规则 ===
        rules.extend([
            TransformationRule(
                name="cast_string_type",
                description="使用 StringType() 替代字符串",
                pattern=r"\.cast\(['\"]string['\"]\)",
                replacement=".cast(StringType())",
                priority=50,
                version="3.0",
                regex=True,
                auto_fixable=False,  # 需要确保导入了 StringType
            ),
            TransformationRule(
                name="cast_integer_type",
                description="使用 IntegerType() 替代字符串",
                pattern=r"\.cast\(['\"]int['\"]\)",
                replacement=".cast(IntegerType())",
                priority=50,
                version="3.0",
                regex=True,
                auto_fixable=False,
            ),
        ])

        # === 函数调用规则 ===
        rules.extend([
            TransformationRule(
                name="udf_return_type",
                description="UDF 需要显式指定返回类型",
                pattern=r"@udf\(\)",
                replacement="@udf(returnType=StringType())",
                priority=40,
                version="3.0",
                regex=True,
                auto_fixable=False,
            ),
            TransformationRule(
                name="pandas_udf_type",
                description="pandas_udf 类型参数变更",
                pattern="PandasUDFType.SCALAR",
                replacement="'scalar'",
                priority=40,
                version="3.0",
            ),
            TransformationRule(
                name="pandas_udf_grouped_map",
                description="pandas_udf GROUPED_MAP 变更",
                pattern="PandasUDFType.GROUPED_MAP",
                replacement="'grouped_map'",
                priority=40,
                version="3.0",
            ),
        ])

        # === RDD 操作警告 ===
        rules.extend([
            TransformationRule(
                name="rdd_map_warning",
                description="RDD map 操作，建议使用 DataFrame API",
                pattern=r"\.rdd\.map\(",
                replacement=".rdd.map(",  # 不替换，只警告
                priority=30,
                version="3.0",
                regex=True,
                auto_fixable=False,
            ),
            TransformationRule(
                name="rdd_flatmap_warning",
                description="RDD flatMap 操作，建议使用 DataFrame API",
                pattern=r"\.rdd\.flatMap\(",
                replacement=".rdd.flatMap(",
                priority=30,
                version="3.0",
                regex=True,
                auto_fixable=False,
            ),
        ])

        # === Spark 3.x 新特性提示 ===
        rules.extend([
            TransformationRule(
                name="aqe_recommendation",
                description="建议启用自适应查询执行 (AQE)",
                pattern="spark.sql.adaptive.enabled",
                replacement="spark.sql.adaptive.enabled",
                priority=20,
                version="3.0",
                auto_fixable=False,
            ),
        ])

        return rules

    def get_rules_for_version(self, version: str) -> List[TransformationRule]:
        """获取指定版本的规则"""
        all_rules = self.rules + self._custom_rules
        return [r for r in all_rules if r.version <= version]

    def preview_changes(self, code_structure: SparkCodeStructure) -> Dict[str, Any]:
        """
        预览将要进行的变更
        
        Args:
            code_structure: 解析后的代码结构
            
        Returns:
            变更预览信息
        """
        result = self.transform(code_structure)

        return {
            "total_changes": len(result.changes_applied),
            "changes": [
                {
                    "rule": c.rule_name,
                    "description": c.description,
                    "original": c.original,
                    "replacement": c.transformed,
                }
                for c in result.changes_applied
            ],
            "warnings": result.warnings,
            "errors": result.errors,
        }
