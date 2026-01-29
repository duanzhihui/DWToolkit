"""验证器 - 验证转换后的代码"""

import ast
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class ValidationIssue:
    """验证问题"""
    severity: str  # error, warning, info
    message: str
    line: Optional[int] = None
    col: Optional[int] = None
    code: Optional[str] = None


@dataclass
class ValidationResult:
    """验证结果"""
    valid: bool
    syntax_valid: bool
    issues: List[ValidationIssue] = field(default_factory=list)
    compatibility_score: float = 1.0

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "warning")


class SparkValidator:
    """Spark 代码验证器"""

    # Spark 3.x 兼容性检查项
    COMPATIBILITY_CHECKS = [
        {
            "pattern": "SparkContext()",
            "message": "直接使用 SparkContext 在 Spark 3.x 中不推荐",
            "severity": "warning",
        },
        {
            "pattern": "SQLContext",
            "message": "SQLContext 在 Spark 3.x 中已弃用",
            "severity": "warning",
        },
        {
            "pattern": "HiveContext",
            "message": "HiveContext 在 Spark 3.x 中已弃用",
            "severity": "warning",
        },
        {
            "pattern": ".registerTempTable(",
            "message": "registerTempTable 在 Spark 3.x 中已弃用",
            "severity": "warning",
        },
        {
            "pattern": ".unionAll(",
            "message": "unionAll 在 Spark 3.x 中已弃用，使用 union",
            "severity": "warning",
        },
        {
            "pattern": "from pyspark.sql.functions import *",
            "message": "不推荐使用 import *，建议使用 import functions as F",
            "severity": "info",
        },
        {
            "pattern": ".rdd.map(",
            "message": "RDD 操作性能较低，建议使用 DataFrame API",
            "severity": "info",
        },
        {
            "pattern": ".rdd.flatMap(",
            "message": "RDD 操作性能较低，建议使用 DataFrame API",
            "severity": "info",
        },
        {
            "pattern": ".toPandas()",
            "message": "toPandas() 可能导致内存问题，确保数据量可控",
            "severity": "info",
        },
        {
            "pattern": "PandasUDFType",
            "message": "PandasUDFType 在 Spark 3.x 中已变更",
            "severity": "warning",
        },
    ]

    def __init__(self, target_version: str = "3.2"):
        self.target_version = target_version

    def validate(self, code: str, file_path: str = "<string>") -> ValidationResult:
        """
        验证代码
        
        Args:
            code: Python 代码字符串
            file_path: 文件路径（用于错误报告）
            
        Returns:
            ValidationResult 验证结果
        """
        issues: List[ValidationIssue] = []

        # 1. 语法验证
        syntax_valid, syntax_issues = self._validate_syntax(code, file_path)
        issues.extend(syntax_issues)

        if not syntax_valid:
            return ValidationResult(
                valid=False,
                syntax_valid=False,
                issues=issues,
                compatibility_score=0.0,
            )

        # 2. 兼容性检查
        compat_issues = self._check_compatibility(code)
        issues.extend(compat_issues)

        # 3. 计算兼容性分数
        compatibility_score = self._calculate_compatibility_score(issues)

        # 4. 确定是否有效（没有错误级别的问题）
        valid = all(i.severity != "error" for i in issues)

        return ValidationResult(
            valid=valid,
            syntax_valid=True,
            issues=issues,
            compatibility_score=compatibility_score,
        )

    def validate_file(self, file_path: str) -> ValidationResult:
        """
        验证文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            ValidationResult 验证结果
        """
        path = Path(file_path)

        if not path.exists():
            return ValidationResult(
                valid=False,
                syntax_valid=False,
                issues=[
                    ValidationIssue(
                        severity="error",
                        message=f"文件不存在: {file_path}",
                    )
                ],
                compatibility_score=0.0,
            )

        with open(path, "r", encoding="utf-8") as f:
            code = f.read()

        return self.validate(code, file_path)

    def _validate_syntax(
        self, code: str, file_path: str
    ) -> tuple[bool, List[ValidationIssue]]:
        """验证 Python 语法"""
        issues: List[ValidationIssue] = []

        try:
            ast.parse(code, filename=file_path)
            return True, issues
        except SyntaxError as e:
            issues.append(
                ValidationIssue(
                    severity="error",
                    message=f"语法错误: {e.msg}",
                    line=e.lineno,
                    col=e.offset,
                    code="E001",
                )
            )
            return False, issues

    def _check_compatibility(self, code: str) -> List[ValidationIssue]:
        """检查 Spark 3.x 兼容性"""
        issues: List[ValidationIssue] = []
        lines = code.split("\n")

        for check in self.COMPATIBILITY_CHECKS:
            pattern = check["pattern"]
            for line_num, line in enumerate(lines, 1):
                if pattern in line:
                    issues.append(
                        ValidationIssue(
                            severity=check["severity"],
                            message=check["message"],
                            line=line_num,
                            col=line.find(pattern) + 1,
                        )
                    )

        return issues

    def _calculate_compatibility_score(
        self, issues: List[ValidationIssue]
    ) -> float:
        """
        计算兼容性分数
        
        分数范围: 0.0 - 1.0
        - error: -0.3
        - warning: -0.1
        - info: -0.02
        """
        score = 1.0

        for issue in issues:
            if issue.severity == "error":
                score -= 0.3
            elif issue.severity == "warning":
                score -= 0.1
            elif issue.severity == "info":
                score -= 0.02

        return max(0.0, min(1.0, score))

    def check_imports(self, code: str) -> List[ValidationIssue]:
        """检查导入语句"""
        issues: List[ValidationIssue] = []

        try:
            tree = ast.parse(code)
        except SyntaxError:
            return issues

        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.module and "pyspark" in node.module:
                    for alias in node.names:
                        if alias.name == "*":
                            issues.append(
                                ValidationIssue(
                                    severity="warning",
                                    message=f"不推荐使用 'from {node.module} import *'",
                                    line=node.lineno,
                                    col=node.col_offset,
                                )
                            )

        return issues

    def check_ruff(self, file_path: str) -> List[ValidationIssue]:
        """
        使用 ruff 检查代码质量
        
        Args:
            file_path: 文件路径
            
        Returns:
            验证问题列表
        """
        issues: List[ValidationIssue] = []

        try:
            result = subprocess.run(
                [sys.executable, "-m", "ruff", "check", file_path, "--output-format=json"],
                capture_output=True,
                text=True,
            )

            if result.stdout:
                import json
                ruff_issues = json.loads(result.stdout)

                for item in ruff_issues:
                    issues.append(
                        ValidationIssue(
                            severity="warning",
                            message=item.get("message", ""),
                            line=item.get("location", {}).get("row"),
                            col=item.get("location", {}).get("column"),
                            code=item.get("code"),
                        )
                    )
        except (subprocess.SubprocessError, FileNotFoundError):
            # ruff 未安装或执行失败
            pass
        except Exception:
            pass

        return issues

    def get_summary(self, result: ValidationResult) -> Dict[str, Any]:
        """
        获取验证结果摘要
        
        Args:
            result: 验证结果
            
        Returns:
            摘要字典
        """
        return {
            "valid": result.valid,
            "syntax_valid": result.syntax_valid,
            "compatibility_score": f"{result.compatibility_score:.1%}",
            "error_count": result.error_count,
            "warning_count": result.warning_count,
            "total_issues": len(result.issues),
        }
