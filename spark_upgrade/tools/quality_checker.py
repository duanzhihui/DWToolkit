"""代码质量检查器 - 集成 ruff 进行代码质量检查"""

import json
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class QualityIssue:
    """代码质量问题"""
    line: int
    column: int
    severity: str  # error, warning, info
    message: str
    code: str
    fixable: bool = False
    end_line: Optional[int] = None
    end_column: Optional[int] = None


@dataclass
class QualityReport:
    """代码质量报告"""
    file_path: str
    issues: List[QualityIssue] = field(default_factory=list)
    score: float = 100.0
    fixable_count: int = 0
    error_count: int = 0
    warning_count: int = 0
    info_count: int = 0

    @property
    def total_issues(self) -> int:
        return len(self.issues)

    @property
    def has_issues(self) -> bool:
        return len(self.issues) > 0


class QualityChecker:
    """代码质量检查器"""

    # ruff 规则严重程度映射
    SEVERITY_MAP = {
        "E": "error",    # pycodestyle errors
        "W": "warning",  # pycodestyle warnings
        "F": "error",    # Pyflakes
        "I": "info",     # isort
        "N": "info",     # pep8-naming
        "UP": "info",    # pyupgrade
        "B": "warning",  # flake8-bugbear
        "C": "info",     # flake8-comprehensions
        "S": "warning",  # flake8-bandit (security)
    }

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path
        self._ruff_available = self._check_ruff_available()

    def _check_ruff_available(self) -> bool:
        """检查 ruff 是否可用"""
        try:
            result = subprocess.run(
                [sys.executable, "-m", "ruff", "--version"],
                capture_output=True,
                text=True,
            )
            return result.returncode == 0
        except (subprocess.SubprocessError, FileNotFoundError):
            return False

    def check_file(self, file_path: str) -> QualityReport:
        """
        使用 ruff 检查代码质量
        
        Args:
            file_path: 文件路径
            
        Returns:
            QualityReport 质量报告
        """
        path = Path(file_path)

        if not path.exists():
            return QualityReport(
                file_path=file_path,
                issues=[
                    QualityIssue(
                        line=0,
                        column=0,
                        severity="error",
                        message=f"文件不存在: {file_path}",
                        code="FILE_NOT_FOUND",
                    )
                ],
                score=0.0,
            )

        if not self._ruff_available:
            return QualityReport(
                file_path=file_path,
                issues=[
                    QualityIssue(
                        line=0,
                        column=0,
                        severity="warning",
                        message="ruff 未安装，跳过代码质量检查",
                        code="RUFF_NOT_AVAILABLE",
                    )
                ],
                score=100.0,
            )

        # 运行 ruff check
        result = self._run_ruff(["check", file_path, "--output-format=json"])

        issues: List[QualityIssue] = []
        fixable_count = 0
        error_count = 0
        warning_count = 0
        info_count = 0

        if result.stdout:
            try:
                ruff_issues = json.loads(result.stdout)

                for item in ruff_issues:
                    code = item.get("code", "")
                    severity = self._get_severity(code)
                    fixable = item.get("fix") is not None

                    issue = QualityIssue(
                        line=item.get("location", {}).get("row", 0),
                        column=item.get("location", {}).get("column", 0),
                        severity=severity,
                        message=item.get("message", ""),
                        code=code,
                        fixable=fixable,
                        end_line=item.get("end_location", {}).get("row"),
                        end_column=item.get("end_location", {}).get("column"),
                    )
                    issues.append(issue)

                    if fixable:
                        fixable_count += 1
                    if severity == "error":
                        error_count += 1
                    elif severity == "warning":
                        warning_count += 1
                    else:
                        info_count += 1

            except json.JSONDecodeError:
                pass

        # 计算质量分数
        score = self._calculate_score(issues)

        return QualityReport(
            file_path=file_path,
            issues=issues,
            score=score,
            fixable_count=fixable_count,
            error_count=error_count,
            warning_count=warning_count,
            info_count=info_count,
        )

    def check_code(self, code: str, file_name: str = "temp.py") -> QualityReport:
        """
        检查代码字符串的质量
        
        Args:
            code: 代码字符串
            file_name: 临时文件名
            
        Returns:
            QualityReport 质量报告
        """
        import tempfile

        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".py",
            delete=False,
            encoding="utf-8",
        ) as f:
            f.write(code)
            temp_path = f.name

        try:
            report = self.check_file(temp_path)
            report.file_path = file_name
            return report
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def fix_file(
        self,
        file_path: str,
        fix_only: Optional[List[str]] = None,
        unsafe_fixes: bool = False,
    ) -> bool:
        """
        使用 ruff 自动修复代码
        
        Args:
            file_path: 文件路径
            fix_only: 只修复指定的规则代码
            unsafe_fixes: 是否应用不安全的修复
            
        Returns:
            是否成功
        """
        if not self._ruff_available:
            return False

        args = ["check", file_path, "--fix"]

        if fix_only:
            for code in fix_only:
                args.extend(["--select", code])

        if unsafe_fixes:
            args.append("--unsafe-fixes")

        result = self._run_ruff(args)
        return result.returncode == 0

    def format_file(self, file_path: str) -> bool:
        """
        使用 ruff 格式化代码
        
        Args:
            file_path: 文件路径
            
        Returns:
            是否成功
        """
        if not self._ruff_available:
            return False

        result = self._run_ruff(["format", file_path])
        return result.returncode == 0

    def _run_ruff(self, args: List[str]) -> subprocess.CompletedProcess:
        """执行 ruff 命令"""
        cmd = [sys.executable, "-m", "ruff"] + args

        if self.config_path:
            cmd.extend(["--config", self.config_path])

        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )

    def _get_severity(self, code: str) -> str:
        """根据规则代码获取严重程度"""
        if not code:
            return "info"

        # 获取规则前缀
        prefix = ""
        for char in code:
            if char.isalpha():
                prefix += char
            else:
                break

        return self.SEVERITY_MAP.get(prefix, "info")

    def _calculate_score(self, issues: List[QualityIssue]) -> float:
        """
        计算质量分数
        
        分数范围: 0.0 - 100.0
        - error: -10
        - warning: -5
        - info: -1
        """
        score = 100.0

        for issue in issues:
            if issue.severity == "error":
                score -= 10
            elif issue.severity == "warning":
                score -= 5
            else:
                score -= 1

        return max(0.0, score)

    def get_summary(self, report: QualityReport) -> Dict[str, Any]:
        """
        获取质量报告摘要
        
        Args:
            report: 质量报告
            
        Returns:
            摘要字典
        """
        return {
            "file_path": report.file_path,
            "score": f"{report.score:.1f}",
            "total_issues": report.total_issues,
            "error_count": report.error_count,
            "warning_count": report.warning_count,
            "info_count": report.info_count,
            "fixable_count": report.fixable_count,
            "has_issues": report.has_issues,
        }

    def get_fixable_issues(self, report: QualityReport) -> List[QualityIssue]:
        """获取可自动修复的问题"""
        return [i for i in report.issues if i.fixable]

    def is_available(self) -> bool:
        """检查 ruff 是否可用"""
        return self._ruff_available
