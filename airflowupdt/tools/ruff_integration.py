# -*- coding: utf-8 -*-
"""
Ruff 代码检查集成
"""

import json
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Any


@dataclass
class RuffIssue:
    """Ruff 检查问题"""
    code: str
    message: str
    filename: str
    line: int
    column: int
    end_line: int
    end_column: int
    fix_available: bool = False
    fix_description: Optional[str] = None


@dataclass
class RuffReport:
    """Ruff 检查报告"""
    success: bool
    issues: List[RuffIssue] = field(default_factory=list)
    fixed_count: int = 0
    error: Optional[str] = None
    
    @property
    def issue_count(self) -> int:
        return len(self.issues)
    
    @property
    def has_issues(self) -> bool:
        return len(self.issues) > 0


class RuffChecker:
    """Ruff 代码检查器"""
    
    # Airflow 相关的 Ruff 规则
    AIRFLOW_RULES = [
        'AIR',    # Airflow 特定规则
        'E',      # pycodestyle 错误
        'F',      # Pyflakes
        'I',      # isort
        'W',      # pycodestyle 警告
        'UP',     # pyupgrade
        'B',      # flake8-bugbear
        'C4',     # flake8-comprehensions
        'SIM',    # flake8-simplify
    ]
    
    # 默认忽略的规则
    DEFAULT_IGNORE = [
        'E501',   # 行太长 (由项目配置决定)
        'E402',   # 模块级导入不在文件顶部
    ]
    
    def __init__(
        self,
        rules: Optional[List[str]] = None,
        ignore: Optional[List[str]] = None,
        line_length: int = 120,
        target_version: str = "py39"
    ):
        self.rules = rules or self.AIRFLOW_RULES
        self.ignore = ignore or self.DEFAULT_IGNORE
        self.line_length = line_length
        self.target_version = target_version
        self._check_ruff_installed()
    
    def _check_ruff_installed(self) -> bool:
        """检查 ruff 是否已安装"""
        try:
            result = subprocess.run(
                ['ruff', '--version'],
                capture_output=True,
                text=True
            )
            return result.returncode == 0
        except FileNotFoundError:
            return False
    
    def check_file(self, file_path: str, fix: bool = False) -> RuffReport:
        """检查单个文件"""
        path = Path(file_path)
        if not path.exists():
            return RuffReport(
                success=False,
                error=f"文件不存在: {file_path}"
            )
        
        return self._run_ruff(str(path), fix=fix)
    
    def check_code(self, code: str, fix: bool = False) -> RuffReport:
        """检查代码字符串"""
        # 创建临时文件
        with tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.py',
            delete=False,
            encoding='utf-8'
        ) as f:
            f.write(code)
            temp_path = f.name
        
        try:
            report = self._run_ruff(temp_path, fix=fix)
            
            # 如果修复了代码,读取修复后的内容
            if fix and report.success:
                with open(temp_path, 'r', encoding='utf-8') as f:
                    report.fixed_code = f.read()
            
            return report
        finally:
            # 清理临时文件
            Path(temp_path).unlink(missing_ok=True)
    
    def check_directory(
        self,
        directory: str,
        fix: bool = False,
        recursive: bool = True
    ) -> RuffReport:
        """检查目录"""
        path = Path(directory)
        if not path.exists():
            return RuffReport(
                success=False,
                error=f"目录不存在: {directory}"
            )
        
        return self._run_ruff(str(path), fix=fix)
    
    def _run_ruff(self, path: str, fix: bool = False) -> RuffReport:
        """运行 ruff 检查"""
        cmd = ['ruff', 'check']
        
        # 添加规则
        if self.rules:
            cmd.extend(['--select', ','.join(self.rules)])
        
        # 添加忽略规则
        if self.ignore:
            cmd.extend(['--ignore', ','.join(self.ignore)])
        
        # 添加行长度限制
        cmd.extend(['--line-length', str(self.line_length)])
        
        # 添加目标 Python 版本
        cmd.extend(['--target-version', self.target_version])
        
        # 输出格式
        cmd.extend(['--output-format', 'json'])
        
        # 是否修复
        if fix:
            cmd.append('--fix')
        
        cmd.append(path)
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True
            )
            
            issues = []
            if result.stdout:
                try:
                    data = json.loads(result.stdout)
                    for item in data:
                        issues.append(RuffIssue(
                            code=item.get('code', ''),
                            message=item.get('message', ''),
                            filename=item.get('filename', ''),
                            line=item.get('location', {}).get('row', 0),
                            column=item.get('location', {}).get('column', 0),
                            end_line=item.get('end_location', {}).get('row', 0),
                            end_column=item.get('end_location', {}).get('column', 0),
                            fix_available=item.get('fix') is not None,
                            fix_description=item.get('fix', {}).get('message') if item.get('fix') else None
                        ))
                except json.JSONDecodeError:
                    pass
            
            return RuffReport(
                success=True,
                issues=issues,
                fixed_count=len([i for i in issues if i.fix_available]) if fix else 0
            )
            
        except FileNotFoundError:
            return RuffReport(
                success=False,
                error="ruff 未安装。请运行: pip install ruff"
            )
        except Exception as e:
            return RuffReport(
                success=False,
                error=f"运行 ruff 时出错: {str(e)}"
            )
    
    def format_file(self, file_path: str) -> bool:
        """格式化文件"""
        try:
            result = subprocess.run(
                ['ruff', 'format', file_path],
                capture_output=True,
                text=True
            )
            return result.returncode == 0
        except Exception:
            return False
    
    def format_code(self, code: str) -> Optional[str]:
        """格式化代码字符串"""
        with tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.py',
            delete=False,
            encoding='utf-8'
        ) as f:
            f.write(code)
            temp_path = f.name
        
        try:
            if self.format_file(temp_path):
                with open(temp_path, 'r', encoding='utf-8') as f:
                    return f.read()
            return None
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def get_issue_summary(self, report: RuffReport) -> Dict[str, int]:
        """获取问题摘要"""
        summary: Dict[str, int] = {}
        for issue in report.issues:
            code = issue.code
            summary[code] = summary.get(code, 0) + 1
        return summary
    
    def generate_report_text(self, report: RuffReport) -> str:
        """生成文本报告"""
        lines = [
            "=" * 60,
            "Ruff 代码检查报告",
            "=" * 60,
            "",
        ]
        
        if not report.success:
            lines.append(f"错误: {report.error}")
            return '\n'.join(lines)
        
        if not report.has_issues:
            lines.append("✓ 未发现问题")
            return '\n'.join(lines)
        
        lines.append(f"发现 {report.issue_count} 个问题:")
        lines.append("")
        
        # 按文件分组
        by_file: Dict[str, List[RuffIssue]] = {}
        for issue in report.issues:
            if issue.filename not in by_file:
                by_file[issue.filename] = []
            by_file[issue.filename].append(issue)
        
        for filename, issues in by_file.items():
            lines.append(f"文件: {filename}")
            for issue in issues:
                fix_mark = " [可自动修复]" if issue.fix_available else ""
                lines.append(
                    f"  行 {issue.line}:{issue.column} - "
                    f"{issue.code}: {issue.message}{fix_mark}"
                )
            lines.append("")
        
        # 摘要
        summary = self.get_issue_summary(report)
        lines.append("问题摘要:")
        for code, count in sorted(summary.items()):
            lines.append(f"  {code}: {count}")
        
        return '\n'.join(lines)


def create_ruff_config() -> str:
    """生成 ruff 配置文件内容"""
    config = """# Ruff configuration for Airflow DAG files
# https://docs.astral.sh/ruff/configuration/

[tool.ruff]
# Python version
target-version = "py39"

# Line length
line-length = 120

# Enable rules
select = [
    "AIR",  # Airflow specific rules
    "E",    # pycodestyle errors
    "F",    # Pyflakes
    "I",    # isort
    "W",    # pycodestyle warnings
    "UP",   # pyupgrade
    "B",    # flake8-bugbear
    "C4",   # flake8-comprehensions
    "SIM",  # flake8-simplify
]

# Ignore rules
ignore = [
    "E501",  # Line too long (handled by formatter)
]

# Exclude directories
exclude = [
    ".git",
    "__pycache__",
    ".venv",
    "venv",
    "build",
    "dist",
]

[tool.ruff.lint.isort]
known-first-party = ["airflow", "dags"]

[tool.ruff.format]
# Use double quotes
quote-style = "double"

# Indent with spaces
indent-style = "space"
"""
    return config
