# -*- coding: utf-8 -*-
"""
Flake8 代码检查集成
"""

import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class Flake8Issue:
    """Flake8 检查问题"""
    code: str
    message: str
    filename: str
    line: int
    column: int
    
    @property
    def category(self) -> str:
        """获取问题类别"""
        if self.code.startswith('E'):
            return 'error'
        elif self.code.startswith('W'):
            return 'warning'
        elif self.code.startswith('F'):
            return 'pyflakes'
        elif self.code.startswith('C'):
            return 'complexity'
        elif self.code.startswith('N'):
            return 'naming'
        elif self.code.startswith('D'):
            return 'docstring'
        else:
            return 'other'


@dataclass
class Flake8Report:
    """Flake8 检查报告"""
    success: bool
    issues: List[Flake8Issue] = field(default_factory=list)
    error: Optional[str] = None
    
    @property
    def issue_count(self) -> int:
        return len(self.issues)
    
    @property
    def has_issues(self) -> bool:
        return len(self.issues) > 0
    
    @property
    def error_count(self) -> int:
        return len([i for i in self.issues if i.category == 'error'])
    
    @property
    def warning_count(self) -> int:
        return len([i for i in self.issues if i.category == 'warning'])


class Flake8Checker:
    """Flake8 代码检查器"""
    
    # 默认启用的插件检查
    DEFAULT_PLUGINS = [
        'flake8-bugbear',
        'flake8-comprehensions',
        'flake8-docstrings',
        'flake8-naming',
    ]
    
    # 默认忽略的规则
    DEFAULT_IGNORE = [
        'E501',   # 行太长
        'W503',   # 二元运算符前换行 (与 black 冲突)
        'E203',   # 冒号前空格 (与 black 冲突)
        'D100',   # 缺少模块文档字符串
        'D104',   # 缺少包文档字符串
    ]
    
    def __init__(
        self,
        max_line_length: int = 120,
        max_complexity: int = 10,
        ignore: Optional[List[str]] = None,
        select: Optional[List[str]] = None
    ):
        self.max_line_length = max_line_length
        self.max_complexity = max_complexity
        self.ignore = ignore or self.DEFAULT_IGNORE
        self.select = select
        self._check_flake8_installed()
    
    def _check_flake8_installed(self) -> bool:
        """检查 flake8 是否已安装"""
        try:
            result = subprocess.run(
                ['flake8', '--version'],
                capture_output=True,
                text=True
            )
            return result.returncode == 0
        except FileNotFoundError:
            return False
    
    def check_file(self, file_path: str) -> Flake8Report:
        """检查单个文件"""
        path = Path(file_path)
        if not path.exists():
            return Flake8Report(
                success=False,
                error=f"文件不存在: {file_path}"
            )
        
        return self._run_flake8(str(path))
    
    def check_code(self, code: str) -> Flake8Report:
        """检查代码字符串"""
        with tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.py',
            delete=False,
            encoding='utf-8'
        ) as f:
            f.write(code)
            temp_path = f.name
        
        try:
            return self._run_flake8(temp_path)
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def check_directory(self, directory: str, recursive: bool = True) -> Flake8Report:
        """检查目录"""
        path = Path(directory)
        if not path.exists():
            return Flake8Report(
                success=False,
                error=f"目录不存在: {directory}"
            )
        
        return self._run_flake8(str(path))
    
    def _run_flake8(self, path: str) -> Flake8Report:
        """运行 flake8 检查"""
        cmd = ['flake8']
        
        # 添加行长度限制
        cmd.extend(['--max-line-length', str(self.max_line_length)])
        
        # 添加复杂度检查
        cmd.extend(['--max-complexity', str(self.max_complexity)])
        
        # 添加忽略规则
        if self.ignore:
            cmd.extend(['--ignore', ','.join(self.ignore)])
        
        # 添加选择规则
        if self.select:
            cmd.extend(['--select', ','.join(self.select)])
        
        # 输出格式
        cmd.extend(['--format', '%(path)s:%(row)d:%(col)d: %(code)s %(text)s'])
        
        cmd.append(path)
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True
            )
            
            issues = []
            if result.stdout:
                for line in result.stdout.strip().split('\n'):
                    if line:
                        issue = self._parse_issue(line)
                        if issue:
                            issues.append(issue)
            
            return Flake8Report(
                success=True,
                issues=issues
            )
            
        except FileNotFoundError:
            return Flake8Report(
                success=False,
                error="flake8 未安装。请运行: pip install flake8"
            )
        except Exception as e:
            return Flake8Report(
                success=False,
                error=f"运行 flake8 时出错: {str(e)}"
            )
    
    def _parse_issue(self, line: str) -> Optional[Flake8Issue]:
        """解析 flake8 输出行"""
        try:
            # 格式: path:line:col: code message
            parts = line.split(':', 3)
            if len(parts) >= 4:
                filename = parts[0]
                line_num = int(parts[1])
                column = int(parts[2])
                code_msg = parts[3].strip()
                
                # 分离 code 和 message
                code_parts = code_msg.split(' ', 1)
                code = code_parts[0]
                message = code_parts[1] if len(code_parts) > 1 else ''
                
                return Flake8Issue(
                    code=code,
                    message=message,
                    filename=filename,
                    line=line_num,
                    column=column
                )
        except (ValueError, IndexError):
            pass
        return None
    
    def get_issue_summary(self, report: Flake8Report) -> Dict[str, int]:
        """获取问题摘要"""
        summary: Dict[str, int] = {}
        for issue in report.issues:
            code = issue.code
            summary[code] = summary.get(code, 0) + 1
        return summary
    
    def get_category_summary(self, report: Flake8Report) -> Dict[str, int]:
        """获取分类摘要"""
        summary: Dict[str, int] = {}
        for issue in report.issues:
            category = issue.category
            summary[category] = summary.get(category, 0) + 1
        return summary
    
    def generate_report_text(self, report: Flake8Report) -> str:
        """生成文本报告"""
        lines = [
            "=" * 60,
            "Flake8 代码检查报告",
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
        lines.append(f"  - 错误: {report.error_count}")
        lines.append(f"  - 警告: {report.warning_count}")
        lines.append("")
        
        # 按文件分组
        by_file: Dict[str, List[Flake8Issue]] = {}
        for issue in report.issues:
            if issue.filename not in by_file:
                by_file[issue.filename] = []
            by_file[issue.filename].append(issue)
        
        for filename, issues in by_file.items():
            lines.append(f"文件: {filename}")
            for issue in issues:
                lines.append(
                    f"  行 {issue.line}:{issue.column} - "
                    f"{issue.code}: {issue.message}"
                )
            lines.append("")
        
        # 摘要
        summary = self.get_issue_summary(report)
        lines.append("问题摘要:")
        for code, count in sorted(summary.items()):
            lines.append(f"  {code}: {count}")
        
        return '\n'.join(lines)


def create_flake8_config() -> str:
    """生成 flake8 配置文件内容"""
    config = """# Flake8 configuration for Airflow DAG files
# https://flake8.pycqa.org/en/latest/user/configuration.html

[flake8]
# Maximum line length
max-line-length = 120

# Maximum cyclomatic complexity
max-complexity = 10

# Ignore rules
ignore =
    E501,  # Line too long
    W503,  # Line break before binary operator
    E203,  # Whitespace before ':'
    D100,  # Missing docstring in public module
    D104,  # Missing docstring in public package

# Exclude directories
exclude =
    .git,
    __pycache__,
    .venv,
    venv,
    build,
    dist,
    *.egg-info

# Per-file ignores
per-file-ignores =
    __init__.py: F401
    tests/*: D100,D101,D102,D103

# Enable extensions
extend-select =
    B,    # flake8-bugbear
    C4,   # flake8-comprehensions

# Docstring convention
docstring-convention = google
"""
    return config
