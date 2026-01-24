# -*- coding: utf-8 -*-
"""
DAG验证器 - 验证转换后的DAG代码
"""

import ast
import re
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


@dataclass
class ValidationIssue:
    """验证问题"""
    level: str  # error, warning, info
    message: str
    line_number: Optional[int] = None
    rule: str = ""


@dataclass
class ValidationResult:
    """验证结果"""
    is_valid: bool
    issues: List[ValidationIssue] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    info: List[str] = field(default_factory=list)
    airflow3_compatible: bool = False
    
    def add_issue(self, issue: ValidationIssue):
        self.issues.append(issue)
        if issue.level == 'error':
            self.errors.append(issue.message)
        elif issue.level == 'warning':
            self.warnings.append(issue.message)
        else:
            self.info.append(issue.message)


class DAGValidator:
    """DAG验证器"""
    
    # Airflow 3.x 中已移除的模块/类
    REMOVED_IN_V3 = {
        'SubDagOperator',
        'airflow.operators.subdag_operator',
        'airflow.contrib',
    }
    
    # Airflow 3.x 中已弃用的参数
    DEPRECATED_PARAMS_V3 = {
        'schedule_interval',  # 使用 schedule
        'provide_context',    # 自动提供
        'execution_date',     # 使用 logical_date
    }
    
    # 必需的导入检查
    REQUIRED_IMPORTS = {
        'EmptyOperator': 'airflow.operators.empty',
        'DAG': ['airflow', 'airflow.models'],
    }
    
    def __init__(self, target_version: str = "3.0"):
        self.target_version = target_version
    
    def validate_code(self, code: str) -> ValidationResult:
        """验证代码"""
        result = ValidationResult(is_valid=True, airflow3_compatible=True)
        
        # 1. 语法验证
        syntax_valid = self._validate_syntax(code, result)
        if not syntax_valid:
            result.is_valid = False
            return result
        
        # 2. 导入验证
        self._validate_imports(code, result)
        
        # 3. DAG结构验证
        self._validate_dag_structure(code, result)
        
        # 4. 操作符验证
        self._validate_operators(code, result)
        
        # 5. 参数验证
        self._validate_parameters(code, result)
        
        # 6. Airflow 3.x 兼容性验证
        self._validate_airflow3_compatibility(code, result)
        
        # 根据错误数量判断是否有效
        result.is_valid = len(result.errors) == 0
        result.airflow3_compatible = result.is_valid and len([
            w for w in result.warnings 
            if 'Airflow 3' in w or '3.x' in w
        ]) == 0
        
        return result
    
    def validate_file(self, file_path: str) -> ValidationResult:
        """验证文件"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                code = f.read()
            return self.validate_code(code)
        except FileNotFoundError:
            result = ValidationResult(is_valid=False)
            result.add_issue(ValidationIssue(
                level='error',
                message=f"文件不存在: {file_path}",
                rule='file_exists'
            ))
            return result
        except Exception as e:
            result = ValidationResult(is_valid=False)
            result.add_issue(ValidationIssue(
                level='error',
                message=f"读取文件错误: {str(e)}",
                rule='file_read'
            ))
            return result
    
    def _validate_syntax(self, code: str, result: ValidationResult) -> bool:
        """验证Python语法"""
        try:
            ast.parse(code)
            return True
        except SyntaxError as e:
            result.add_issue(ValidationIssue(
                level='error',
                message=f"语法错误: {e.msg} (行 {e.lineno})",
                line_number=e.lineno,
                rule='syntax'
            ))
            return False
    
    def _validate_imports(self, code: str, result: ValidationResult):
        """验证导入语句"""
        tree = ast.parse(code)
        
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append((alias.name, node.lineno))
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.append((node.module, node.lineno))
                    for alias in node.names:
                        imports.append((f"{node.module}.{alias.name}", node.lineno))
        
        # 检查已移除的导入
        for imp, line in imports:
            for removed in self.REMOVED_IN_V3:
                if removed in imp:
                    result.add_issue(ValidationIssue(
                        level='error',
                        message=f"导入 '{imp}' 在 Airflow 3.x 中已移除 (行 {line})",
                        line_number=line,
                        rule='removed_import'
                    ))
        
        # 检查 contrib 导入
        for imp, line in imports:
            if 'airflow.contrib' in imp:
                result.add_issue(ValidationIssue(
                    level='error',
                    message=f"airflow.contrib 在 Airflow 3.x 中已移除,请使用 airflow.providers (行 {line})",
                    line_number=line,
                    rule='contrib_import'
                ))
        
        # 检查旧版操作符导入路径
        old_import_patterns = [
            ('airflow.operators.python_operator', 'airflow.operators.python'),
            ('airflow.operators.bash_operator', 'airflow.operators.bash'),
            ('airflow.operators.dummy_operator', 'airflow.operators.empty'),
        ]
        
        for imp, line in imports:
            for old_path, new_path in old_import_patterns:
                if old_path in imp:
                    result.add_issue(ValidationIssue(
                        level='warning',
                        message=f"建议将 '{old_path}' 更新为 '{new_path}' (行 {line})",
                        line_number=line,
                        rule='deprecated_import'
                    ))
    
    def _validate_dag_structure(self, code: str, result: ValidationResult):
        """验证DAG结构"""
        # 检查是否有DAG定义
        has_dag = (
            'DAG(' in code or
            '@dag' in code or
            'with DAG' in code
        )
        
        if not has_dag:
            result.add_issue(ValidationIssue(
                level='warning',
                message="未检测到DAG定义",
                rule='dag_definition'
            ))
        
        # 检查DAG ID
        dag_id_pattern = r'dag_id\s*=\s*["\']([^"\']+)["\']'
        dag_ids = re.findall(dag_id_pattern, code)
        
        for dag_id in dag_ids:
            # DAG ID 命名规范检查
            if not re.match(r'^[a-zA-Z][a-zA-Z0-9_-]*$', dag_id):
                result.add_issue(ValidationIssue(
                    level='warning',
                    message=f"DAG ID '{dag_id}' 不符合命名规范 (应以字母开头,只包含字母、数字、下划线和连字符)",
                    rule='dag_id_naming'
                ))
    
    def _validate_operators(self, code: str, result: ValidationResult):
        """验证操作符"""
        # 检查 DummyOperator (应使用 EmptyOperator)
        if 'DummyOperator' in code:
            # 检查是否已导入 EmptyOperator
            if 'EmptyOperator' not in code:
                result.add_issue(ValidationIssue(
                    level='warning',
                    message="DummyOperator 已弃用,建议使用 EmptyOperator",
                    rule='deprecated_operator'
                ))
        
        # 检查 SubDagOperator
        if 'SubDagOperator' in code:
            result.add_issue(ValidationIssue(
                level='error',
                message="SubDagOperator 在 Airflow 3.x 中已移除,请使用 TaskGroup 替代",
                rule='removed_operator'
            ))
        
        # 检查 task_id 是否存在
        tree = ast.parse(code)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func_name = self._get_call_name(node)
                if func_name and 'Operator' in func_name:
                    has_task_id = any(
                        kw.arg == 'task_id' for kw in node.keywords
                    )
                    if not has_task_id:
                        result.add_issue(ValidationIssue(
                            level='warning',
                            message=f"{func_name} 缺少 task_id 参数 (行 {node.lineno})",
                            line_number=node.lineno,
                            rule='missing_task_id'
                        ))
    
    def _validate_parameters(self, code: str, result: ValidationResult):
        """验证参数"""
        # 检查已弃用的参数
        for param in self.DEPRECATED_PARAMS_V3:
            pattern = rf'\b{param}\s*='
            matches = list(re.finditer(pattern, code))
            for match in matches:
                line_num = code[:match.start()].count('\n') + 1
                
                if param == 'schedule_interval':
                    result.add_issue(ValidationIssue(
                        level='warning',
                        message=f"参数 'schedule_interval' 建议更新为 'schedule' (行 {line_num})",
                        line_number=line_num,
                        rule='deprecated_param'
                    ))
                elif param == 'provide_context':
                    result.add_issue(ValidationIssue(
                        level='info',
                        message=f"参数 'provide_context' 在 Airflow 2.0+ 中已自动提供,可以移除 (行 {line_num})",
                        line_number=line_num,
                        rule='unnecessary_param'
                    ))
                elif param == 'execution_date':
                    result.add_issue(ValidationIssue(
                        level='warning',
                        message=f"'execution_date' 在 Airflow 3.x 中已被 'logical_date' 替代 (行 {line_num})",
                        line_number=line_num,
                        rule='deprecated_param'
                    ))
    
    def _validate_airflow3_compatibility(self, code: str, result: ValidationResult):
        """验证Airflow 3.x兼容性"""
        # 检查 context 字典访问方式
        old_context_keys = ['execution_date', 'next_execution_date', 'prev_execution_date']
        for key in old_context_keys:
            if f"context['{key}']" in code or f'context["{key}"]' in code:
                result.add_issue(ValidationIssue(
                    level='warning',
                    message=f"context['{key}'] 在 Airflow 3.x 中已更改,请使用新的命名",
                    rule='context_key'
                ))
        
        # 检查 XCom 序列化
        if 'xcom_push' in code or 'xcom_pull' in code:
            result.add_issue(ValidationIssue(
                level='info',
                message="建议使用 TaskFlow API (@task 装饰器) 简化 XCom 操作",
                rule='xcom_style'
            ))
        
        # 检查 Jinja 模板使用
        jinja_patterns = [
            (r'\{\{\s*execution_date\s*\}\}', 'execution_date', 'logical_date'),
            (r'\{\{\s*next_execution_date\s*\}\}', 'next_execution_date', 'data_interval_end'),
            (r'\{\{\s*prev_execution_date\s*\}\}', 'prev_execution_date', 'data_interval_start'),
        ]
        
        for pattern, old_name, new_name in jinja_patterns:
            if re.search(pattern, code):
                result.add_issue(ValidationIssue(
                    level='warning',
                    message=f"Jinja 模板变量 '{old_name}' 建议更新为 '{new_name}'",
                    rule='jinja_template'
                ))
        
        # 检查时区处理
        if 'pendulum' in code.lower() or 'timezone' in code.lower():
            result.add_issue(ValidationIssue(
                level='info',
                message="检测到时区处理代码,请确保与 Airflow 3.x 的时区处理兼容",
                rule='timezone'
            ))
    
    def _get_call_name(self, node: ast.Call) -> Optional[str]:
        """获取函数调用名称"""
        if isinstance(node.func, ast.Name):
            return node.func.id
        elif isinstance(node.func, ast.Attribute):
            return node.func.attr
        return None
    
    def get_compatibility_score(self, result: ValidationResult) -> Dict[str, Any]:
        """计算兼容性评分"""
        total_issues = len(result.issues)
        error_count = len(result.errors)
        warning_count = len(result.warnings)
        info_count = len(result.info)
        
        # 计算分数 (满分100)
        score = 100
        score -= error_count * 20  # 每个错误扣20分
        score -= warning_count * 5  # 每个警告扣5分
        score -= info_count * 1     # 每个提示扣1分
        score = max(0, score)
        
        return {
            'score': score,
            'grade': self._get_grade(score),
            'total_issues': total_issues,
            'errors': error_count,
            'warnings': warning_count,
            'info': info_count,
            'is_compatible': error_count == 0,
            'recommendation': self._get_recommendation(score, error_count)
        }
    
    def _get_grade(self, score: int) -> str:
        """获取等级"""
        if score >= 90:
            return 'A'
        elif score >= 80:
            return 'B'
        elif score >= 70:
            return 'C'
        elif score >= 60:
            return 'D'
        else:
            return 'F'
    
    def _get_recommendation(self, score: int, error_count: int) -> str:
        """获取建议"""
        if error_count > 0:
            return "存在兼容性错误,需要修复后才能在 Airflow 3.x 中运行"
        elif score >= 90:
            return "代码兼容性良好,可以直接迁移到 Airflow 3.x"
        elif score >= 70:
            return "代码基本兼容,建议处理警告后再迁移"
        else:
            return "代码需要较多修改,建议逐步升级"
