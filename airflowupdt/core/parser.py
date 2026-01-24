# -*- coding: utf-8 -*-
"""
DAG解析器 - 解析Airflow DAG文件结构
"""

import ast
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Any


@dataclass
class ImportInfo:
    """导入语句信息"""
    module: str
    names: List[str]
    alias: Optional[str] = None
    line_number: int = 0
    is_from_import: bool = False


@dataclass
class OperatorInfo:
    """操作符信息"""
    name: str
    operator_type: str
    task_id: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    line_number: int = 0
    decorators: List[str] = field(default_factory=list)


@dataclass
class DAGConfig:
    """DAG配置信息"""
    dag_id: str
    schedule_interval: Optional[str] = None
    schedule: Optional[str] = None
    start_date: Optional[str] = None
    catchup: Optional[bool] = None
    default_args: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    params: Dict[str, Any] = field(default_factory=dict)
    line_number: int = 0


@dataclass
class TaskDependency:
    """任务依赖关系"""
    upstream: str
    downstream: str
    line_number: int = 0


@dataclass
class DAGStructure:
    """DAG结构"""
    file_path: str
    source_code: str
    imports: List[ImportInfo] = field(default_factory=list)
    operators: List[OperatorInfo] = field(default_factory=list)
    dag_config: Optional[DAGConfig] = None
    dependencies: List[TaskDependency] = field(default_factory=list)
    decorators: List[str] = field(default_factory=list)
    variables: Dict[str, Any] = field(default_factory=dict)
    connections: List[str] = field(default_factory=list)
    hooks: List[str] = field(default_factory=list)
    sensors: List[str] = field(default_factory=list)
    airflow_version: Optional[str] = None
    issues: List[str] = field(default_factory=list)


class DAGParser:
    """DAG文件解析器"""
    
    # Airflow 2.x 常见操作符
    AIRFLOW2_OPERATORS = {
        'PythonOperator', 'BashOperator', 'DummyOperator', 'EmptyOperator',
        'BranchPythonOperator', 'ShortCircuitOperator', 'PythonVirtualenvOperator',
        'ExternalPythonOperator', 'SqlSensor', 'HttpSensor', 'FileSensor',
        'TimeDeltaSensor', 'TimeSensor', 'DateTimeSensor',
        'PostgresOperator', 'MySqlOperator', 'MsSqlOperator', 'OracleOperator',
        'BigQueryOperator', 'BigQueryInsertJobOperator',
        'S3KeySensor', 'S3ToRedshiftOperator', 'RedshiftSQLOperator',
        'SparkSubmitOperator', 'SparkSqlOperator',
        'EmailOperator', 'SlackWebhookOperator',
        'TriggerDagRunOperator', 'ExternalTaskSensor',
        'KubernetesPodOperator', 'DockerOperator',
    }
    
    # Airflow 2.x 已弃用的导入路径
    DEPRECATED_IMPORTS = {
        'airflow.operators.python_operator': 'airflow.operators.python',
        'airflow.operators.bash_operator': 'airflow.operators.bash',
        'airflow.operators.dummy_operator': 'airflow.operators.empty',
        'airflow.operators.email_operator': 'airflow.operators.email',
        'airflow.operators.subdag_operator': None,  # 已移除
        'airflow.contrib.operators': 'airflow.providers',
        'airflow.contrib.hooks': 'airflow.providers',
        'airflow.contrib.sensors': 'airflow.providers',
    }
    
    def __init__(self):
        self.ast_tree: Optional[ast.AST] = None
    
    def parse_file(self, file_path: str) -> DAGStructure:
        """解析DAG文件"""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        if not path.suffix == '.py':
            raise ValueError(f"不是Python文件: {file_path}")
        
        source_code = path.read_text(encoding='utf-8')
        return self.parse_source(source_code, file_path)
    
    def parse_source(self, source_code: str, file_path: str = "<string>") -> DAGStructure:
        """解析DAG源代码"""
        dag_structure = DAGStructure(
            file_path=file_path,
            source_code=source_code
        )
        
        try:
            self.ast_tree = ast.parse(source_code)
        except SyntaxError as e:
            dag_structure.issues.append(f"语法错误: {e}")
            return dag_structure
        
        # 解析各个组件
        dag_structure.imports = self._parse_imports(self.ast_tree)
        dag_structure.operators = self._parse_operators(self.ast_tree)
        dag_structure.dag_config = self._parse_dag_config(self.ast_tree)
        dag_structure.dependencies = self._parse_dependencies(self.ast_tree)
        dag_structure.decorators = self._parse_decorators(self.ast_tree)
        dag_structure.variables = self._parse_variables(self.ast_tree)
        dag_structure.connections = self._parse_connections(source_code)
        dag_structure.hooks = self._parse_hooks(self.ast_tree)
        dag_structure.sensors = self._parse_sensors(self.ast_tree)
        dag_structure.airflow_version = self._detect_airflow_version(dag_structure)
        
        return dag_structure
    
    def _parse_imports(self, tree: ast.AST) -> List[ImportInfo]:
        """解析导入语句"""
        imports = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(ImportInfo(
                        module=alias.name,
                        names=[alias.name],
                        alias=alias.asname,
                        line_number=node.lineno,
                        is_from_import=False
                    ))
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.append(ImportInfo(
                        module=node.module,
                        names=[alias.name for alias in node.names],
                        alias=None,
                        line_number=node.lineno,
                        is_from_import=True
                    ))
        
        return imports
    
    def _parse_operators(self, tree: ast.AST) -> List[OperatorInfo]:
        """解析操作符"""
        operators = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                operator_name = self._get_call_name(node)
                if operator_name and self._is_operator(operator_name):
                    task_id = self._extract_task_id(node)
                    params = self._extract_parameters(node)
                    operators.append(OperatorInfo(
                        name=operator_name,
                        operator_type=self._classify_operator(operator_name),
                        task_id=task_id or "unknown",
                        parameters=params,
                        line_number=node.lineno
                    ))
        
        return operators
    
    def _parse_dag_config(self, tree: ast.AST) -> Optional[DAGConfig]:
        """解析DAG配置"""
        for node in ast.walk(tree):
            # 检查 DAG() 调用
            if isinstance(node, ast.Call):
                call_name = self._get_call_name(node)
                if call_name == 'DAG':
                    return self._extract_dag_config(node)
            
            # 检查 @dag 装饰器
            if isinstance(node, ast.FunctionDef):
                for decorator in node.decorator_list:
                    dec_name = self._get_decorator_name(decorator)
                    if dec_name == 'dag':
                        return self._extract_dag_config_from_decorator(decorator, node)
        
        return None
    
    def _parse_dependencies(self, tree: ast.AST) -> List[TaskDependency]:
        """解析任务依赖关系"""
        dependencies = []
        
        for node in ast.walk(tree):
            # 检查 >> 和 << 操作符
            if isinstance(node, ast.BinOp):
                if isinstance(node.op, ast.RShift):  # >>
                    upstream = self._get_task_name(node.left)
                    downstream = self._get_task_name(node.right)
                    if upstream and downstream:
                        dependencies.append(TaskDependency(
                            upstream=upstream,
                            downstream=downstream,
                            line_number=node.lineno
                        ))
                elif isinstance(node.op, ast.LShift):  # <<
                    upstream = self._get_task_name(node.right)
                    downstream = self._get_task_name(node.left)
                    if upstream and downstream:
                        dependencies.append(TaskDependency(
                            upstream=upstream,
                            downstream=downstream,
                            line_number=node.lineno
                        ))
            
            # 检查 set_upstream/set_downstream 方法调用
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute):
                    if node.func.attr in ('set_upstream', 'set_downstream'):
                        task = self._get_task_name(node.func.value)
                        if node.args:
                            dep_task = self._get_task_name(node.args[0])
                            if task and dep_task:
                                if node.func.attr == 'set_upstream':
                                    dependencies.append(TaskDependency(
                                        upstream=dep_task,
                                        downstream=task,
                                        line_number=node.lineno
                                    ))
                                else:
                                    dependencies.append(TaskDependency(
                                        upstream=task,
                                        downstream=dep_task,
                                        line_number=node.lineno
                                    ))
        
        return dependencies
    
    def _parse_decorators(self, tree: ast.AST) -> List[str]:
        """解析装饰器"""
        decorators = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                for decorator in node.decorator_list:
                    dec_name = self._get_decorator_name(decorator)
                    if dec_name:
                        decorators.append(dec_name)
        
        return decorators
    
    def _parse_variables(self, tree: ast.AST) -> Dict[str, Any]:
        """解析Airflow变量引用"""
        variables = {}
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                call_name = self._get_call_name(node)
                # Variable.get()
                if call_name == 'Variable.get' or (
                    isinstance(node.func, ast.Attribute) and 
                    node.func.attr == 'get' and
                    isinstance(node.func.value, ast.Name) and
                    node.func.value.id == 'Variable'
                ):
                    if node.args:
                        var_name = self._get_string_value(node.args[0])
                        if var_name:
                            variables[var_name] = {
                                'line': node.lineno,
                                'method': 'get'
                            }
        
        return variables
    
    def _parse_connections(self, source_code: str) -> List[str]:
        """解析连接引用"""
        connections = []
        
        # 匹配 conn_id 参数
        conn_pattern = r'conn_id\s*=\s*["\']([^"\']+)["\']'
        matches = re.findall(conn_pattern, source_code)
        connections.extend(matches)
        
        # 匹配 *_conn_id 参数
        conn_pattern2 = r'\w+_conn_id\s*=\s*["\']([^"\']+)["\']'
        matches2 = re.findall(conn_pattern2, source_code)
        connections.extend(matches2)
        
        return list(set(connections))
    
    def _parse_hooks(self, tree: ast.AST) -> List[str]:
        """解析Hook使用"""
        hooks = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                call_name = self._get_call_name(node)
                if call_name and 'Hook' in call_name:
                    hooks.append(call_name)
        
        return list(set(hooks))
    
    def _parse_sensors(self, tree: ast.AST) -> List[str]:
        """解析Sensor使用"""
        sensors = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                call_name = self._get_call_name(node)
                if call_name and 'Sensor' in call_name:
                    sensors.append(call_name)
        
        return list(set(sensors))
    
    def _detect_airflow_version(self, dag_structure: DAGStructure) -> Optional[str]:
        """检测Airflow版本"""
        # 基于导入路径和使用的特性推断版本
        has_taskflow = any(
            'decorators' in imp.module or 
            imp.module == 'airflow.decorators'
            for imp in dag_structure.imports
        )
        
        has_deprecated = any(
            imp.module in self.DEPRECATED_IMPORTS
            for imp in dag_structure.imports
        )
        
        uses_dag_decorator = 'dag' in dag_structure.decorators
        uses_task_decorator = 'task' in dag_structure.decorators
        
        # 检查是否使用 schedule 而不是 schedule_interval
        uses_schedule = dag_structure.dag_config and dag_structure.dag_config.schedule is not None
        
        if has_deprecated:
            return "2.x (deprecated imports)"
        elif has_taskflow and (uses_dag_decorator or uses_task_decorator):
            if uses_schedule:
                return "2.4+"
            return "2.0+"
        else:
            return "2.x"
    
    def _get_call_name(self, node: ast.Call) -> Optional[str]:
        """获取函数调用名称"""
        if isinstance(node.func, ast.Name):
            return node.func.id
        elif isinstance(node.func, ast.Attribute):
            parts = []
            current = node.func
            while isinstance(current, ast.Attribute):
                parts.append(current.attr)
                current = current.value
            if isinstance(current, ast.Name):
                parts.append(current.id)
            return '.'.join(reversed(parts))
        return None
    
    def _get_decorator_name(self, decorator: ast.expr) -> Optional[str]:
        """获取装饰器名称"""
        if isinstance(decorator, ast.Name):
            return decorator.id
        elif isinstance(decorator, ast.Call):
            return self._get_call_name(decorator)
        elif isinstance(decorator, ast.Attribute):
            return decorator.attr
        return None
    
    def _is_operator(self, name: str) -> bool:
        """判断是否是操作符"""
        if name in self.AIRFLOW2_OPERATORS:
            return True
        if 'Operator' in name or 'Sensor' in name:
            return True
        return False
    
    def _classify_operator(self, name: str) -> str:
        """分类操作符类型"""
        if 'Sensor' in name:
            return 'sensor'
        elif 'Python' in name:
            return 'python'
        elif 'Bash' in name:
            return 'bash'
        elif 'Sql' in name or 'SQL' in name:
            return 'sql'
        elif 'Dummy' in name or 'Empty' in name:
            return 'empty'
        elif 'Branch' in name:
            return 'branch'
        elif 'Trigger' in name:
            return 'trigger'
        else:
            return 'other'
    
    def _extract_task_id(self, node: ast.Call) -> Optional[str]:
        """提取task_id"""
        for keyword in node.keywords:
            if keyword.arg == 'task_id':
                return self._get_string_value(keyword.value)
        return None
    
    def _extract_parameters(self, node: ast.Call) -> Dict[str, Any]:
        """提取参数"""
        params = {}
        for keyword in node.keywords:
            if keyword.arg:
                params[keyword.arg] = self._get_value_repr(keyword.value)
        return params
    
    def _extract_dag_config(self, node: ast.Call) -> DAGConfig:
        """从DAG()调用提取配置"""
        config = DAGConfig(dag_id="unknown", line_number=node.lineno)
        
        # 位置参数 (dag_id)
        if node.args:
            config.dag_id = self._get_string_value(node.args[0]) or "unknown"
        
        # 关键字参数
        for keyword in node.keywords:
            if keyword.arg == 'dag_id':
                config.dag_id = self._get_string_value(keyword.value) or "unknown"
            elif keyword.arg == 'schedule_interval':
                config.schedule_interval = self._get_value_repr(keyword.value)
            elif keyword.arg == 'schedule':
                config.schedule = self._get_value_repr(keyword.value)
            elif keyword.arg == 'start_date':
                config.start_date = self._get_value_repr(keyword.value)
            elif keyword.arg == 'catchup':
                config.catchup = self._get_bool_value(keyword.value)
            elif keyword.arg == 'default_args':
                config.default_args = self._get_dict_value(keyword.value)
            elif keyword.arg == 'tags':
                config.tags = self._get_list_value(keyword.value)
            elif keyword.arg == 'params':
                config.params = self._get_dict_value(keyword.value)
        
        return config
    
    def _extract_dag_config_from_decorator(self, decorator: ast.expr, func: ast.FunctionDef) -> DAGConfig:
        """从@dag装饰器提取配置"""
        config = DAGConfig(dag_id=func.name, line_number=decorator.lineno if hasattr(decorator, 'lineno') else 0)
        
        if isinstance(decorator, ast.Call):
            for keyword in decorator.keywords:
                if keyword.arg == 'dag_id':
                    config.dag_id = self._get_string_value(keyword.value) or func.name
                elif keyword.arg == 'schedule_interval':
                    config.schedule_interval = self._get_value_repr(keyword.value)
                elif keyword.arg == 'schedule':
                    config.schedule = self._get_value_repr(keyword.value)
                elif keyword.arg == 'start_date':
                    config.start_date = self._get_value_repr(keyword.value)
                elif keyword.arg == 'catchup':
                    config.catchup = self._get_bool_value(keyword.value)
                elif keyword.arg == 'default_args':
                    config.default_args = self._get_dict_value(keyword.value)
                elif keyword.arg == 'tags':
                    config.tags = self._get_list_value(keyword.value)
        
        return config
    
    def _get_task_name(self, node: ast.expr) -> Optional[str]:
        """获取任务名称"""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Call):
            return self._extract_task_id(node)
        elif isinstance(node, ast.Subscript):
            if isinstance(node.value, ast.Name):
                return node.value.id
        return None
    
    def _get_string_value(self, node: ast.expr) -> Optional[str]:
        """获取字符串值"""
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        elif isinstance(node, ast.Str):  # Python 3.7 兼容
            return node.s
        return None
    
    def _get_bool_value(self, node: ast.expr) -> Optional[bool]:
        """获取布尔值"""
        if isinstance(node, ast.Constant) and isinstance(node.value, bool):
            return node.value
        elif isinstance(node, ast.NameConstant):  # Python 3.7 兼容
            return node.value
        return None
    
    def _get_value_repr(self, node: ast.expr) -> str:
        """获取值的字符串表示"""
        if isinstance(node, ast.Constant):
            return repr(node.value)
        elif isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            return ast.unparse(node) if hasattr(ast, 'unparse') else str(node.attr)
        elif isinstance(node, ast.Call):
            return self._get_call_name(node) or "call"
        else:
            try:
                return ast.unparse(node)
            except:
                return "<complex>"
    
    def _get_dict_value(self, node: ast.expr) -> Dict[str, Any]:
        """获取字典值"""
        result = {}
        if isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values):
                if key:
                    key_str = self._get_string_value(key) or self._get_value_repr(key)
                    result[key_str] = self._get_value_repr(value)
        elif isinstance(node, ast.Name):
            result['__ref__'] = node.id
        return result
    
    def _get_list_value(self, node: ast.expr) -> List[str]:
        """获取列表值"""
        result = []
        if isinstance(node, ast.List):
            for elt in node.elts:
                val = self._get_string_value(elt)
                if val:
                    result.append(val)
        return result
