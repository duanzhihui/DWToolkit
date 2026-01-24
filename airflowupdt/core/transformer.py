# -*- coding: utf-8 -*-
"""
DAG转换器 - 应用升级规则转换DAG代码
"""

import ast
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Callable

from airflowupdt.core.parser import DAGStructure, ImportInfo, OperatorInfo


@dataclass
class Transformation:
    """转换记录"""
    rule_name: str
    description: str
    line_number: int
    old_code: str
    new_code: str
    category: str = "general"


@dataclass
class TransformResult:
    """转换结果"""
    success: bool
    transformed_code: str
    transformations: List[Transformation] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class DAGTransformer:
    """DAG代码转换器"""
    
    def __init__(self, target_version: str = "3.0"):
        self.target_version = target_version
        self.transformations: List[Transformation] = []
        self._setup_rules()
    
    def _setup_rules(self):
        """设置转换规则"""
        # 导入路径映射 (Airflow 2.x -> 3.x)
        self.import_mappings: Dict[str, Optional[str]] = {
            # 操作符导入路径变更
            'airflow.operators.python_operator': 'airflow.operators.python',
            'airflow.operators.bash_operator': 'airflow.operators.bash',
            'airflow.operators.dummy_operator': 'airflow.operators.empty',
            'airflow.operators.dummy': 'airflow.operators.empty',
            'airflow.operators.email_operator': 'airflow.operators.email',
            'airflow.operators.subdag_operator': None,  # 已移除,需要重构
            'airflow.operators.latest_only_operator': 'airflow.operators.latest_only',
            
            # Sensors
            'airflow.sensors.base_sensor_operator': 'airflow.sensors.base',
            'airflow.sensors.external_task_sensor': 'airflow.sensors.external_task',
            'airflow.sensors.time_sensor': 'airflow.sensors.time_sensor',
            
            # Hooks
            'airflow.hooks.base_hook': 'airflow.hooks.base',
            'airflow.hooks.dbapi_hook': 'airflow.hooks.dbapi',
            
            # contrib -> providers 迁移
            'airflow.contrib.operators.bigquery_operator': 'airflow.providers.google.cloud.operators.bigquery',
            'airflow.contrib.operators.gcs_to_bq': 'airflow.providers.google.cloud.transfers.gcs_to_bigquery',
            'airflow.contrib.operators.dataproc_operator': 'airflow.providers.google.cloud.operators.dataproc',
            'airflow.contrib.operators.slack_webhook_operator': 'airflow.providers.slack.operators.slack_webhook',
            'airflow.contrib.operators.ssh_operator': 'airflow.providers.ssh.operators.ssh',
            'airflow.contrib.operators.sftp_operator': 'airflow.providers.sftp.operators.sftp',
            'airflow.contrib.hooks.bigquery_hook': 'airflow.providers.google.cloud.hooks.bigquery',
            'airflow.contrib.hooks.gcs_hook': 'airflow.providers.google.cloud.hooks.gcs',
            'airflow.contrib.hooks.slack_webhook_hook': 'airflow.providers.slack.hooks.slack_webhook',
            'airflow.contrib.sensors.bigquery_sensor': 'airflow.providers.google.cloud.sensors.bigquery',
            
            # AWS providers
            'airflow.contrib.operators.aws_athena_operator': 'airflow.providers.amazon.aws.operators.athena',
            'airflow.contrib.operators.s3_to_redshift_operator': 'airflow.providers.amazon.aws.transfers.s3_to_redshift',
            'airflow.contrib.hooks.aws_hook': 'airflow.providers.amazon.aws.hooks.base_aws',
            'airflow.contrib.hooks.s3_hook': 'airflow.providers.amazon.aws.hooks.s3',
            'airflow.contrib.sensors.s3_key_sensor': 'airflow.providers.amazon.aws.sensors.s3',
            
            # 数据库 providers
            'airflow.operators.postgres_operator': 'airflow.providers.postgres.operators.postgres',
            'airflow.operators.mysql_operator': 'airflow.providers.mysql.operators.mysql',
            'airflow.operators.mssql_operator': 'airflow.providers.microsoft.mssql.operators.mssql',
            'airflow.operators.oracle_operator': 'airflow.providers.oracle.operators.oracle',
            'airflow.hooks.postgres_hook': 'airflow.providers.postgres.hooks.postgres',
            'airflow.hooks.mysql_hook': 'airflow.providers.mysql.hooks.mysql',
            'airflow.hooks.mssql_hook': 'airflow.providers.microsoft.mssql.hooks.mssql',
            'airflow.hooks.oracle_hook': 'airflow.providers.oracle.hooks.oracle',
        }
        
        # 类名映射
        self.class_mappings: Dict[str, str] = {
            'DummyOperator': 'EmptyOperator',
            'BigQueryOperator': 'BigQueryInsertJobOperator',
            'DataProcPySparkOperator': 'DataprocSubmitJobOperator',
            'DataProcSparkOperator': 'DataprocSubmitJobOperator',
            'GoogleCloudStorageToBigQueryOperator': 'GCSToBigQueryOperator',
        }
        
        # 参数名映射
        self.param_mappings: Dict[str, Dict[str, str]] = {
            'DAG': {
                'schedule_interval': 'schedule',
                'concurrency': 'max_active_tasks',
            },
            'BaseOperator': {
                'execution_timeout': 'execution_timeout',
                'task_concurrency': 'max_active_tis_per_dag',
            },
            'BigQueryInsertJobOperator': {
                'bql': 'sql',
                'destination_dataset_table': 'destination_table',
            },
        }
        
        # 已弃用的参数
        self.deprecated_params: Dict[str, List[str]] = {
            'DAG': ['schedule_interval'],  # 在3.0中推荐使用schedule
            'BaseOperator': ['provide_context'],  # 在2.0+中已自动提供
        }
    
    def transform(self, dag_structure: DAGStructure) -> TransformResult:
        """执行DAG转换"""
        self.transformations = []
        errors = []
        warnings = []
        
        code = dag_structure.source_code
        lines = code.split('\n')
        
        try:
            # 1. 转换导入语句
            code, import_transforms = self._transform_imports(code, dag_structure.imports)
            self.transformations.extend(import_transforms)
            
            # 2. 转换类名
            code, class_transforms = self._transform_class_names(code)
            self.transformations.extend(class_transforms)
            
            # 3. 转换DAG参数
            code, dag_transforms = self._transform_dag_params(code, dag_structure.dag_config)
            self.transformations.extend(dag_transforms)
            
            # 4. 转换操作符参数
            code, op_transforms = self._transform_operator_params(code, dag_structure.operators)
            self.transformations.extend(op_transforms)
            
            # 5. 移除已弃用的参数
            code, deprecated_transforms = self._remove_deprecated_params(code)
            self.transformations.extend(deprecated_transforms)
            
            # 6. 添加必要的新导入
            code, new_import_transforms = self._add_required_imports(code, dag_structure)
            self.transformations.extend(new_import_transforms)
            
            # 生成警告
            warnings = self._generate_warnings(dag_structure)
            
            return TransformResult(
                success=True,
                transformed_code=code,
                transformations=self.transformations,
                errors=errors,
                warnings=warnings
            )
            
        except Exception as e:
            errors.append(f"转换错误: {str(e)}")
            return TransformResult(
                success=False,
                transformed_code=dag_structure.source_code,
                transformations=self.transformations,
                errors=errors,
                warnings=warnings
            )
    
    def _transform_imports(self, code: str, imports: List[ImportInfo]) -> Tuple[str, List[Transformation]]:
        """转换导入语句"""
        transforms = []
        
        for imp in imports:
            if imp.module in self.import_mappings:
                new_module = self.import_mappings[imp.module]
                
                if new_module is None:
                    # 模块已移除
                    transforms.append(Transformation(
                        rule_name="import_removed",
                        description=f"模块 {imp.module} 在Airflow 3.x中已移除",
                        line_number=imp.line_number,
                        old_code=imp.module,
                        new_code="# REMOVED: " + imp.module,
                        category="import"
                    ))
                    # 注释掉该导入
                    if imp.is_from_import:
                        old_import = f"from {imp.module} import"
                        new_import = f"# REMOVED in Airflow 3.x: from {imp.module} import"
                    else:
                        old_import = f"import {imp.module}"
                        new_import = f"# REMOVED in Airflow 3.x: import {imp.module}"
                    code = code.replace(old_import, new_import, 1)
                else:
                    # 模块路径变更
                    transforms.append(Transformation(
                        rule_name="import_migration",
                        description=f"导入路径从 {imp.module} 迁移到 {new_module}",
                        line_number=imp.line_number,
                        old_code=imp.module,
                        new_code=new_module,
                        category="import"
                    ))
                    code = code.replace(imp.module, new_module)
        
        return code, transforms
    
    def _transform_class_names(self, code: str) -> Tuple[str, List[Transformation]]:
        """转换类名"""
        transforms = []
        
        for old_name, new_name in self.class_mappings.items():
            if old_name in code:
                # 使用正则确保是完整的类名匹配
                pattern = r'\b' + re.escape(old_name) + r'\b'
                if re.search(pattern, code):
                    transforms.append(Transformation(
                        rule_name="class_rename",
                        description=f"类名从 {old_name} 重命名为 {new_name}",
                        line_number=0,
                        old_code=old_name,
                        new_code=new_name,
                        category="class"
                    ))
                    code = re.sub(pattern, new_name, code)
        
        return code, transforms
    
    def _transform_dag_params(self, code: str, dag_config) -> Tuple[str, List[Transformation]]:
        """转换DAG参数"""
        transforms = []
        
        if dag_config is None:
            return code, transforms
        
        # schedule_interval -> schedule
        if dag_config.schedule_interval and not dag_config.schedule:
            pattern = r'schedule_interval\s*='
            if re.search(pattern, code):
                transforms.append(Transformation(
                    rule_name="dag_param_rename",
                    description="参数 schedule_interval 重命名为 schedule",
                    line_number=dag_config.line_number,
                    old_code="schedule_interval",
                    new_code="schedule",
                    category="dag_config"
                ))
                code = re.sub(r'\bschedule_interval\b', 'schedule', code)
        
        # concurrency -> max_active_tasks
        pattern = r'(?<![a-zA-Z_])concurrency\s*='
        if re.search(pattern, code):
            transforms.append(Transformation(
                rule_name="dag_param_rename",
                description="参数 concurrency 重命名为 max_active_tasks",
                line_number=0,
                old_code="concurrency",
                new_code="max_active_tasks",
                category="dag_config"
            ))
            code = re.sub(r'(?<![a-zA-Z_])concurrency(?=\s*=)', 'max_active_tasks', code)
        
        return code, transforms
    
    def _transform_operator_params(self, code: str, operators: List[OperatorInfo]) -> Tuple[str, List[Transformation]]:
        """转换操作符参数"""
        transforms = []
        
        # provide_context 参数在 Airflow 2.0+ 中已自动提供
        pattern = r',?\s*provide_context\s*=\s*(True|False)'
        matches = re.finditer(pattern, code)
        for match in matches:
            transforms.append(Transformation(
                rule_name="remove_deprecated_param",
                description="移除已弃用的 provide_context 参数 (Airflow 2.0+ 自动提供)",
                line_number=0,
                old_code=match.group(0),
                new_code="",
                category="operator_param"
            ))
        code = re.sub(pattern, '', code)
        
        # task_concurrency -> max_active_tis_per_dag
        pattern = r'\btask_concurrency\b'
        if re.search(pattern, code):
            transforms.append(Transformation(
                rule_name="operator_param_rename",
                description="参数 task_concurrency 重命名为 max_active_tis_per_dag",
                line_number=0,
                old_code="task_concurrency",
                new_code="max_active_tis_per_dag",
                category="operator_param"
            ))
            code = re.sub(pattern, 'max_active_tis_per_dag', code)
        
        return code, transforms
    
    def _remove_deprecated_params(self, code: str) -> Tuple[str, List[Transformation]]:
        """移除已弃用的参数"""
        transforms = []
        
        # 移除 execution_date 相关的旧用法提示
        # 这里主要是添加警告,不直接修改代码
        
        return code, transforms
    
    def _add_required_imports(self, code: str, dag_structure: DAGStructure) -> Tuple[str, List[Transformation]]:
        """添加必要的新导入"""
        transforms = []
        lines = code.split('\n')
        
        # 检查是否需要添加 EmptyOperator 导入
        if 'EmptyOperator' in code:
            has_empty_import = any(
                'from airflow.operators.empty import EmptyOperator' in line or
                'from airflow.operators.empty import' in line and 'EmptyOperator' in line
                for line in lines
            )
            if not has_empty_import:
                # 找到最后一个 import 语句的位置
                last_import_idx = 0
                for i, line in enumerate(lines):
                    if line.strip().startswith('import ') or line.strip().startswith('from '):
                        last_import_idx = i
                
                new_import = "from airflow.operators.empty import EmptyOperator"
                lines.insert(last_import_idx + 1, new_import)
                
                transforms.append(Transformation(
                    rule_name="add_import",
                    description="添加 EmptyOperator 导入",
                    line_number=last_import_idx + 1,
                    old_code="",
                    new_code=new_import,
                    category="import"
                ))
                
                code = '\n'.join(lines)
        
        return code, transforms
    
    def _generate_warnings(self, dag_structure: DAGStructure) -> List[str]:
        """生成警告信息"""
        warnings = []
        
        # 检查 SubDagOperator 使用
        for op in dag_structure.operators:
            if 'SubDag' in op.name:
                warnings.append(
                    f"警告: SubDagOperator 在 Airflow 3.x 中已移除。"
                    f"请考虑使用 TaskGroup 替代。(行 {op.line_number})"
                )
        
        # 检查旧版 execution_date 用法
        if 'execution_date' in dag_structure.source_code:
            warnings.append(
                "警告: execution_date 在 Airflow 3.x 中已被 logical_date 替代。"
                "请检查代码中的 execution_date 引用。"
            )
        
        # 检查 XCom 用法
        if 'xcom_push' in dag_structure.source_code or 'xcom_pull' in dag_structure.source_code:
            warnings.append(
                "提示: 建议使用 TaskFlow API (@task 装饰器) 来简化 XCom 操作。"
            )
        
        # 检查 Variable 用法
        if dag_structure.variables:
            warnings.append(
                "提示: 在 DAG 解析时使用 Variable.get() 可能影响性能。"
                "考虑使用 Jinja 模板 {{ var.value.key }} 或在任务内部获取变量。"
            )
        
        return warnings
    
    def get_transformation_summary(self) -> Dict[str, int]:
        """获取转换摘要"""
        summary = {}
        for t in self.transformations:
            category = t.category
            summary[category] = summary.get(category, 0) + 1
        return summary
