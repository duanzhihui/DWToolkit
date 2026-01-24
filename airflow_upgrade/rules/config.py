# -*- coding: utf-8 -*-
"""
配置更新规则 - Airflow 2.x to 3.x
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any


@dataclass
class ParamChange:
    """参数变更"""
    old_name: str
    new_name: Optional[str]
    transform: Optional[str] = None  # 值转换说明
    notes: str = ""


@dataclass
class ConfigChange:
    """配置变更"""
    category: str
    changes: List[ParamChange] = field(default_factory=list)


class ConfigRules:
    """配置更新规则"""
    
    # DAG 参数变更
    DAG_PARAM_CHANGES: Dict[str, Optional[str]] = {
        'schedule_interval': 'schedule',
        'concurrency': 'max_active_tasks',
        'max_active_runs': 'max_active_runs',  # 保持不变
        'dagrun_timeout': 'dagrun_timeout',    # 保持不变
        'sla_miss_callback': 'sla_miss_callback',  # 保持不变
        'default_view': 'default_view',        # 保持不变
        'orientation': 'orientation',          # 保持不变
    }
    
    # DAG 已移除的参数
    DAG_REMOVED_PARAMS: List[str] = [
        'full_filepath',  # 在 2.x 中已弃用
        'is_paused_upon_creation',  # 使用 airflow.cfg 配置
    ]
    
    # BaseOperator 参数变更
    OPERATOR_PARAM_CHANGES: Dict[str, Optional[str]] = {
        'task_concurrency': 'max_active_tis_per_dag',
        'provide_context': None,  # 已移除,自动提供
        'resources': None,        # 已移除
    }
    
    # Context 变量变更 (execution_date 等)
    CONTEXT_CHANGES: Dict[str, str] = {
        'execution_date': 'logical_date',
        'next_execution_date': 'data_interval_end',
        'prev_execution_date': 'data_interval_start',
        'next_ds': 'ds',  # 需要使用 data_interval_end
        'prev_ds': 'ds',  # 需要使用 data_interval_start
        'tomorrow_ds': 'ds',  # 需要手动计算
        'yesterday_ds': 'ds',  # 需要手动计算
    }
    
    # Jinja 模板变量变更
    JINJA_TEMPLATE_CHANGES: Dict[str, str] = {
        '{{ execution_date }}': '{{ logical_date }}',
        '{{ next_execution_date }}': '{{ data_interval_end }}',
        '{{ prev_execution_date }}': '{{ data_interval_start }}',
        '{{ ds }}': '{{ ds }}',  # 保持不变
        '{{ ts }}': '{{ ts }}',  # 保持不变
        '{{ next_ds }}': '{{ data_interval_end | ds }}',
        '{{ prev_ds }}': '{{ data_interval_start | ds }}',
    }
    
    # airflow.cfg 配置变更
    AIRFLOW_CFG_CHANGES: Dict[str, Dict[str, str]] = {
        'core': {
            'executor': 'executor',  # 保持不变
            'sql_alchemy_conn': 'sql_alchemy_conn',  # 保持不变
            'parallelism': 'parallelism',  # 保持不变
            'dag_concurrency': 'max_active_tasks_per_dag',
            'non_pooled_task_slot_count': None,  # 已移除
            'dag_file_processor_timeout': 'dag_file_processor_timeout',
            'min_file_process_interval': 'min_file_process_interval',
        },
        'scheduler': {
            'job_heartbeat_sec': 'job_heartbeat_sec',
            'scheduler_heartbeat_sec': 'scheduler_heartbeat_sec',
            'num_runs': 'num_runs',
            'processor_poll_interval': 'processor_poll_interval',
            'min_file_parsing_loop_time': 'min_file_parsing_loop_time',
            'statsd_on': None,  # 移到 metrics 部分
        },
        'webserver': {
            'base_url': 'base_url',
            'web_server_host': 'web_server_host',
            'web_server_port': 'web_server_port',
            'authenticate': None,  # 使用 auth_backend
            'filter_by_owner': None,  # 已移除
        },
    }
    
    # 连接类型变更
    CONNECTION_TYPE_CHANGES: Dict[str, str] = {
        'bigquery': 'google_cloud_platform',
        'google_cloud_storage': 'google_cloud_platform',
        'dataproc': 'google_cloud_platform',
        's3': 'aws',
        'redshift': 'redshift',
        'emr': 'aws',
    }
    
    @classmethod
    def get_dag_param_change(cls, old_param: str) -> Optional[str]:
        """获取 DAG 参数变更"""
        return cls.DAG_PARAM_CHANGES.get(old_param)
    
    @classmethod
    def is_dag_param_removed(cls, param: str) -> bool:
        """检查 DAG 参数是否已移除"""
        return param in cls.DAG_REMOVED_PARAMS
    
    @classmethod
    def get_operator_param_change(cls, old_param: str) -> Optional[str]:
        """获取操作符参数变更"""
        return cls.OPERATOR_PARAM_CHANGES.get(old_param)
    
    @classmethod
    def get_context_change(cls, old_key: str) -> Optional[str]:
        """获取 context 变量变更"""
        return cls.CONTEXT_CHANGES.get(old_key)
    
    @classmethod
    def get_jinja_change(cls, old_template: str) -> Optional[str]:
        """获取 Jinja 模板变更"""
        return cls.JINJA_TEMPLATE_CHANGES.get(old_template)
    
    @classmethod
    def transform_dag_params(cls, params: Dict[str, Any]) -> Dict[str, Any]:
        """转换 DAG 参数"""
        new_params = {}
        
        for key, value in params.items():
            # 检查是否已移除
            if cls.is_dag_param_removed(key):
                continue
            
            # 检查是否需要重命名
            new_key = cls.get_dag_param_change(key)
            if new_key is None:
                continue  # 参数已移除
            elif new_key:
                new_params[new_key] = value
            else:
                new_params[key] = value
        
        return new_params
    
    @classmethod
    def get_all_changes(cls) -> List[ConfigChange]:
        """获取所有配置变更"""
        changes = []
        
        # DAG 参数变更
        dag_changes = ConfigChange(category='DAG')
        for old, new in cls.DAG_PARAM_CHANGES.items():
            if old != new:
                dag_changes.changes.append(ParamChange(
                    old_name=old,
                    new_name=new,
                    notes='参数重命名' if new else '参数已移除'
                ))
        changes.append(dag_changes)
        
        # 操作符参数变更
        op_changes = ConfigChange(category='Operator')
        for old, new in cls.OPERATOR_PARAM_CHANGES.items():
            op_changes.changes.append(ParamChange(
                old_name=old,
                new_name=new,
                notes='参数重命名' if new else '参数已移除'
            ))
        changes.append(op_changes)
        
        # Context 变更
        ctx_changes = ConfigChange(category='Context')
        for old, new in cls.CONTEXT_CHANGES.items():
            ctx_changes.changes.append(ParamChange(
                old_name=old,
                new_name=new,
                notes='变量重命名'
            ))
        changes.append(ctx_changes)
        
        return changes
    
    @classmethod
    def generate_migration_checklist(cls) -> str:
        """生成迁移检查清单"""
        checklist = """
# Airflow 2.x to 3.x 迁移检查清单

## DAG 配置
- [ ] `schedule_interval` 更新为 `schedule`
- [ ] `concurrency` 更新为 `max_active_tasks`
- [ ] 移除已弃用的参数

## 操作符参数
- [ ] 移除 `provide_context` 参数
- [ ] `task_concurrency` 更新为 `max_active_tis_per_dag`
- [ ] 移除 `resources` 参数

## Context 变量
- [ ] `execution_date` 更新为 `logical_date`
- [ ] `next_execution_date` 更新为 `data_interval_end`
- [ ] `prev_execution_date` 更新为 `data_interval_start`

## Jinja 模板
- [ ] 更新模板中的日期变量引用
- [ ] 检查自定义宏的兼容性

## 导入语句
- [ ] 更新 `airflow.contrib` 到 `airflow.providers`
- [ ] 更新操作符导入路径
- [ ] 安装所需的 provider 包

## 连接配置
- [ ] 检查连接类型是否需要更新
- [ ] 验证连接参数格式

## 测试验证
- [ ] 运行 DAG 语法检查
- [ ] 验证任务依赖关系
- [ ] 测试 XCom 数据传递
- [ ] 验证调度行为
"""
        return checklist
