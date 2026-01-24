# -*- coding: utf-8 -*-
"""
操作符转换规则 - Airflow 2.x to 3.x
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass
class OperatorMapping:
    """操作符映射"""
    old_name: str
    new_name: str
    old_module: str
    new_module: str
    param_changes: Dict[str, str] = field(default_factory=dict)
    removed_params: List[str] = field(default_factory=list)
    notes: str = ""


class OperatorRules:
    """操作符转换规则"""
    
    # 操作符重命名映射
    OPERATOR_RENAMES: Dict[str, str] = {
        'DummyOperator': 'EmptyOperator',
        'BigQueryOperator': 'BigQueryInsertJobOperator',
        'DataProcPySparkOperator': 'DataprocSubmitJobOperator',
        'DataProcSparkOperator': 'DataprocSubmitJobOperator',
        'DataProcHadoopOperator': 'DataprocSubmitJobOperator',
        'DataProcHiveOperator': 'DataprocSubmitJobOperator',
        'DataProcPigOperator': 'DataprocSubmitJobOperator',
        'GoogleCloudStorageToBigQueryOperator': 'GCSToBigQueryOperator',
        'GoogleCloudStorageToGoogleCloudStorageOperator': 'GCSToGCSOperator',
        'FileToGoogleCloudStorageOperator': 'LocalFilesystemToGCSOperator',
        'GoogleCloudStorageDownloadOperator': 'GCSToLocalFilesystemOperator',
        'MySqlToGoogleCloudStorageOperator': 'MySQLToGCSOperator',
        'S3ToRedshiftTransfer': 'S3ToRedshiftOperator',
        'RedshiftToS3Transfer': 'RedshiftToS3Operator',
    }
    
    # 完整的操作符映射 (包含模块路径)
    OPERATOR_MAPPINGS: List[OperatorMapping] = [
        # 核心操作符
        OperatorMapping(
            old_name='DummyOperator',
            new_name='EmptyOperator',
            old_module='airflow.operators.dummy',
            new_module='airflow.operators.empty',
            notes='DummyOperator 在 Airflow 2.4+ 中已弃用'
        ),
        OperatorMapping(
            old_name='DummyOperator',
            new_name='EmptyOperator',
            old_module='airflow.operators.dummy_operator',
            new_module='airflow.operators.empty',
            notes='旧版导入路径'
        ),
        
        # BigQuery 操作符
        OperatorMapping(
            old_name='BigQueryOperator',
            new_name='BigQueryInsertJobOperator',
            old_module='airflow.contrib.operators.bigquery_operator',
            new_module='airflow.providers.google.cloud.operators.bigquery',
            param_changes={
                'bql': 'sql',
                'destination_dataset_table': 'destination_table',
                'bigquery_conn_id': 'gcp_conn_id',
            },
            notes='BigQueryOperator 已重构为 BigQueryInsertJobOperator'
        ),
        
        # Dataproc 操作符
        OperatorMapping(
            old_name='DataProcPySparkOperator',
            new_name='DataprocSubmitJobOperator',
            old_module='airflow.contrib.operators.dataproc_operator',
            new_module='airflow.providers.google.cloud.operators.dataproc',
            notes='需要重构 job 参数结构'
        ),
        
        # GCS 操作符
        OperatorMapping(
            old_name='GoogleCloudStorageToBigQueryOperator',
            new_name='GCSToBigQueryOperator',
            old_module='airflow.contrib.operators.gcs_to_bq',
            new_module='airflow.providers.google.cloud.transfers.gcs_to_bigquery',
            param_changes={
                'google_cloud_storage_conn_id': 'gcp_conn_id',
            }
        ),
        
        # S3/Redshift 操作符
        OperatorMapping(
            old_name='S3ToRedshiftTransfer',
            new_name='S3ToRedshiftOperator',
            old_module='airflow.contrib.operators.s3_to_redshift_operator',
            new_module='airflow.providers.amazon.aws.transfers.s3_to_redshift',
        ),
        
        # SSH 操作符
        OperatorMapping(
            old_name='SSHOperator',
            new_name='SSHOperator',
            old_module='airflow.contrib.operators.ssh_operator',
            new_module='airflow.providers.ssh.operators.ssh',
        ),
        
        # Slack 操作符
        OperatorMapping(
            old_name='SlackWebhookOperator',
            new_name='SlackWebhookOperator',
            old_module='airflow.contrib.operators.slack_webhook_operator',
            new_module='airflow.providers.slack.operators.slack_webhook',
        ),
    ]
    
    # 已移除的操作符
    REMOVED_OPERATORS: Dict[str, str] = {
        'SubDagOperator': '使用 TaskGroup 替代',
        'SkipMixin': '已集成到 BaseOperator',
    }
    
    # 操作符参数变更
    PARAM_CHANGES: Dict[str, Dict[str, str]] = {
        'BaseOperator': {
            'task_concurrency': 'max_active_tis_per_dag',
            'execution_timeout': 'execution_timeout',  # 保持不变但类型可能变化
        },
        'PythonOperator': {
            'provide_context': None,  # 已移除,自动提供
        },
        'BranchPythonOperator': {
            'provide_context': None,
        },
        'BigQueryInsertJobOperator': {
            'bql': 'sql',
            'destination_dataset_table': 'destination_table',
            'bigquery_conn_id': 'gcp_conn_id',
        },
        'PostgresOperator': {
            'postgres_conn_id': 'postgres_conn_id',
            'sql': 'sql',
        },
    }
    
    # 已移除的参数
    REMOVED_PARAMS: Dict[str, List[str]] = {
        'PythonOperator': ['provide_context'],
        'BranchPythonOperator': ['provide_context'],
        'ShortCircuitOperator': ['provide_context'],
        'BaseOperator': ['resources'],
    }
    
    @classmethod
    def get_operator_mapping(cls, old_name: str) -> Optional[str]:
        """获取操作符新名称"""
        return cls.OPERATOR_RENAMES.get(old_name)
    
    @classmethod
    def get_full_mapping(cls, old_name: str, old_module: str) -> Optional[OperatorMapping]:
        """获取完整的操作符映射"""
        for mapping in cls.OPERATOR_MAPPINGS:
            if mapping.old_name == old_name and mapping.old_module == old_module:
                return mapping
        return None
    
    @classmethod
    def get_param_changes(cls, operator_name: str) -> Dict[str, Optional[str]]:
        """获取参数变更"""
        return cls.PARAM_CHANGES.get(operator_name, {})
    
    @classmethod
    def get_removed_params(cls, operator_name: str) -> List[str]:
        """获取已移除的参数"""
        return cls.REMOVED_PARAMS.get(operator_name, [])
    
    @classmethod
    def is_removed(cls, operator_name: str) -> Tuple[bool, str]:
        """检查操作符是否已移除"""
        if operator_name in cls.REMOVED_OPERATORS:
            return True, cls.REMOVED_OPERATORS[operator_name]
        return False, ""
    
    @classmethod
    def get_all_renames(cls) -> Dict[str, str]:
        """获取所有重命名映射"""
        return cls.OPERATOR_RENAMES.copy()
    
    @classmethod
    def get_migration_guide(cls, operator_name: str) -> str:
        """获取迁移指南"""
        guides = {
            'DummyOperator': """
DummyOperator -> EmptyOperator 迁移指南:
1. 更新导入: from airflow.operators.empty import EmptyOperator
2. 替换类名: DummyOperator -> EmptyOperator
3. 参数保持不变
""",
            'SubDagOperator': """
SubDagOperator 迁移指南:
SubDagOperator 在 Airflow 3.x 中已移除,请使用 TaskGroup 替代。

原代码:
    subdag = SubDagOperator(
        task_id='subdag_task',
        subdag=create_subdag(...)
    )

新代码:
    from airflow.utils.task_group import TaskGroup
    
    with TaskGroup(group_id='task_group') as tg:
        task1 = ...
        task2 = ...
        task1 >> task2
""",
            'BigQueryOperator': """
BigQueryOperator -> BigQueryInsertJobOperator 迁移指南:
1. 更新导入: from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
2. 参数变更:
   - bql -> sql
   - destination_dataset_table -> destination_table
   - bigquery_conn_id -> gcp_conn_id
3. 需要安装 apache-airflow-providers-google
""",
        }
        return guides.get(operator_name, f"暂无 {operator_name} 的迁移指南")
