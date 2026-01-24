# -*- coding: utf-8 -*-
"""
导入语句转换规则 - Airflow 2.x to 3.x
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set


@dataclass
class ImportMapping:
    """导入映射"""
    old_module: str
    new_module: Optional[str]
    old_names: List[str] = field(default_factory=list)
    new_names: List[str] = field(default_factory=list)
    provider_package: Optional[str] = None
    notes: str = ""


class ImportRules:
    """导入语句转换规则"""
    
    # 模块路径映射
    MODULE_MAPPINGS: Dict[str, Optional[str]] = {
        # 核心操作符
        'airflow.operators.python_operator': 'airflow.operators.python',
        'airflow.operators.bash_operator': 'airflow.operators.bash',
        'airflow.operators.dummy_operator': 'airflow.operators.empty',
        'airflow.operators.dummy': 'airflow.operators.empty',
        'airflow.operators.email_operator': 'airflow.operators.email',
        'airflow.operators.subdag_operator': None,  # 已移除
        'airflow.operators.latest_only_operator': 'airflow.operators.latest_only',
        'airflow.operators.branch_operator': 'airflow.operators.python',
        
        # Sensors
        'airflow.sensors.base_sensor_operator': 'airflow.sensors.base',
        'airflow.sensors.external_task_sensor': 'airflow.sensors.external_task',
        'airflow.sensors.time_sensor': 'airflow.sensors.time_sensor',
        'airflow.sensors.sql_sensor': 'airflow.sensors.sql',
        
        # Hooks
        'airflow.hooks.base_hook': 'airflow.hooks.base',
        'airflow.hooks.dbapi_hook': 'airflow.hooks.dbapi',
        'airflow.hooks.http_hook': 'airflow.providers.http.hooks.http',
        
        # 数据库操作符 -> providers
        'airflow.operators.postgres_operator': 'airflow.providers.postgres.operators.postgres',
        'airflow.operators.mysql_operator': 'airflow.providers.mysql.operators.mysql',
        'airflow.operators.mssql_operator': 'airflow.providers.microsoft.mssql.operators.mssql',
        'airflow.operators.oracle_operator': 'airflow.providers.oracle.operators.oracle',
        'airflow.operators.jdbc_operator': 'airflow.providers.jdbc.operators.jdbc',
        'airflow.operators.sqlite_operator': 'airflow.providers.sqlite.operators.sqlite',
        
        # 数据库 Hooks -> providers
        'airflow.hooks.postgres_hook': 'airflow.providers.postgres.hooks.postgres',
        'airflow.hooks.mysql_hook': 'airflow.providers.mysql.hooks.mysql',
        'airflow.hooks.mssql_hook': 'airflow.providers.microsoft.mssql.hooks.mssql',
        'airflow.hooks.oracle_hook': 'airflow.providers.oracle.hooks.oracle',
        'airflow.hooks.jdbc_hook': 'airflow.providers.jdbc.hooks.jdbc',
        'airflow.hooks.sqlite_hook': 'airflow.providers.sqlite.hooks.sqlite',
        
        # contrib -> providers (Google Cloud)
        'airflow.contrib.operators.bigquery_operator': 'airflow.providers.google.cloud.operators.bigquery',
        'airflow.contrib.operators.bigquery_to_gcs': 'airflow.providers.google.cloud.transfers.bigquery_to_gcs',
        'airflow.contrib.operators.bigquery_to_bigquery': 'airflow.providers.google.cloud.transfers.bigquery_to_bigquery',
        'airflow.contrib.operators.gcs_to_bq': 'airflow.providers.google.cloud.transfers.gcs_to_bigquery',
        'airflow.contrib.operators.gcs_to_gcs': 'airflow.providers.google.cloud.transfers.gcs_to_gcs',
        'airflow.contrib.operators.gcs_operator': 'airflow.providers.google.cloud.operators.gcs',
        'airflow.contrib.operators.dataproc_operator': 'airflow.providers.google.cloud.operators.dataproc',
        'airflow.contrib.operators.dataflow_operator': 'airflow.providers.google.cloud.operators.dataflow',
        'airflow.contrib.operators.pubsub_operator': 'airflow.providers.google.cloud.operators.pubsub',
        'airflow.contrib.operators.gcp_sql_operator': 'airflow.providers.google.cloud.operators.cloud_sql',
        'airflow.contrib.hooks.bigquery_hook': 'airflow.providers.google.cloud.hooks.bigquery',
        'airflow.contrib.hooks.gcs_hook': 'airflow.providers.google.cloud.hooks.gcs',
        'airflow.contrib.hooks.dataproc_hook': 'airflow.providers.google.cloud.hooks.dataproc',
        'airflow.contrib.sensors.bigquery_sensor': 'airflow.providers.google.cloud.sensors.bigquery',
        'airflow.contrib.sensors.gcs_sensor': 'airflow.providers.google.cloud.sensors.gcs',
        
        # contrib -> providers (AWS)
        'airflow.contrib.operators.aws_athena_operator': 'airflow.providers.amazon.aws.operators.athena',
        'airflow.contrib.operators.s3_to_redshift_operator': 'airflow.providers.amazon.aws.transfers.s3_to_redshift',
        'airflow.contrib.operators.redshift_to_s3_operator': 'airflow.providers.amazon.aws.transfers.redshift_to_s3',
        'airflow.contrib.operators.s3_copy_object_operator': 'airflow.providers.amazon.aws.operators.s3',
        'airflow.contrib.operators.s3_delete_objects_operator': 'airflow.providers.amazon.aws.operators.s3',
        'airflow.contrib.operators.s3_list_operator': 'airflow.providers.amazon.aws.operators.s3',
        'airflow.contrib.operators.emr_add_steps_operator': 'airflow.providers.amazon.aws.operators.emr',
        'airflow.contrib.operators.emr_create_job_flow_operator': 'airflow.providers.amazon.aws.operators.emr',
        'airflow.contrib.operators.emr_terminate_job_flow_operator': 'airflow.providers.amazon.aws.operators.emr',
        'airflow.contrib.operators.ecs_operator': 'airflow.providers.amazon.aws.operators.ecs',
        'airflow.contrib.operators.sagemaker_operator': 'airflow.providers.amazon.aws.operators.sagemaker',
        'airflow.contrib.hooks.aws_hook': 'airflow.providers.amazon.aws.hooks.base_aws',
        'airflow.contrib.hooks.s3_hook': 'airflow.providers.amazon.aws.hooks.s3',
        'airflow.contrib.hooks.redshift_hook': 'airflow.providers.amazon.aws.hooks.redshift_sql',
        'airflow.contrib.hooks.emr_hook': 'airflow.providers.amazon.aws.hooks.emr',
        'airflow.contrib.sensors.s3_key_sensor': 'airflow.providers.amazon.aws.sensors.s3',
        'airflow.contrib.sensors.s3_prefix_sensor': 'airflow.providers.amazon.aws.sensors.s3',
        'airflow.contrib.sensors.emr_step_sensor': 'airflow.providers.amazon.aws.sensors.emr',
        
        # contrib -> providers (Azure)
        'airflow.contrib.operators.wasb_delete_blob_operator': 'airflow.providers.microsoft.azure.operators.wasb_delete_blob',
        'airflow.contrib.operators.azure_container_instances_operator': 'airflow.providers.microsoft.azure.operators.container_instances',
        'airflow.contrib.hooks.wasb_hook': 'airflow.providers.microsoft.azure.hooks.wasb',
        'airflow.contrib.hooks.azure_data_lake_hook': 'airflow.providers.microsoft.azure.hooks.data_lake',
        
        # contrib -> providers (其他)
        'airflow.contrib.operators.slack_webhook_operator': 'airflow.providers.slack.operators.slack_webhook',
        'airflow.contrib.operators.ssh_operator': 'airflow.providers.ssh.operators.ssh',
        'airflow.contrib.operators.sftp_operator': 'airflow.providers.sftp.operators.sftp',
        'airflow.contrib.operators.spark_submit_operator': 'airflow.providers.apache.spark.operators.spark_submit',
        'airflow.contrib.operators.spark_sql_operator': 'airflow.providers.apache.spark.operators.spark_sql',
        'airflow.contrib.operators.kubernetes_pod_operator': 'airflow.providers.cncf.kubernetes.operators.pod',
        'airflow.contrib.operators.docker_operator': 'airflow.providers.docker.operators.docker',
        'airflow.contrib.hooks.slack_webhook_hook': 'airflow.providers.slack.hooks.slack_webhook',
        'airflow.contrib.hooks.ssh_hook': 'airflow.providers.ssh.hooks.ssh',
        'airflow.contrib.hooks.sftp_hook': 'airflow.providers.sftp.hooks.sftp',
        'airflow.contrib.hooks.spark_submit_hook': 'airflow.providers.apache.spark.hooks.spark_submit',
    }
    
    # 类名映射 (同一模块内的类名变更)
    CLASS_MAPPINGS: Dict[str, str] = {
        'DummyOperator': 'EmptyOperator',
        'BaseSensorOperator': 'BaseSensor',
        'BigQueryOperator': 'BigQueryInsertJobOperator',
        'GoogleCloudStorageToBigQueryOperator': 'GCSToBigQueryOperator',
        'GoogleCloudStorageToGoogleCloudStorageOperator': 'GCSToGCSOperator',
        'S3KeySensor': 'S3KeySensor',  # 保持不变但模块变了
    }
    
    # Provider 包依赖
    PROVIDER_PACKAGES: Dict[str, str] = {
        'airflow.providers.google': 'apache-airflow-providers-google',
        'airflow.providers.amazon': 'apache-airflow-providers-amazon',
        'airflow.providers.microsoft.azure': 'apache-airflow-providers-microsoft-azure',
        'airflow.providers.microsoft.mssql': 'apache-airflow-providers-microsoft-mssql',
        'airflow.providers.postgres': 'apache-airflow-providers-postgres',
        'airflow.providers.mysql': 'apache-airflow-providers-mysql',
        'airflow.providers.oracle': 'apache-airflow-providers-oracle',
        'airflow.providers.jdbc': 'apache-airflow-providers-jdbc',
        'airflow.providers.sqlite': 'apache-airflow-providers-common-sql',
        'airflow.providers.slack': 'apache-airflow-providers-slack',
        'airflow.providers.ssh': 'apache-airflow-providers-ssh',
        'airflow.providers.sftp': 'apache-airflow-providers-sftp',
        'airflow.providers.apache.spark': 'apache-airflow-providers-apache-spark',
        'airflow.providers.cncf.kubernetes': 'apache-airflow-providers-cncf-kubernetes',
        'airflow.providers.docker': 'apache-airflow-providers-docker',
        'airflow.providers.http': 'apache-airflow-providers-http',
    }
    
    @classmethod
    def get_new_module(cls, old_module: str) -> Optional[str]:
        """获取新的模块路径"""
        return cls.MODULE_MAPPINGS.get(old_module)
    
    @classmethod
    def get_new_class_name(cls, old_name: str) -> Optional[str]:
        """获取新的类名"""
        return cls.CLASS_MAPPINGS.get(old_name)
    
    @classmethod
    def is_deprecated(cls, module: str) -> bool:
        """检查模块是否已弃用"""
        return module in cls.MODULE_MAPPINGS
    
    @classmethod
    def is_removed(cls, module: str) -> bool:
        """检查模块是否已移除"""
        return cls.MODULE_MAPPINGS.get(module) is None and module in cls.MODULE_MAPPINGS
    
    @classmethod
    def get_required_provider(cls, new_module: str) -> Optional[str]:
        """获取所需的 provider 包"""
        for prefix, package in cls.PROVIDER_PACKAGES.items():
            if new_module.startswith(prefix):
                return package
        return None
    
    @classmethod
    def get_all_required_providers(cls, modules: List[str]) -> Set[str]:
        """获取所有需要的 provider 包"""
        providers = set()
        for module in modules:
            new_module = cls.get_new_module(module)
            if new_module:
                provider = cls.get_required_provider(new_module)
                if provider:
                    providers.add(provider)
        return providers
    
    @classmethod
    def transform_import_statement(cls, old_import: str) -> Optional[str]:
        """转换导入语句"""
        # 解析 from ... import ... 格式
        if old_import.strip().startswith('from '):
            parts = old_import.strip().split(' import ')
            if len(parts) == 2:
                module = parts[0].replace('from ', '').strip()
                names = parts[1].strip()
                
                new_module = cls.get_new_module(module)
                if new_module is None:
                    return f"# REMOVED: {old_import}"
                elif new_module:
                    # 检查类名是否需要更新
                    new_names = []
                    for name in names.split(','):
                        name = name.strip()
                        new_name = cls.get_new_class_name(name)
                        new_names.append(new_name if new_name else name)
                    
                    return f"from {new_module} import {', '.join(new_names)}"
        
        # 解析 import ... 格式
        elif old_import.strip().startswith('import '):
            module = old_import.strip().replace('import ', '').strip()
            new_module = cls.get_new_module(module)
            if new_module is None:
                return f"# REMOVED: {old_import}"
            elif new_module:
                return f"import {new_module}"
        
        return None
    
    @classmethod
    def get_import_mappings(cls) -> List[ImportMapping]:
        """获取所有导入映射"""
        mappings = []
        for old_module, new_module in cls.MODULE_MAPPINGS.items():
            provider = cls.get_required_provider(new_module) if new_module else None
            mappings.append(ImportMapping(
                old_module=old_module,
                new_module=new_module,
                provider_package=provider
            ))
        return mappings
    
    @classmethod
    def generate_requirements(cls, modules: List[str]) -> List[str]:
        """生成 requirements.txt 内容"""
        providers = cls.get_all_required_providers(modules)
        requirements = ['apache-airflow>=2.7.0']
        requirements.extend(sorted(providers))
        return requirements
