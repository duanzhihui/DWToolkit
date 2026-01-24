# -*- coding: utf-8 -*-
"""
AirflowUpgrade - Airflow 2.x to 3.x DAG升级工具

自动化将Airflow 2.x DAG文件升级到Airflow 3.x兼容版本
"""

__version__ = "0.1.0"
__author__ = "DWToolkit"

from airflow_upgrade.core.parser import DAGParser
from airflow_upgrade.core.transformer import DAGTransformer
from airflow_upgrade.core.migrator import DAGMigrator
from airflow_upgrade.core.validator import DAGValidator

__all__ = [
    "DAGParser",
    "DAGTransformer",
    "DAGMigrator",
    "DAGValidator",
]
