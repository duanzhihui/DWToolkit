# -*- coding: utf-8 -*-
"""
DAG迁移器 - 管理版本迁移流程
"""

import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from airflowupdt.core.parser import DAGParser, DAGStructure
from airflowupdt.core.transformer import DAGTransformer, TransformResult
from airflowupdt.core.validator import DAGValidator, ValidationResult


@dataclass
class MigrationReport:
    """迁移报告"""
    file_path: str
    source_version: Optional[str]
    target_version: str
    success: bool
    transform_result: Optional[TransformResult] = None
    validation_result: Optional[ValidationResult] = None
    backup_path: Optional[str] = None
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class BatchMigrationReport:
    """批量迁移报告"""
    total_files: int = 0
    successful: int = 0
    failed: int = 0
    skipped: int = 0
    reports: List[MigrationReport] = field(default_factory=list)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    @property
    def success_rate(self) -> float:
        if self.total_files == 0:
            return 0.0
        return (self.successful / self.total_files) * 100


class DAGMigrator:
    """DAG迁移器"""
    
    def __init__(
        self,
        target_version: str = "3.0",
        backup_enabled: bool = True,
        backup_dir: Optional[str] = None,
        dry_run: bool = False
    ):
        self.target_version = target_version
        self.backup_enabled = backup_enabled
        self.backup_dir = backup_dir
        self.dry_run = dry_run
        
        self.parser = DAGParser()
        self.transformer = DAGTransformer(target_version)
        self.validator = DAGValidator()
    
    def migrate_file(self, file_path: str) -> MigrationReport:
        """迁移单个DAG文件"""
        report = MigrationReport(
            file_path=file_path,
            source_version=None,
            target_version=self.target_version,
            success=False
        )
        
        path = Path(file_path)
        
        # 检查文件存在
        if not path.exists():
            report.errors.append(f"文件不存在: {file_path}")
            return report
        
        if not path.suffix == '.py':
            report.errors.append(f"不是Python文件: {file_path}")
            return report
        
        try:
            # 1. 解析DAG
            dag_structure = self.parser.parse_file(file_path)
            report.source_version = dag_structure.airflow_version
            
            if dag_structure.issues:
                report.errors.extend(dag_structure.issues)
                return report
            
            # 2. 转换DAG
            transform_result = self.transformer.transform(dag_structure)
            report.transform_result = transform_result
            
            if not transform_result.success:
                report.errors.extend(transform_result.errors)
                return report
            
            report.warnings.extend(transform_result.warnings)
            
            # 3. 验证转换后的代码
            validation_result = self.validator.validate_code(transform_result.transformed_code)
            report.validation_result = validation_result
            
            if not validation_result.is_valid:
                report.errors.extend([f"验证错误: {e}" for e in validation_result.errors])
                return report
            
            report.warnings.extend(validation_result.warnings)
            
            # 4. 备份原文件
            if self.backup_enabled and not self.dry_run:
                backup_path = self._create_backup(path)
                report.backup_path = str(backup_path)
            
            # 5. 写入转换后的代码
            if not self.dry_run:
                path.write_text(transform_result.transformed_code, encoding='utf-8')
            
            report.success = True
            
        except Exception as e:
            report.errors.append(f"迁移错误: {str(e)}")
        
        return report
    
    def migrate_directory(
        self,
        directory: str,
        recursive: bool = True,
        pattern: str = "*.py",
        exclude_patterns: Optional[List[str]] = None
    ) -> BatchMigrationReport:
        """批量迁移目录中的DAG文件"""
        batch_report = BatchMigrationReport()
        
        dir_path = Path(directory)
        if not dir_path.exists():
            batch_report.reports.append(MigrationReport(
                file_path=directory,
                source_version=None,
                target_version=self.target_version,
                success=False,
                errors=[f"目录不存在: {directory}"]
            ))
            return batch_report
        
        # 收集文件
        if recursive:
            files = list(dir_path.rglob(pattern))
        else:
            files = list(dir_path.glob(pattern))
        
        # 排除模式
        exclude_patterns = exclude_patterns or ['__pycache__', '.git', 'test_*', '*_test.py']
        filtered_files = []
        for f in files:
            skip = False
            for exc in exclude_patterns:
                if exc in str(f):
                    skip = True
                    break
            if not skip:
                filtered_files.append(f)
        
        batch_report.total_files = len(filtered_files)
        
        # 迁移每个文件
        for file_path in filtered_files:
            # 检查是否是DAG文件
            if not self._is_dag_file(file_path):
                batch_report.skipped += 1
                continue
            
            report = self.migrate_file(str(file_path))
            batch_report.reports.append(report)
            
            if report.success:
                batch_report.successful += 1
            else:
                batch_report.failed += 1
        
        return batch_report
    
    def analyze_file(self, file_path: str) -> MigrationReport:
        """仅分析文件,不进行实际迁移"""
        original_dry_run = self.dry_run
        self.dry_run = True
        
        report = self.migrate_file(file_path)
        
        self.dry_run = original_dry_run
        return report
    
    def analyze_directory(
        self,
        directory: str,
        recursive: bool = True
    ) -> BatchMigrationReport:
        """仅分析目录,不进行实际迁移"""
        original_dry_run = self.dry_run
        self.dry_run = True
        
        report = self.migrate_directory(directory, recursive)
        
        self.dry_run = original_dry_run
        return report
    
    def _create_backup(self, file_path: Path) -> Path:
        """创建文件备份"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if self.backup_dir:
            backup_dir = Path(self.backup_dir)
            backup_dir.mkdir(parents=True, exist_ok=True)
            backup_path = backup_dir / f"{file_path.stem}_{timestamp}{file_path.suffix}.bak"
        else:
            backup_path = file_path.with_suffix(f".{timestamp}.bak")
        
        shutil.copy2(file_path, backup_path)
        return backup_path
    
    def _is_dag_file(self, file_path: Path) -> bool:
        """检查是否是DAG文件"""
        try:
            content = file_path.read_text(encoding='utf-8')
            # 检查是否包含DAG相关的导入或定义
            dag_indicators = [
                'from airflow import DAG',
                'from airflow.models import DAG',
                'from airflow.decorators import dag',
                '@dag',
                'DAG(',
            ]
            return any(indicator in content for indicator in dag_indicators)
        except Exception:
            return False
    
    def rollback(self, backup_path: str, original_path: str) -> bool:
        """回滚到备份版本"""
        try:
            backup = Path(backup_path)
            original = Path(original_path)
            
            if not backup.exists():
                return False
            
            shutil.copy2(backup, original)
            return True
        except Exception:
            return False
    
    def generate_rollback_script(self, batch_report: BatchMigrationReport) -> str:
        """生成回滚脚本"""
        lines = [
            "#!/bin/bash",
            "# Airflow DAG迁移回滚脚本",
            f"# 生成时间: {batch_report.timestamp}",
            "",
            "set -e",
            "",
        ]
        
        for report in batch_report.reports:
            if report.success and report.backup_path:
                lines.append(f'echo "回滚: {report.file_path}"')
                lines.append(f'cp "{report.backup_path}" "{report.file_path}"')
                lines.append("")
        
        lines.append('echo "回滚完成"')
        
        return '\n'.join(lines)


def create_migrator(
    target_version: str = "3.0",
    backup: bool = True,
    backup_dir: Optional[str] = None,
    dry_run: bool = False
) -> DAGMigrator:
    """创建迁移器的工厂函数"""
    return DAGMigrator(
        target_version=target_version,
        backup_enabled=backup,
        backup_dir=backup_dir,
        dry_run=dry_run
    )
