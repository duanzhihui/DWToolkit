# -*- coding: utf-8 -*-
"""
DAG 迁移器测试
"""

import tempfile
from pathlib import Path

import pytest
from airflowupdt.core.migrator import DAGMigrator, MigrationReport, BatchMigrationReport


class TestDAGMigrator:
    """DAG 迁移器测试类"""
    
    @pytest.fixture
    def migrator(self):
        return DAGMigrator(target_version="3.0", backup_enabled=False, dry_run=True)
    
    @pytest.fixture
    def sample_dag_code(self):
        return '''
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

with DAG(
    dag_id='sample_dag',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1)
) as dag:
    task = DummyOperator(task_id='dummy_task')
'''
    
    def test_migrate_file_dry_run(self, migrator, sample_dag_code):
        """测试干运行模式"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as f:
            f.write(sample_dag_code)
            temp_path = f.name
        
        try:
            report = migrator.migrate_file(temp_path)
            
            assert isinstance(report, MigrationReport)
            assert report.file_path == temp_path
            
            # 干运行模式下文件不应被修改
            with open(temp_path, 'r', encoding='utf-8') as f:
                content = f.read()
            assert 'DummyOperator' in content  # 原内容未变
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def test_migrate_file_with_backup(self, sample_dag_code):
        """测试带备份的迁移"""
        migrator = DAGMigrator(target_version="3.0", backup_enabled=True, dry_run=False)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as f:
            f.write(sample_dag_code)
            temp_path = f.name
        
        try:
            report = migrator.migrate_file(temp_path)
            
            assert report.success
            assert report.backup_path is not None
            assert Path(report.backup_path).exists()
            
            # 清理备份文件
            Path(report.backup_path).unlink(missing_ok=True)
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def test_migrate_nonexistent_file(self, migrator):
        """测试迁移不存在的文件"""
        report = migrator.migrate_file('/nonexistent/path/dag.py')
        
        assert not report.success
        assert len(report.errors) > 0
    
    def test_migrate_non_python_file(self, migrator):
        """测试迁移非 Python 文件"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("not a python file")
            temp_path = f.name
        
        try:
            report = migrator.migrate_file(temp_path)
            
            assert not report.success
            assert len(report.errors) > 0
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def test_analyze_file(self, migrator, sample_dag_code):
        """测试分析模式"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as f:
            f.write(sample_dag_code)
            temp_path = f.name
        
        try:
            report = migrator.analyze_file(temp_path)
            
            assert isinstance(report, MigrationReport)
            # 分析模式不应修改文件
            with open(temp_path, 'r', encoding='utf-8') as f:
                content = f.read()
            assert content == sample_dag_code
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def test_migrate_directory(self, migrator, sample_dag_code):
        """测试目录迁移"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建测试文件
            dag_file = Path(temp_dir) / 'test_dag.py'
            dag_file.write_text(sample_dag_code, encoding='utf-8')
            
            # 创建非 DAG 文件
            non_dag_file = Path(temp_dir) / 'utils.py'
            non_dag_file.write_text('def helper(): pass', encoding='utf-8')
            
            report = migrator.migrate_directory(temp_dir)
            
            assert isinstance(report, BatchMigrationReport)
            assert report.total_files >= 1
    
    def test_migrate_directory_recursive(self, migrator, sample_dag_code):
        """测试递归目录迁移"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建子目录
            sub_dir = Path(temp_dir) / 'subdags'
            sub_dir.mkdir()
            
            # 在子目录创建 DAG 文件
            dag_file = sub_dir / 'sub_dag.py'
            dag_file.write_text(sample_dag_code, encoding='utf-8')
            
            report = migrator.migrate_directory(temp_dir, recursive=True)
            
            assert isinstance(report, BatchMigrationReport)
    
    def test_rollback(self, sample_dag_code):
        """测试回滚功能"""
        migrator = DAGMigrator(target_version="3.0", backup_enabled=True, dry_run=False)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as f:
            f.write(sample_dag_code)
            temp_path = f.name
        
        try:
            # 执行迁移
            report = migrator.migrate_file(temp_path)
            assert report.success
            assert report.backup_path
            
            # 执行回滚
            success = migrator.rollback(report.backup_path, temp_path)
            assert success
            
            # 验证回滚后的内容
            with open(temp_path, 'r', encoding='utf-8') as f:
                content = f.read()
            assert 'DummyOperator' in content
            
            # 清理
            Path(report.backup_path).unlink(missing_ok=True)
        finally:
            Path(temp_path).unlink(missing_ok=True)


class TestBatchMigrationReport:
    """批量迁移报告测试"""
    
    def test_success_rate_calculation(self):
        """测试成功率计算"""
        report = BatchMigrationReport(
            total_files=10,
            successful=8,
            failed=2
        )
        
        assert report.success_rate == 80.0
    
    def test_success_rate_zero_files(self):
        """测试零文件时的成功率"""
        report = BatchMigrationReport(total_files=0)
        
        assert report.success_rate == 0.0
