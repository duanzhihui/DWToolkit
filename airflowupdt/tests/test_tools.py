# -*- coding: utf-8 -*-
"""
工具集成测试
"""

import tempfile
from pathlib import Path

import pytest
from airflowupdt.tools.ruff_integration import RuffChecker, create_ruff_config
from airflowupdt.tools.flake8_integration import Flake8Checker, create_flake8_config
from airflowupdt.tools.backup_manager import BackupManager


class TestRuffChecker:
    """Ruff 检查器测试"""
    
    @pytest.fixture
    def checker(self):
        return RuffChecker()
    
    def test_check_valid_code(self, checker):
        """测试检查有效代码"""
        code = '''
def hello():
    """Say hello."""
    print("Hello World")
'''
        report = checker.check_code(code)
        
        assert report.success
    
    def test_check_code_with_issues(self, checker):
        """测试检查有问题的代码"""
        code = '''
import os
import sys
import os  # duplicate import

def hello():
    x = 1
    return
'''
        report = checker.check_code(code)
        
        assert report.success  # 检查成功执行
        # 可能有重复导入等问题
    
    def test_generate_report_text(self, checker):
        """测试生成报告文本"""
        code = '''
def hello():
    pass
'''
        report = checker.check_code(code)
        text = checker.generate_report_text(report)
        
        assert isinstance(text, str)
        assert 'Ruff' in text
    
    def test_create_ruff_config(self):
        """测试生成 Ruff 配置"""
        config = create_ruff_config()
        
        assert isinstance(config, str)
        assert 'ruff' in config.lower()
        assert 'line-length' in config


class TestFlake8Checker:
    """Flake8 检查器测试"""
    
    @pytest.fixture
    def checker(self):
        return Flake8Checker()
    
    def test_check_valid_code(self, checker):
        """测试检查有效代码"""
        code = '''
def hello():
    """Say hello."""
    print("Hello World")
'''
        report = checker.check_code(code)
        
        assert report.success
    
    def test_generate_report_text(self, checker):
        """测试生成报告文本"""
        code = '''
def hello():
    pass
'''
        report = checker.check_code(code)
        text = checker.generate_report_text(report)
        
        assert isinstance(text, str)
        assert 'Flake8' in text
    
    def test_create_flake8_config(self):
        """测试生成 Flake8 配置"""
        config = create_flake8_config()
        
        assert isinstance(config, str)
        assert 'flake8' in config.lower()
        assert 'max-line-length' in config


class TestBackupManager:
    """备份管理器测试"""
    
    def test_backup_file(self):
        """测试备份文件"""
        with tempfile.TemporaryDirectory() as backup_dir:
            manager = BackupManager(backup_dir=backup_dir)
            
            # 创建测试文件
            with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as f:
                f.write("# test content")
                temp_path = f.name
            
            try:
                backup_info = manager.backup_file(temp_path)
                
                assert backup_info is not None
                assert Path(backup_info.backup_path).exists()
                assert backup_info.original_path == str(Path(temp_path).absolute())
            finally:
                Path(temp_path).unlink(missing_ok=True)
    
    def test_restore_file(self):
        """测试恢复文件"""
        with tempfile.TemporaryDirectory() as backup_dir:
            manager = BackupManager(backup_dir=backup_dir)
            
            # 创建测试文件
            with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as f:
                f.write("original content")
                temp_path = f.name
            
            try:
                # 备份
                backup_info = manager.backup_file(temp_path)
                
                # 修改原文件
                Path(temp_path).write_text("modified content", encoding='utf-8')
                
                # 恢复
                success = manager.restore_file(backup_info)
                
                assert success
                assert Path(temp_path).read_text(encoding='utf-8') == "original content"
            finally:
                Path(temp_path).unlink(missing_ok=True)
    
    def test_list_backups(self):
        """测试列出备份"""
        with tempfile.TemporaryDirectory() as backup_dir:
            manager = BackupManager(backup_dir=backup_dir)
            
            # 创建测试文件并备份
            with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as f:
                f.write("# test")
                temp_path = f.name
            
            try:
                manager.backup_file(temp_path)
                manager.backup_file(temp_path)
                
                backups = manager.list_backups()
                
                assert len(backups) >= 2
            finally:
                Path(temp_path).unlink(missing_ok=True)
    
    def test_verify_backup(self):
        """测试验证备份"""
        with tempfile.TemporaryDirectory() as backup_dir:
            manager = BackupManager(backup_dir=backup_dir)
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as f:
                f.write("# test content")
                temp_path = f.name
            
            try:
                backup_info = manager.backup_file(temp_path)
                
                is_valid = manager.verify_backup(backup_info)
                
                assert is_valid
            finally:
                Path(temp_path).unlink(missing_ok=True)
    
    def test_generate_rollback_script_bash(self):
        """测试生成 Bash 回滚脚本"""
        with tempfile.TemporaryDirectory() as backup_dir:
            manager = BackupManager(backup_dir=backup_dir)
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as f:
                f.write("# test")
                temp_path = f.name
            
            try:
                manager.backup_file(temp_path)
                
                script = manager.generate_rollback_script(output_format='bash')
                
                assert '#!/bin/bash' in script
                assert 'cp' in script
            finally:
                Path(temp_path).unlink(missing_ok=True)
    
    def test_generate_rollback_script_powershell(self):
        """测试生成 PowerShell 回滚脚本"""
        with tempfile.TemporaryDirectory() as backup_dir:
            manager = BackupManager(backup_dir=backup_dir)
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as f:
                f.write("# test")
                temp_path = f.name
            
            try:
                manager.backup_file(temp_path)
                
                script = manager.generate_rollback_script(output_format='powershell')
                
                assert 'Copy-Item' in script
            finally:
                Path(temp_path).unlink(missing_ok=True)
    
    def test_backup_summary(self):
        """测试备份摘要"""
        with tempfile.TemporaryDirectory() as backup_dir:
            manager = BackupManager(backup_dir=backup_dir)
            
            summary = manager.get_backup_summary()
            
            assert 'total_backups' in summary
            assert 'total_size' in summary
            assert 'files_backed_up' in summary
