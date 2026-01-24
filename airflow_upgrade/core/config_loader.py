# -*- coding: utf-8 -*-
"""
配置文件加载器
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


class Config:
    """配置类"""
    
    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        """初始化配置
        
        Args:
            config_dict: 配置字典
        """
        self._config = config_dict or {}
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值，支持点号分隔的嵌套键
        
        Args:
            key: 配置键，支持 'backup.enabled' 这样的嵌套键
            default: 默认值
            
        Returns:
            配置值
        """
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        
        return value if value is not None else default
    
    def get_target_version(self) -> str:
        """获取目标版本"""
        return self.get('target_version', '3.0')
    
    def get_backup_enabled(self) -> bool:
        """获取是否启用备份"""
        return self.get('backup.enabled', True)
    
    def get_backup_directory(self) -> Optional[str]:
        """获取备份目录"""
        return self.get('backup.directory')
    
    def get_backup_keep_count(self) -> int:
        """获取备份保留数量"""
        return self.get('backup.keep_count', 5)
    
    def get_ruff_enabled(self) -> bool:
        """获取是否启用 Ruff 检查"""
        return self.get('quality_checks.ruff.enabled', True)
    
    def get_ruff_auto_fix(self) -> bool:
        """获取 Ruff 是否自动修复"""
        return self.get('quality_checks.ruff.auto_fix', False)
    
    def get_ruff_line_length(self) -> int:
        """获取 Ruff 行长度限制"""
        return self.get('quality_checks.ruff.line_length', 120)
    
    def get_ruff_rules(self) -> list:
        """获取 Ruff 规则列表"""
        return self.get('quality_checks.ruff.rules', ['AIR', 'E', 'F', 'I', 'W'])
    
    def get_ruff_ignore(self) -> list:
        """获取 Ruff 忽略规则列表"""
        return self.get('quality_checks.ruff.ignore', ['E501'])
    
    def get_flake8_enabled(self) -> bool:
        """获取是否启用 Flake8 检查"""
        return self.get('quality_checks.flake8.enabled', True)
    
    def get_flake8_max_line_length(self) -> int:
        """获取 Flake8 最大行长度"""
        return self.get('quality_checks.flake8.max_line_length', 120)
    
    def get_flake8_max_complexity(self) -> int:
        """获取 Flake8 最大复杂度"""
        return self.get('quality_checks.flake8.max_complexity', 10)
    
    def get_flake8_ignore(self) -> list:
        """获取 Flake8 忽略规则列表"""
        return self.get('quality_checks.flake8.ignore', ['E501', 'W503'])
    
    def get_upgrade_rules(self) -> Dict[str, bool]:
        """获取升级规则配置"""
        return {
            'import_migration': self.get('upgrade_rules.import_migration', True),
            'operator_deprecation': self.get('upgrade_rules.operator_deprecation', True),
            'config_update': self.get('upgrade_rules.config_update', True),
            'param_rename': self.get('upgrade_rules.param_rename', True),
        }
    
    def get_exclude_patterns(self) -> list:
        """获取排除模式列表"""
        return self.get('exclude_patterns', [
            '__pycache__',
            '.git',
            'test_*',
            '*_test.py',
            '.venv',
            'venv'
        ])
    
    def get_output_format(self) -> str:
        """获取输出格式"""
        return self.get('output.format', 'text')
    
    def get_output_verbose(self) -> bool:
        """获取是否详细输出"""
        return self.get('output.verbose', False)
    
    def get_output_color(self) -> bool:
        """获取是否彩色输出"""
        return self.get('output.color', True)


class ConfigLoader:
    """配置文件加载器"""
    
    DEFAULT_CONFIG_FILES = [
        '.airflow_upgrade.yml',
        '.airflow_upgrade.yaml',
        'airflow_upgrade.yml',
        'airflow_upgrade.yaml',
    ]
    
    @classmethod
    def load(cls, config_path: Optional[str] = None) -> Config:
        """加载配置文件
        
        Args:
            config_path: 配置文件路径，如果为 None 则自动查找
            
        Returns:
            Config 对象
        """
        if config_path:
            # 使用指定的配置文件
            if os.path.exists(config_path):
                return cls._load_from_file(config_path)
            else:
                # 配置文件不存在，返回空配置
                return Config()
        
        # 自动查找配置文件
        for config_file in cls.DEFAULT_CONFIG_FILES:
            if os.path.exists(config_file):
                return cls._load_from_file(config_file)
        
        # 没有找到配置文件，返回空配置
        return Config()
    
    @classmethod
    def _load_from_file(cls, file_path: str) -> Config:
        """从文件加载配置
        
        Args:
            file_path: 配置文件路径
            
        Returns:
            Config 对象
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                config_dict = yaml.safe_load(f) or {}
            return Config(config_dict)
        except Exception as e:
            # 加载失败，返回空配置
            print(f"警告: 加载配置文件 {file_path} 失败: {e}")
            return Config()
