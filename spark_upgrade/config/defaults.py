"""默认配置定义"""

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class BackupConfig:
    """备份配置"""
    enabled: bool = True
    directory: str = ".spark_upgrade_backup"
    retention_days: int = 30


@dataclass
class QualityConfig:
    """代码质量检查配置"""
    enabled: bool = True
    ruff_config: Optional[str] = None
    auto_fix: bool = False
    fail_on_error: bool = False


@dataclass
class UpgradeConfig:
    """升级配置"""
    target_version: str = "3.2"
    dry_run: bool = False
    parallel_jobs: int = 4
    file_pattern: str = "*.py"


@dataclass
class SparkUpgradeConfig:
    """Spark升级工具主配置"""
    upgrade: UpgradeConfig = field(default_factory=UpgradeConfig)
    backup: BackupConfig = field(default_factory=BackupConfig)
    quality: QualityConfig = field(default_factory=QualityConfig)
    custom_rules: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict) -> "SparkUpgradeConfig":
        """从字典创建配置对象"""
        upgrade_data = data.get("upgrade", {})
        backup_data = data.get("backup", {})
        quality_data = data.get("quality", {})
        custom_rules = data.get("custom_rules", {})

        return cls(
            upgrade=UpgradeConfig(**upgrade_data) if upgrade_data else UpgradeConfig(),
            backup=BackupConfig(**backup_data) if backup_data else BackupConfig(),
            quality=QualityConfig(**quality_data) if quality_data else QualityConfig(),
            custom_rules=custom_rules or {},
        )

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            "upgrade": {
                "target_version": self.upgrade.target_version,
                "dry_run": self.upgrade.dry_run,
                "parallel_jobs": self.upgrade.parallel_jobs,
                "file_pattern": self.upgrade.file_pattern,
            },
            "backup": {
                "enabled": self.backup.enabled,
                "directory": self.backup.directory,
                "retention_days": self.backup.retention_days,
            },
            "quality": {
                "enabled": self.quality.enabled,
                "ruff_config": self.quality.ruff_config,
                "auto_fix": self.quality.auto_fix,
                "fail_on_error": self.quality.fail_on_error,
            },
            "custom_rules": self.custom_rules,
        }
