"""备份管理器 - 管理文件备份和恢复"""

import os
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class BackupInfo:
    """备份信息"""
    original_path: str
    backup_path: str
    created_at: datetime
    file_size: int
    checksum: Optional[str] = None

    @property
    def age_days(self) -> int:
        """备份天数"""
        return (datetime.now() - self.created_at).days


@dataclass
class BackupStats:
    """备份统计"""
    total_backups: int = 0
    total_size: int = 0
    oldest_backup: Optional[datetime] = None
    newest_backup: Optional[datetime] = None
    files: List[BackupInfo] = field(default_factory=list)


class BackupManager:
    """备份管理器"""

    BACKUP_MANIFEST = ".backup_manifest.json"

    def __init__(
        self,
        backup_dir: str = ".spark_upgrade_backup",
        retention_days: int = 30,
    ):
        self.backup_dir = backup_dir
        self.retention_days = retention_days

    def create_backup(
        self,
        file_path: str,
        backup_base: Optional[str] = None,
    ) -> BackupInfo:
        """
        创建文件备份
        
        Args:
            file_path: 原文件路径
            backup_base: 备份基础目录，默认为文件所在目录
            
        Returns:
            BackupInfo 备份信息
        """
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")

        # 确定备份目录
        if backup_base:
            backup_root = Path(backup_base) / self.backup_dir
        else:
            backup_root = path.parent / self.backup_dir

        backup_root.mkdir(parents=True, exist_ok=True)

        # 生成备份文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{path.stem}_{timestamp}{path.suffix}"
        backup_path = backup_root / backup_name

        # 复制文件
        shutil.copy2(file_path, backup_path)

        # 获取文件大小
        file_size = backup_path.stat().st_size

        return BackupInfo(
            original_path=str(path.resolve()),
            backup_path=str(backup_path.resolve()),
            created_at=datetime.now(),
            file_size=file_size,
        )

    def restore_backup(
        self,
        backup_path: str,
        target_path: Optional[str] = None,
    ) -> bool:
        """
        恢复备份
        
        Args:
            backup_path: 备份文件路径
            target_path: 目标路径，默认为原始路径
            
        Returns:
            是否成功
        """
        backup = Path(backup_path)

        if not backup.exists():
            return False

        if target_path:
            target = Path(target_path)
        else:
            # 从备份文件名推断原始路径
            # 格式: filename_YYYYMMDD_HHMMSS.ext
            name = backup.stem
            parts = name.rsplit("_", 2)
            if len(parts) >= 3:
                original_name = "_".join(parts[:-2]) + backup.suffix
            else:
                original_name = name + backup.suffix
            target = backup.parent.parent / original_name

        try:
            shutil.copy2(backup_path, target)
            return True
        except Exception:
            return False

    def list_backups(
        self,
        directory: Optional[str] = None,
        file_pattern: str = "*.py",
    ) -> List[BackupInfo]:
        """
        列出备份文件
        
        Args:
            directory: 搜索目录
            file_pattern: 文件匹配模式
            
        Returns:
            备份信息列表
        """
        backups: List[BackupInfo] = []

        if directory:
            search_paths = [Path(directory)]
        else:
            search_paths = [Path(".")]

        for search_path in search_paths:
            # 查找备份目录
            for backup_dir in search_path.rglob(self.backup_dir):
                if not backup_dir.is_dir():
                    continue

                for backup_file in backup_dir.glob("*"):
                    if not backup_file.is_file():
                        continue

                    # 解析备份信息
                    try:
                        stat = backup_file.stat()
                        created_at = datetime.fromtimestamp(stat.st_mtime)

                        backups.append(
                            BackupInfo(
                                original_path=self._infer_original_path(backup_file),
                                backup_path=str(backup_file.resolve()),
                                created_at=created_at,
                                file_size=stat.st_size,
                            )
                        )
                    except Exception:
                        continue

        # 按创建时间排序
        backups.sort(key=lambda b: b.created_at, reverse=True)
        return backups

    def cleanup_old_backups(
        self,
        directory: Optional[str] = None,
        retention_days: Optional[int] = None,
    ) -> int:
        """
        清理过期备份
        
        Args:
            directory: 搜索目录
            retention_days: 保留天数，默认使用初始化时的值
            
        Returns:
            删除的文件数量
        """
        if retention_days is None:
            retention_days = self.retention_days

        cutoff_date = datetime.now() - timedelta(days=retention_days)
        deleted_count = 0

        backups = self.list_backups(directory)

        for backup in backups:
            if backup.created_at < cutoff_date:
                try:
                    Path(backup.backup_path).unlink()
                    deleted_count += 1
                except Exception:
                    continue

        # 清理空的备份目录
        self._cleanup_empty_dirs(directory)

        return deleted_count

    def get_backup_stats(
        self,
        directory: Optional[str] = None,
    ) -> BackupStats:
        """
        获取备份统计信息
        
        Args:
            directory: 搜索目录
            
        Returns:
            BackupStats 统计信息
        """
        backups = self.list_backups(directory)

        if not backups:
            return BackupStats()

        total_size = sum(b.file_size for b in backups)
        oldest = min(backups, key=lambda b: b.created_at)
        newest = max(backups, key=lambda b: b.created_at)

        return BackupStats(
            total_backups=len(backups),
            total_size=total_size,
            oldest_backup=oldest.created_at,
            newest_backup=newest.created_at,
            files=backups,
        )

    def find_latest_backup(self, file_path: str) -> Optional[BackupInfo]:
        """
        查找文件的最新备份
        
        Args:
            file_path: 原文件路径
            
        Returns:
            最新的备份信息，如果没有则返回 None
        """
        path = Path(file_path)
        backup_dir = path.parent / self.backup_dir

        if not backup_dir.exists():
            return None

        # 查找匹配的备份文件
        pattern = f"{path.stem}_*{path.suffix}"
        backups = list(backup_dir.glob(pattern))

        if not backups:
            return None

        # 按修改时间排序，返回最新的
        latest = max(backups, key=lambda p: p.stat().st_mtime)
        stat = latest.stat()

        return BackupInfo(
            original_path=str(path.resolve()),
            backup_path=str(latest.resolve()),
            created_at=datetime.fromtimestamp(stat.st_mtime),
            file_size=stat.st_size,
        )

    def _infer_original_path(self, backup_file: Path) -> str:
        """从备份文件推断原始路径"""
        name = backup_file.stem
        parts = name.rsplit("_", 2)

        if len(parts) >= 3:
            # 格式: filename_YYYYMMDD_HHMMSS
            original_name = "_".join(parts[:-2]) + backup_file.suffix
        else:
            original_name = name + backup_file.suffix

        # 原始文件应该在备份目录的父目录
        return str(backup_file.parent.parent / original_name)

    def _cleanup_empty_dirs(self, directory: Optional[str] = None) -> None:
        """清理空的备份目录"""
        if directory:
            search_paths = [Path(directory)]
        else:
            search_paths = [Path(".")]

        for search_path in search_paths:
            for backup_dir in search_path.rglob(self.backup_dir):
                if backup_dir.is_dir() and not any(backup_dir.iterdir()):
                    try:
                        backup_dir.rmdir()
                    except Exception:
                        pass

    def get_summary(self, stats: BackupStats) -> Dict[str, Any]:
        """
        获取备份统计摘要
        
        Args:
            stats: 备份统计
            
        Returns:
            摘要字典
        """
        return {
            "total_backups": stats.total_backups,
            "total_size": self._format_size(stats.total_size),
            "oldest_backup": (
                stats.oldest_backup.strftime("%Y-%m-%d %H:%M:%S")
                if stats.oldest_backup
                else None
            ),
            "newest_backup": (
                stats.newest_backup.strftime("%Y-%m-%d %H:%M:%S")
                if stats.newest_backup
                else None
            ),
        }

    def _format_size(self, size: int) -> str:
        """格式化文件大小"""
        for unit in ["B", "KB", "MB", "GB"]:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"
