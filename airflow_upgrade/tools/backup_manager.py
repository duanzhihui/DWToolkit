# -*- coding: utf-8 -*-
"""
备份管理器 - 管理DAG文件备份和回滚
"""

import hashlib
import json
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class BackupInfo:
    """备份信息"""
    original_path: str
    backup_path: str
    timestamp: str
    file_hash: str
    size: int
    metadata: Dict = field(default_factory=dict)


@dataclass
class BackupManifest:
    """备份清单"""
    version: str = "1.0"
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    backups: List[BackupInfo] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return {
            'version': self.version,
            'created_at': self.created_at,
            'backups': [
                {
                    'original_path': b.original_path,
                    'backup_path': b.backup_path,
                    'timestamp': b.timestamp,
                    'file_hash': b.file_hash,
                    'size': b.size,
                    'metadata': b.metadata
                }
                for b in self.backups
            ]
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'BackupManifest':
        manifest = cls(
            version=data.get('version', '1.0'),
            created_at=data.get('created_at', '')
        )
        for b in data.get('backups', []):
            manifest.backups.append(BackupInfo(
                original_path=b['original_path'],
                backup_path=b['backup_path'],
                timestamp=b['timestamp'],
                file_hash=b['file_hash'],
                size=b['size'],
                metadata=b.get('metadata', {})
            ))
        return manifest


class BackupManager:
    """备份管理器"""
    
    MANIFEST_FILE = 'backup_manifest.json'
    
    def __init__(self, backup_dir: Optional[str] = None):
        """
        初始化备份管理器
        
        Args:
            backup_dir: 备份目录路径,如果为None则在原文件目录创建备份
        """
        self.backup_dir = Path(backup_dir) if backup_dir else None
        self.manifest: Optional[BackupManifest] = None
        
        if self.backup_dir:
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            self._load_manifest()
    
    def _load_manifest(self):
        """加载备份清单"""
        if self.backup_dir:
            manifest_path = self.backup_dir / self.MANIFEST_FILE
            if manifest_path.exists():
                try:
                    with open(manifest_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    self.manifest = BackupManifest.from_dict(data)
                except Exception:
                    self.manifest = BackupManifest()
            else:
                self.manifest = BackupManifest()
    
    def _save_manifest(self):
        """保存备份清单"""
        if self.backup_dir and self.manifest:
            manifest_path = self.backup_dir / self.MANIFEST_FILE
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(self.manifest.to_dict(), f, indent=2, ensure_ascii=False)
    
    def _calculate_hash(self, file_path: Path) -> str:
        """计算文件哈希"""
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                hasher.update(chunk)
        return hasher.hexdigest()
    
    def backup_file(
        self,
        file_path: str,
        metadata: Optional[Dict] = None
    ) -> Optional[BackupInfo]:
        """
        备份单个文件
        
        Args:
            file_path: 要备份的文件路径
            metadata: 附加元数据
            
        Returns:
            备份信息,如果失败返回None
        """
        source = Path(file_path)
        if not source.exists():
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 确定备份路径
        if self.backup_dir:
            # 使用集中备份目录
            backup_name = f"{source.stem}_{timestamp}{source.suffix}"
            backup_path = self.backup_dir / backup_name
        else:
            # 在原文件目录创建备份
            backup_path = source.with_suffix(f".{timestamp}.bak")
        
        try:
            # 复制文件
            shutil.copy2(source, backup_path)
            
            # 创建备份信息
            backup_info = BackupInfo(
                original_path=str(source.absolute()),
                backup_path=str(backup_path.absolute()),
                timestamp=timestamp,
                file_hash=self._calculate_hash(source),
                size=source.stat().st_size,
                metadata=metadata or {}
            )
            
            # 更新清单
            if self.manifest:
                self.manifest.backups.append(backup_info)
                self._save_manifest()
            
            return backup_info
            
        except Exception as e:
            print(f"备份失败: {e}")
            return None
    
    def backup_directory(
        self,
        directory: str,
        pattern: str = "*.py",
        recursive: bool = True,
        metadata: Optional[Dict] = None
    ) -> List[BackupInfo]:
        """
        备份目录中的文件
        
        Args:
            directory: 目录路径
            pattern: 文件匹配模式
            recursive: 是否递归
            metadata: 附加元数据
            
        Returns:
            备份信息列表
        """
        dir_path = Path(directory)
        if not dir_path.exists():
            return []
        
        backups = []
        
        if recursive:
            files = list(dir_path.rglob(pattern))
        else:
            files = list(dir_path.glob(pattern))
        
        for file_path in files:
            backup_info = self.backup_file(str(file_path), metadata)
            if backup_info:
                backups.append(backup_info)
        
        return backups
    
    def restore_file(self, backup_info: BackupInfo) -> bool:
        """
        从备份恢复文件
        
        Args:
            backup_info: 备份信息
            
        Returns:
            是否成功
        """
        backup_path = Path(backup_info.backup_path)
        original_path = Path(backup_info.original_path)
        
        if not backup_path.exists():
            print(f"备份文件不存在: {backup_path}")
            return False
        
        try:
            shutil.copy2(backup_path, original_path)
            return True
        except Exception as e:
            print(f"恢复失败: {e}")
            return False
    
    def restore_by_path(self, original_path: str) -> bool:
        """
        根据原始路径恢复最新备份
        
        Args:
            original_path: 原始文件路径
            
        Returns:
            是否成功
        """
        if not self.manifest:
            return False
        
        # 查找该文件的最新备份
        matching_backups = [
            b for b in self.manifest.backups
            if b.original_path == str(Path(original_path).absolute())
        ]
        
        if not matching_backups:
            return False
        
        # 按时间戳排序,获取最新的
        latest = max(matching_backups, key=lambda b: b.timestamp)
        return self.restore_file(latest)
    
    def list_backups(self, original_path: Optional[str] = None) -> List[BackupInfo]:
        """
        列出备份
        
        Args:
            original_path: 如果指定,只列出该文件的备份
            
        Returns:
            备份信息列表
        """
        if not self.manifest:
            return []
        
        if original_path:
            abs_path = str(Path(original_path).absolute())
            return [b for b in self.manifest.backups if b.original_path == abs_path]
        
        return self.manifest.backups
    
    def cleanup_old_backups(self, keep_count: int = 5) -> int:
        """
        清理旧备份,每个文件只保留最新的N个备份
        
        Args:
            keep_count: 每个文件保留的备份数量
            
        Returns:
            删除的备份数量
        """
        if not self.manifest:
            return 0
        
        # 按原始文件分组
        by_file: Dict[str, List[BackupInfo]] = {}
        for backup in self.manifest.backups:
            if backup.original_path not in by_file:
                by_file[backup.original_path] = []
            by_file[backup.original_path].append(backup)
        
        deleted_count = 0
        new_backups = []
        
        for original_path, backups in by_file.items():
            # 按时间戳排序
            sorted_backups = sorted(backups, key=lambda b: b.timestamp, reverse=True)
            
            # 保留最新的
            keep = sorted_backups[:keep_count]
            remove = sorted_backups[keep_count:]
            
            new_backups.extend(keep)
            
            # 删除旧备份文件
            for backup in remove:
                backup_path = Path(backup.backup_path)
                if backup_path.exists():
                    try:
                        backup_path.unlink()
                        deleted_count += 1
                    except Exception:
                        pass
        
        self.manifest.backups = new_backups
        self._save_manifest()
        
        return deleted_count
    
    def verify_backup(self, backup_info: BackupInfo) -> bool:
        """
        验证备份完整性
        
        Args:
            backup_info: 备份信息
            
        Returns:
            备份是否完整
        """
        backup_path = Path(backup_info.backup_path)
        
        if not backup_path.exists():
            return False
        
        current_hash = self._calculate_hash(backup_path)
        return current_hash == backup_info.file_hash
    
    def generate_rollback_script(
        self,
        backups: Optional[List[BackupInfo]] = None,
        output_format: str = 'bash'
    ) -> str:
        """
        生成回滚脚本
        
        Args:
            backups: 备份列表,如果为None则使用所有备份
            output_format: 输出格式 ('bash' 或 'powershell')
            
        Returns:
            回滚脚本内容
        """
        backups = backups or (self.manifest.backups if self.manifest else [])
        
        if output_format == 'powershell':
            return self._generate_powershell_script(backups)
        else:
            return self._generate_bash_script(backups)
    
    def _generate_bash_script(self, backups: List[BackupInfo]) -> str:
        """生成 Bash 回滚脚本"""
        lines = [
            "#!/bin/bash",
            "# Airflow DAG 迁移回滚脚本",
            f"# 生成时间: {datetime.now().isoformat()}",
            "",
            "set -e",
            "",
            'echo "开始回滚..."',
            "",
        ]
        
        for backup in backups:
            lines.append(f'echo "恢复: {backup.original_path}"')
            lines.append(f'cp "{backup.backup_path}" "{backup.original_path}"')
            lines.append("")
        
        lines.append('echo "回滚完成!"')
        
        return '\n'.join(lines)
    
    def _generate_powershell_script(self, backups: List[BackupInfo]) -> str:
        """生成 PowerShell 回滚脚本"""
        lines = [
            "# Airflow DAG 迁移回滚脚本",
            f"# 生成时间: {datetime.now().isoformat()}",
            "",
            "$ErrorActionPreference = 'Stop'",
            "",
            'Write-Host "开始回滚..."',
            "",
        ]
        
        for backup in backups:
            lines.append(f'Write-Host "恢复: {backup.original_path}"')
            lines.append(f'Copy-Item -Path "{backup.backup_path}" -Destination "{backup.original_path}" -Force')
            lines.append("")
        
        lines.append('Write-Host "回滚完成!"')
        
        return '\n'.join(lines)
    
    def get_backup_summary(self) -> Dict:
        """获取备份摘要"""
        if not self.manifest:
            return {
                'total_backups': 0,
                'total_size': 0,
                'files_backed_up': 0
            }
        
        unique_files = set(b.original_path for b in self.manifest.backups)
        total_size = sum(b.size for b in self.manifest.backups)
        
        return {
            'total_backups': len(self.manifest.backups),
            'total_size': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'files_backed_up': len(unique_files),
            'created_at': self.manifest.created_at
        }
