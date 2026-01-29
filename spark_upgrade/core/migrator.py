"""版本迁移器 - 协调整个迁移过程"""

import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .parser import SparkParser, SparkCodeStructure
from .transformer import SparkTransformer, TransformationResult
from .validator import SparkValidator, ValidationResult


@dataclass
class MigrationResult:
    """迁移结果"""
    success: bool
    file_path: str
    changes_applied: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    backup_path: Optional[str] = None
    compatibility_score: float = 0.0
    migration_time: str = ""
    original_code: str = ""
    transformed_code: str = ""
    dry_run: bool = False

    @property
    def has_changes(self) -> bool:
        return len(self.changes_applied) > 0


@dataclass
class BatchMigrationResult:
    """批量迁移结果"""
    total_files: int
    successful_files: int
    failed_files: int
    skipped_files: int
    results: List[MigrationResult] = field(default_factory=list)
    total_time: str = ""

    @property
    def success_rate(self) -> float:
        if self.total_files == 0:
            return 0.0
        return self.successful_files / self.total_files


class SparkMigrator:
    """Spark 代码迁移器"""

    def __init__(
        self,
        target_version: str = "3.2",
        backup_enabled: bool = True,
        backup_dir: str = ".spark_upgrade_backup",
        dry_run: bool = False,
    ):
        self.target_version = target_version
        self.backup_enabled = backup_enabled
        self.backup_dir = backup_dir
        self.dry_run = dry_run

        self.parser = SparkParser()
        self.transformer = SparkTransformer(target_version)
        self.validator = SparkValidator(target_version)

        self._progress_callback: Optional[Callable[[str, int, int], None]] = None

    def set_progress_callback(
        self, callback: Callable[[str, int, int], None]
    ) -> None:
        """设置进度回调函数"""
        self._progress_callback = callback

    def migrate_file(self, file_path: str) -> MigrationResult:
        """
        迁移单个文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            MigrationResult 迁移结果
        """
        start_time = datetime.now()
        path = Path(file_path)

        # 检查文件是否存在
        if not path.exists():
            return MigrationResult(
                success=False,
                file_path=file_path,
                errors=[f"文件不存在: {file_path}"],
                migration_time=self._format_duration(start_time),
            )

        # 检查是否是 Python 文件
        if path.suffix != ".py":
            return MigrationResult(
                success=False,
                file_path=file_path,
                errors=[f"不是 Python 文件: {file_path}"],
                migration_time=self._format_duration(start_time),
            )

        try:
            # 1. 解析代码
            structure = self.parser.parse_file(file_path)

            # 检查是否包含 Spark 代码
            if not self.parser.has_spark_code(structure):
                return MigrationResult(
                    success=True,
                    file_path=file_path,
                    warnings=["文件不包含 Spark 相关代码，跳过处理"],
                    migration_time=self._format_duration(start_time),
                    original_code=structure.raw_code,
                    transformed_code=structure.raw_code,
                )

            # 2. 转换代码
            transform_result = self.transformer.transform(structure)

            # 3. 验证转换后的代码
            validation_result = self.validator.validate(
                transform_result.transformed_code, file_path
            )

            # 4. 创建备份（如果启用且不是 dry-run）
            backup_path = None
            if self.backup_enabled and not self.dry_run and transform_result.has_changes:
                backup_path = self._create_backup(file_path)

            # 5. 写入转换后的代码（如果不是 dry-run）
            if not self.dry_run and transform_result.has_changes:
                self._write_file(file_path, transform_result.transformed_code)

            # 构建变更列表
            changes = [
                {
                    "rule": c.rule_name,
                    "description": c.description,
                    "original": c.original,
                    "replacement": c.transformed,
                    "line": c.line,
                }
                for c in transform_result.changes_applied
            ]

            return MigrationResult(
                success=transform_result.success and validation_result.valid,
                file_path=file_path,
                changes_applied=changes,
                warnings=transform_result.warnings + [
                    f"[验证] {i.message}" for i in validation_result.issues
                    if i.severity == "warning"
                ],
                errors=transform_result.errors + [
                    f"[验证] {i.message}" for i in validation_result.issues
                    if i.severity == "error"
                ],
                backup_path=backup_path,
                compatibility_score=validation_result.compatibility_score,
                migration_time=self._format_duration(start_time),
                original_code=structure.raw_code,
                transformed_code=transform_result.transformed_code,
                dry_run=self.dry_run,
            )

        except Exception as e:
            return MigrationResult(
                success=False,
                file_path=file_path,
                errors=[f"迁移过程出错: {str(e)}"],
                migration_time=self._format_duration(start_time),
            )

    def migrate_directory(
        self,
        directory_path: str,
        recursive: bool = True,
        pattern: str = "*.py",
        parallel: int = 4,
    ) -> BatchMigrationResult:
        """
        批量迁移目录中的文件
        
        Args:
            directory_path: 目录路径
            recursive: 是否递归处理子目录
            pattern: 文件匹配模式
            parallel: 并行处理数量
            
        Returns:
            BatchMigrationResult 批量迁移结果
        """
        start_time = datetime.now()
        path = Path(directory_path)

        if not path.exists():
            return BatchMigrationResult(
                total_files=0,
                successful_files=0,
                failed_files=0,
                skipped_files=0,
                results=[
                    MigrationResult(
                        success=False,
                        file_path=directory_path,
                        errors=[f"目录不存在: {directory_path}"],
                    )
                ],
            )

        # 收集所有匹配的文件
        if recursive:
            files = list(path.rglob(pattern))
        else:
            files = list(path.glob(pattern))

        # 过滤掉备份目录中的文件
        files = [
            f for f in files
            if self.backup_dir not in str(f) and "__pycache__" not in str(f)
        ]

        total_files = len(files)
        results: List[MigrationResult] = []
        successful = 0
        failed = 0
        skipped = 0

        if parallel > 1 and total_files > 1:
            # 并行处理
            with ThreadPoolExecutor(max_workers=parallel) as executor:
                future_to_file = {
                    executor.submit(self.migrate_file, str(f)): f
                    for f in files
                }

                for i, future in enumerate(as_completed(future_to_file)):
                    result = future.result()
                    results.append(result)

                    if result.success:
                        if result.has_changes:
                            successful += 1
                        else:
                            skipped += 1
                    else:
                        failed += 1

                    if self._progress_callback:
                        self._progress_callback(result.file_path, i + 1, total_files)
        else:
            # 串行处理
            for i, file_path in enumerate(files):
                result = self.migrate_file(str(file_path))
                results.append(result)

                if result.success:
                    if result.has_changes:
                        successful += 1
                    else:
                        skipped += 1
                else:
                    failed += 1

                if self._progress_callback:
                    self._progress_callback(result.file_path, i + 1, total_files)

        return BatchMigrationResult(
            total_files=total_files,
            successful_files=successful,
            failed_files=failed,
            skipped_files=skipped,
            results=results,
            total_time=self._format_duration(start_time),
        )

    def preview_migration(self, file_path: str) -> Dict[str, Any]:
        """
        预览迁移变更
        
        Args:
            file_path: 文件路径
            
        Returns:
            预览信息字典
        """
        try:
            structure = self.parser.parse_file(file_path)

            if not self.parser.has_spark_code(structure):
                return {
                    "file_path": file_path,
                    "has_spark_code": False,
                    "message": "文件不包含 Spark 相关代码",
                }

            preview = self.transformer.preview_changes(structure)

            return {
                "file_path": file_path,
                "has_spark_code": True,
                **preview,
            }
        except Exception as e:
            return {
                "file_path": file_path,
                "error": str(e),
            }

    def rollback(self, file_path: str, backup_path: str) -> bool:
        """
        回滚到备份版本
        
        Args:
            file_path: 原文件路径
            backup_path: 备份文件路径
            
        Returns:
            是否成功
        """
        try:
            if not Path(backup_path).exists():
                return False

            shutil.copy2(backup_path, file_path)
            return True
        except Exception:
            return False

    def _create_backup(self, file_path: str) -> str:
        """创建备份"""
        path = Path(file_path)
        backup_base = path.parent / self.backup_dir
        backup_base.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{path.stem}_{timestamp}{path.suffix}"
        backup_path = backup_base / backup_name

        shutil.copy2(file_path, backup_path)
        return str(backup_path)

    def _write_file(self, file_path: str, content: str) -> None:
        """写入文件"""
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)

    def _format_duration(self, start_time: datetime) -> str:
        """格式化持续时间"""
        duration = datetime.now() - start_time
        total_seconds = duration.total_seconds()

        if total_seconds < 1:
            return f"{int(total_seconds * 1000)}ms"
        elif total_seconds < 60:
            return f"{total_seconds:.2f}s"
        else:
            minutes = int(total_seconds // 60)
            seconds = total_seconds % 60
            return f"{minutes}m {seconds:.1f}s"

    def get_migration_summary(
        self, result: BatchMigrationResult
    ) -> Dict[str, Any]:
        """
        获取迁移摘要
        
        Args:
            result: 批量迁移结果
            
        Returns:
            摘要字典
        """
        total_changes = sum(
            len(r.changes_applied) for r in result.results
        )
        total_warnings = sum(
            len(r.warnings) for r in result.results
        )
        total_errors = sum(
            len(r.errors) for r in result.results
        )

        avg_score = 0.0
        scored_files = [r for r in result.results if r.compatibility_score > 0]
        if scored_files:
            avg_score = sum(r.compatibility_score for r in scored_files) / len(scored_files)

        return {
            "total_files": result.total_files,
            "successful_files": result.successful_files,
            "failed_files": result.failed_files,
            "skipped_files": result.skipped_files,
            "success_rate": f"{result.success_rate:.1%}",
            "total_changes": total_changes,
            "total_warnings": total_warnings,
            "total_errors": total_errors,
            "average_compatibility_score": f"{avg_score:.1%}",
            "total_time": result.total_time,
        }
