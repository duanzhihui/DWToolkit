"""迁移器单元测试"""

import os
import tempfile
from pathlib import Path

import pytest

from spark_upgrade.core.migrator import SparkMigrator, MigrationResult, BatchMigrationResult


class TestSparkMigrator:
    """SparkMigrator 测试类"""

    def setup_method(self):
        """测试前置"""
        self.migrator = SparkMigrator(
            target_version="3.2",
            backup_enabled=True,
            dry_run=True,
        )

    def test_migrate_file_dry_run(self):
        """测试 dry-run 模式迁移文件"""
        code = '''
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("data.csv")
df.registerTempTable("test")
'''
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False, encoding="utf-8"
        ) as f:
            f.write(code)
            temp_path = f.name

        try:
            result = self.migrator.migrate_file(temp_path)

            assert result.success is True
            assert result.dry_run is True
            assert result.has_changes is True

            # 验证原文件未被修改
            with open(temp_path, "r", encoding="utf-8") as f:
                content = f.read()
            assert "registerTempTable" in content
        finally:
            os.unlink(temp_path)

    def test_migrate_file_not_exists(self):
        """测试迁移不存在的文件"""
        result = self.migrator.migrate_file("/nonexistent/path/file.py")

        assert result.success is False
        assert len(result.errors) > 0

    def test_migrate_file_not_python(self):
        """测试迁移非 Python 文件"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False
        ) as f:
            f.write("not python")
            temp_path = f.name

        try:
            result = self.migrator.migrate_file(temp_path)

            assert result.success is False
            assert len(result.errors) > 0
        finally:
            os.unlink(temp_path)

    def test_migrate_file_no_spark_code(self):
        """测试迁移不包含 Spark 代码的文件"""
        code = '''
import pandas as pd
df = pd.read_csv("data.csv")
print(df.head())
'''
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False, encoding="utf-8"
        ) as f:
            f.write(code)
            temp_path = f.name

        try:
            result = self.migrator.migrate_file(temp_path)

            assert result.success is True
            assert result.has_changes is False
            assert len(result.warnings) > 0
        finally:
            os.unlink(temp_path)

    def test_migrate_file_with_backup(self):
        """测试迁移文件并创建备份"""
        migrator = SparkMigrator(
            target_version="3.2",
            backup_enabled=True,
            dry_run=False,
        )

        code = '''
from pyspark.sql import SparkSession
df.registerTempTable("test")
'''
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir) / "test.py"
            temp_path.write_text(code, encoding="utf-8")

            result = migrator.migrate_file(str(temp_path))

            assert result.success is True
            if result.has_changes:
                assert result.backup_path is not None
                assert Path(result.backup_path).exists()

    def test_migrate_directory_dry_run(self):
        """测试 dry-run 模式批量迁移"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建测试文件
            for i in range(3):
                file_path = Path(temp_dir) / f"test_{i}.py"
                file_path.write_text(
                    f"df.registerTempTable('table_{i}')",
                    encoding="utf-8",
                )

            result = self.migrator.migrate_directory(
                temp_dir,
                recursive=False,
                pattern="*.py",
            )

            assert isinstance(result, BatchMigrationResult)
            assert result.total_files == 3

    def test_migrate_directory_recursive(self):
        """测试递归批量迁移"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建子目录和文件
            sub_dir = Path(temp_dir) / "subdir"
            sub_dir.mkdir()

            (Path(temp_dir) / "test1.py").write_text(
                "df.registerTempTable('t1')", encoding="utf-8"
            )
            (sub_dir / "test2.py").write_text(
                "df.registerTempTable('t2')", encoding="utf-8"
            )

            result = self.migrator.migrate_directory(
                temp_dir,
                recursive=True,
                pattern="*.py",
            )

            assert result.total_files == 2

    def test_preview_migration(self):
        """测试预览迁移"""
        code = '''
from pyspark.sql import SparkSession
df.registerTempTable("test")
df.unionAll(df2)
'''
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False, encoding="utf-8"
        ) as f:
            f.write(code)
            temp_path = f.name

        try:
            preview = self.migrator.preview_migration(temp_path)

            assert "has_spark_code" in preview
            assert preview["has_spark_code"] is True
            assert preview["total_changes"] >= 2
        finally:
            os.unlink(temp_path)

    def test_get_migration_summary(self):
        """测试获取迁移摘要"""
        batch_result = BatchMigrationResult(
            total_files=10,
            successful_files=8,
            failed_files=1,
            skipped_files=1,
            results=[
                MigrationResult(
                    success=True,
                    file_path="test.py",
                    changes_applied=[{"rule": "test"}],
                    compatibility_score=0.9,
                )
            ],
        )

        summary = self.migrator.get_migration_summary(batch_result)

        assert summary["total_files"] == 10
        assert summary["successful_files"] == 8
        assert summary["failed_files"] == 1
        assert "success_rate" in summary


class TestMigrationResult:
    """MigrationResult 测试类"""

    def test_result_success(self):
        """测试成功结果"""
        result = MigrationResult(
            success=True,
            file_path="test.py",
            changes_applied=[{"rule": "test"}],
            compatibility_score=0.95,
        )

        assert result.success is True
        assert result.has_changes is True

    def test_result_failure(self):
        """测试失败结果"""
        result = MigrationResult(
            success=False,
            file_path="test.py",
            errors=["错误信息"],
        )

        assert result.success is False
        assert len(result.errors) == 1

    def test_result_no_changes(self):
        """测试无变更结果"""
        result = MigrationResult(
            success=True,
            file_path="test.py",
        )

        assert result.has_changes is False


class TestBatchMigrationResult:
    """BatchMigrationResult 测试类"""

    def test_batch_result(self):
        """测试批量结果"""
        result = BatchMigrationResult(
            total_files=10,
            successful_files=8,
            failed_files=2,
            skipped_files=0,
        )

        assert result.total_files == 10
        assert result.success_rate == 0.8

    def test_batch_result_empty(self):
        """测试空批量结果"""
        result = BatchMigrationResult(
            total_files=0,
            successful_files=0,
            failed_files=0,
            skipped_files=0,
        )

        assert result.success_rate == 0.0
