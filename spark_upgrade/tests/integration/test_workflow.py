"""集成测试 - 端到端工作流测试"""

import os
import tempfile
from pathlib import Path

import pytest

from spark_upgrade.core import SparkMigrator, SparkParser, SparkTransformer, SparkValidator
from spark_upgrade.tools import QualityChecker, BackupManager, ReportGenerator
from spark_upgrade.config import load_config, SparkUpgradeConfig


class TestEndToEndWorkflow:
    """端到端工作流测试"""

    def test_full_migration_workflow(self):
        """测试完整迁移工作流"""
        # 1. 准备测试代码
        code = '''
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").getOrCreate()

df = spark.read.csv("data.csv")
df.registerTempTable("temp_table")

df2 = spark.sql("SELECT * FROM temp_table")
result = df.unionAll(df2)

result.show()
'''
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "test_spark.py"
            file_path.write_text(code, encoding="utf-8")

            # 2. 解析代码
            parser = SparkParser()
            structure = parser.parse_file(str(file_path))

            assert parser.has_spark_code(structure)
            assert len(parser.get_deprecated_usage(structure)) > 0

            # 3. 转换代码
            transformer = SparkTransformer(target_version="3.2")
            transform_result = transformer.transform(structure)

            assert transform_result.success
            assert transform_result.has_changes

            # 4. 验证转换后的代码
            validator = SparkValidator(target_version="3.2")
            validation_result = validator.validate(
                transform_result.transformed_code, str(file_path)
            )

            assert validation_result.syntax_valid

            # 5. 执行迁移
            migrator = SparkMigrator(
                target_version="3.2",
                backup_enabled=True,
                dry_run=False,
            )
            migration_result = migrator.migrate_file(str(file_path))

            assert migration_result.success
            assert migration_result.has_changes

            # 6. 验证文件已更新
            updated_code = file_path.read_text(encoding="utf-8")
            assert ".createOrReplaceTempView(" in updated_code
            assert ".union(" in updated_code
            assert ".registerTempTable(" not in updated_code
            assert ".unionAll(" not in updated_code

            # 7. 验证备份已创建
            assert migration_result.backup_path is not None
            assert Path(migration_result.backup_path).exists()

    def test_batch_migration_workflow(self):
        """测试批量迁移工作流"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建多个测试文件
            files_content = {
                "file1.py": 'df.registerTempTable("t1")',
                "file2.py": 'df.unionAll(df2)',
                "file3.py": 'from pyspark.sql.functions import *',
                "subdir/file4.py": 'df.registerTempTable("t4")',
            }

            for rel_path, content in files_content.items():
                file_path = Path(temp_dir) / rel_path
                file_path.parent.mkdir(parents=True, exist_ok=True)
                file_path.write_text(content, encoding="utf-8")

            # 执行批量迁移
            migrator = SparkMigrator(
                target_version="3.2",
                backup_enabled=False,
                dry_run=False,
            )
            batch_result = migrator.migrate_directory(
                temp_dir,
                recursive=True,
                pattern="*.py",
            )

            assert batch_result.total_files == 4
            assert batch_result.successful_files >= 3

    def test_report_generation_workflow(self):
        """测试报告生成工作流"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建测试文件
            code = '''
from pyspark.sql import SparkSession
df.registerTempTable("test")
'''
            file_path = Path(temp_dir) / "test.py"
            file_path.write_text(code, encoding="utf-8")

            # 执行迁移
            migrator = SparkMigrator(target_version="3.2", dry_run=True)
            batch_result = migrator.migrate_directory(temp_dir)

            # 生成报告
            generator = ReportGenerator(target_version="3.2")
            report = generator.generate_report(batch_result)

            # 保存各种格式的报告
            html_path = Path(temp_dir) / "report.html"
            md_path = Path(temp_dir) / "report.md"
            json_path = Path(temp_dir) / "report.json"

            generator.save_report(report, str(html_path), "html")
            generator.save_report(report, str(md_path), "markdown")
            generator.save_report(report, str(json_path), "json")

            assert html_path.exists()
            assert md_path.exists()
            assert json_path.exists()

            # 验证报告内容
            html_content = html_path.read_text(encoding="utf-8")
            assert "Spark" in html_content
            assert "迁移" in html_content


class TestConfigurationWorkflow:
    """配置工作流测试"""

    def test_load_default_config(self):
        """测试加载默认配置"""
        config = load_config(None)

        assert isinstance(config, SparkUpgradeConfig)
        assert config.upgrade.target_version == "3.2"
        assert config.backup.enabled is True

    def test_config_from_file(self):
        """测试从文件加载配置"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_content = '''
upgrade:
  target_version: "3.3"
  dry_run: true
  parallel_jobs: 8

backup:
  enabled: false
  directory: ".my_backup"
'''
            config_path = Path(temp_dir) / ".spark_upgrade.yml"
            config_path.write_text(config_content, encoding="utf-8")

            config = load_config(str(config_path))

            assert config.upgrade.target_version == "3.3"
            assert config.upgrade.dry_run is True
            assert config.upgrade.parallel_jobs == 8
            assert config.backup.enabled is False


class TestBackupWorkflow:
    """备份工作流测试"""

    def test_backup_and_restore(self):
        """测试备份和恢复"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建原始文件
            original_content = "original content"
            file_path = Path(temp_dir) / "test.py"
            file_path.write_text(original_content, encoding="utf-8")

            # 创建备份
            manager = BackupManager(backup_dir=".backup")
            backup_info = manager.create_backup(str(file_path), temp_dir)

            assert Path(backup_info.backup_path).exists()

            # 修改原文件
            file_path.write_text("modified content", encoding="utf-8")

            # 恢复备份
            success = manager.restore_backup(backup_info.backup_path, str(file_path))

            assert success
            assert file_path.read_text(encoding="utf-8") == original_content

    def test_list_and_cleanup_backups(self):
        """测试列出和清理备份"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建多个备份
            manager = BackupManager(backup_dir=".backup", retention_days=0)

            file_path = Path(temp_dir) / "test.py"
            file_path.write_text("content", encoding="utf-8")

            for _ in range(3):
                manager.create_backup(str(file_path), temp_dir)

            # 列出备份
            backups = manager.list_backups(temp_dir)
            assert len(backups) == 3

            # 清理备份（retention_days=0 会清理所有）
            deleted = manager.cleanup_old_backups(temp_dir, retention_days=0)
            assert deleted == 3


class TestQualityCheckWorkflow:
    """代码质量检查工作流测试"""

    def test_quality_check(self):
        """测试代码质量检查"""
        checker = QualityChecker()

        if not checker.is_available():
            pytest.skip("ruff 未安装")

        code = '''
import os
import sys
x=1
y = 2
'''
        report = checker.check_code(code)

        # ruff 应该能检测到一些问题
        assert report.file_path == "temp.py"


class TestComplexScenarios:
    """复杂场景测试"""

    def test_large_file_migration(self):
        """测试大文件迁移"""
        # 生成大型测试代码
        lines = ['from pyspark.sql import SparkSession']
        lines.append('spark = SparkSession.builder.getOrCreate()')

        for i in range(100):
            lines.append(f'df{i} = spark.read.csv("data{i}.csv")')
            lines.append(f'df{i}.registerTempTable("table{i}")')

        code = '\n'.join(lines)

        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "large_file.py"
            file_path.write_text(code, encoding="utf-8")

            migrator = SparkMigrator(target_version="3.2", dry_run=True)
            result = migrator.migrate_file(str(file_path))

            assert result.success
            assert len(result.changes_applied) > 0

    def test_nested_directory_migration(self):
        """测试嵌套目录迁移"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建嵌套目录结构
            dirs = [
                "src/main",
                "src/utils",
                "src/jobs/daily",
                "src/jobs/weekly",
                "tests",
            ]

            for d in dirs:
                dir_path = Path(temp_dir) / d
                dir_path.mkdir(parents=True, exist_ok=True)
                (dir_path / "spark_job.py").write_text(
                    'df.registerTempTable("test")',
                    encoding="utf-8",
                )

            migrator = SparkMigrator(target_version="3.2", dry_run=True)
            result = migrator.migrate_directory(temp_dir, recursive=True)

            assert result.total_files == 5

    def test_mixed_file_types(self):
        """测试混合文件类型"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建不同类型的文件
            (Path(temp_dir) / "spark.py").write_text(
                'df.registerTempTable("t")', encoding="utf-8"
            )
            (Path(temp_dir) / "config.yml").write_text("key: value", encoding="utf-8")
            (Path(temp_dir) / "readme.md").write_text("# README", encoding="utf-8")
            (Path(temp_dir) / "data.csv").write_text("a,b,c", encoding="utf-8")

            migrator = SparkMigrator(target_version="3.2", dry_run=True)
            result = migrator.migrate_directory(temp_dir, pattern="*.py")

            # 只应该处理 .py 文件
            assert result.total_files == 1
