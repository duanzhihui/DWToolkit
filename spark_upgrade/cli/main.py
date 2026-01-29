"""命令行主入口"""

import sys
from pathlib import Path
from typing import Optional

import click

from ..config import load_config, SparkUpgradeConfig
from ..core import SparkMigrator, SparkParser, SparkValidator
from ..tools import QualityChecker, BackupManager, ReportGenerator


def print_banner():
    """打印横幅"""
    click.echo(click.style("""
╔═══════════════════════════════════════════════════════════╗
║           Spark Upgrade Tool v1.0.0                       ║
║     PySpark 代码从 Spark 2.x 升级到 Spark 3.x             ║
╚═══════════════════════════════════════════════════════════╝
""", fg="cyan"))


def print_result(result, verbose: bool = False):
    """打印迁移结果"""
    if result.success:
        status = click.style("✓ 成功", fg="green")
    else:
        status = click.style("✗ 失败", fg="red")

    click.echo(f"\n{status} {result.file_path}")

    if result.has_changes:
        click.echo(click.style(f"  变更数: {len(result.changes_applied)}", fg="yellow"))
        if verbose:
            for change in result.changes_applied:
                click.echo(f"    - [{change['rule']}] {change['description']}")

    if result.warnings:
        click.echo(click.style(f"  警告数: {len(result.warnings)}", fg="yellow"))
        if verbose:
            for warning in result.warnings:
                click.echo(f"    ⚠ {warning}")

    if result.errors:
        click.echo(click.style(f"  错误数: {len(result.errors)}", fg="red"))
        for error in result.errors:
            click.echo(f"    ✗ {error}")

    if result.compatibility_score > 0:
        score_color = "green" if result.compatibility_score >= 0.8 else "yellow" if result.compatibility_score >= 0.5 else "red"
        click.echo(click.style(f"  兼容性分数: {result.compatibility_score:.1%}", fg=score_color))

    if result.backup_path:
        click.echo(f"  备份路径: {result.backup_path}")

    if result.dry_run:
        click.echo(click.style("  [预览模式 - 未修改文件]", fg="blue"))


@click.group()
@click.version_option(version="1.0.0", prog_name="spark-upgrade")
@click.option(
    "--config", "-c",
    type=click.Path(exists=True),
    help="配置文件路径"
)
@click.pass_context
def cli(ctx, config: Optional[str]):
    """Spark 代码升级工具 - 将 PySpark 代码从 Spark 2.x 升级到 Spark 3.x"""
    ctx.ensure_object(dict)
    ctx.obj["config"] = load_config(config) if config else load_config()


@cli.command()
@click.argument("file_path", type=click.Path(exists=True))
@click.option(
    "--target-version", "-t",
    default="3.2",
    help="目标 Spark 版本 (默认: 3.2)"
)
@click.option(
    "--dry-run", "-d",
    is_flag=True,
    help="仅分析不修改文件"
)
@click.option(
    "--no-backup",
    is_flag=True,
    help="不创建备份"
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    help="详细输出"
)
@click.option(
    "--report",
    type=click.Path(),
    help="生成报告文件路径"
)
@click.option(
    "--report-format",
    type=click.Choice(["html", "markdown", "json"]),
    default="html",
    help="报告格式 (默认: html)"
)
@click.pass_context
def upgrade(
    ctx,
    file_path: str,
    target_version: str,
    dry_run: bool,
    no_backup: bool,
    verbose: bool,
    report: Optional[str],
    report_format: str,
):
    """升级单个 PySpark 文件"""
    print_banner()

    config: SparkUpgradeConfig = ctx.obj["config"]

    # 合并命令行参数和配置
    if dry_run:
        config.upgrade.dry_run = True
    if no_backup:
        config.backup.enabled = False

    click.echo(f"目标版本: Spark {target_version}")
    click.echo(f"处理文件: {file_path}")

    if config.upgrade.dry_run:
        click.echo(click.style("模式: 预览 (不修改文件)", fg="blue"))
    else:
        click.echo(click.style("模式: 执行迁移", fg="green"))

    click.echo("-" * 50)

    # 创建迁移器
    migrator = SparkMigrator(
        target_version=target_version,
        backup_enabled=config.backup.enabled,
        backup_dir=config.backup.directory,
        dry_run=config.upgrade.dry_run,
    )

    # 执行迁移
    result = migrator.migrate_file(file_path)

    # 打印结果
    print_result(result, verbose)

    # 生成报告
    if report:
        generator = ReportGenerator(target_version)
        report_obj = generator.generate_single_report(result)
        generator.save_report(report_obj, report, report_format)
        click.echo(f"\n报告已保存: {report}")

    # 返回状态码
    sys.exit(0 if result.success else 1)


@cli.command("upgrade-dir")
@click.argument("directory_path", type=click.Path(exists=True))
@click.option(
    "--target-version", "-t",
    default="3.2",
    help="目标 Spark 版本 (默认: 3.2)"
)
@click.option(
    "--recursive", "-r",
    is_flag=True,
    default=True,
    help="递归处理子目录 (默认: 是)"
)
@click.option(
    "--pattern", "-p",
    default="*.py",
    help="文件匹配模式 (默认: *.py)"
)
@click.option(
    "--parallel", "-j",
    default=4,
    type=int,
    help="并行处理数量 (默认: 4)"
)
@click.option(
    "--dry-run", "-d",
    is_flag=True,
    help="仅分析不修改文件"
)
@click.option(
    "--no-backup",
    is_flag=True,
    help="不创建备份"
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    help="详细输出"
)
@click.option(
    "--report",
    type=click.Path(),
    help="生成报告文件路径"
)
@click.option(
    "--report-format",
    type=click.Choice(["html", "markdown", "json"]),
    default="html",
    help="报告格式 (默认: html)"
)
@click.pass_context
def upgrade_dir(
    ctx,
    directory_path: str,
    target_version: str,
    recursive: bool,
    pattern: str,
    parallel: int,
    dry_run: bool,
    no_backup: bool,
    verbose: bool,
    report: Optional[str],
    report_format: str,
):
    """批量升级目录中的 PySpark 文件"""
    print_banner()

    config: SparkUpgradeConfig = ctx.obj["config"]

    if dry_run:
        config.upgrade.dry_run = True
    if no_backup:
        config.backup.enabled = False

    click.echo(f"目标版本: Spark {target_version}")
    click.echo(f"处理目录: {directory_path}")
    click.echo(f"文件模式: {pattern}")
    click.echo(f"递归处理: {'是' if recursive else '否'}")
    click.echo(f"并行数量: {parallel}")

    if config.upgrade.dry_run:
        click.echo(click.style("模式: 预览 (不修改文件)", fg="blue"))
    else:
        click.echo(click.style("模式: 执行迁移", fg="green"))

    click.echo("-" * 50)

    # 创建迁移器
    migrator = SparkMigrator(
        target_version=target_version,
        backup_enabled=config.backup.enabled,
        backup_dir=config.backup.directory,
        dry_run=config.upgrade.dry_run,
    )

    # 设置进度回调
    def progress_callback(file_path: str, current: int, total: int):
        click.echo(f"[{current}/{total}] 处理: {Path(file_path).name}")

    migrator.set_progress_callback(progress_callback)

    # 执行批量迁移
    batch_result = migrator.migrate_directory(
        directory_path,
        recursive=recursive,
        pattern=pattern,
        parallel=parallel,
    )

    # 打印摘要
    click.echo("\n" + "=" * 50)
    click.echo(click.style("迁移摘要", fg="cyan", bold=True))
    click.echo("=" * 50)

    summary = migrator.get_migration_summary(batch_result)

    click.echo(f"总文件数: {summary['total_files']}")
    click.echo(click.style(f"成功迁移: {summary['successful_files']}", fg="green"))
    click.echo(click.style(f"迁移失败: {summary['failed_files']}", fg="red"))
    click.echo(f"跳过处理: {summary['skipped_files']}")
    click.echo(f"总变更数: {summary['total_changes']}")
    click.echo(f"成功率: {summary['success_rate']}")
    click.echo(f"总耗时: {summary['total_time']}")

    # 详细输出
    if verbose:
        click.echo("\n" + "-" * 50)
        click.echo("文件详情:")
        for result in batch_result.results:
            print_result(result, verbose=True)

    # 生成报告
    if report:
        generator = ReportGenerator(target_version)
        report_obj = generator.generate_report(batch_result)
        generator.save_report(report_obj, report, report_format)
        click.echo(f"\n报告已保存: {report}")

    # 返回状态码
    sys.exit(0 if batch_result.failed_files == 0 else 1)


@cli.command()
@click.argument("file_path", type=click.Path(exists=True))
@click.option(
    "--fix",
    is_flag=True,
    help="自动修复可修复的问题"
)
@click.option(
    "--format",
    is_flag=True,
    help="格式化代码"
)
@click.pass_context
def lint(ctx, file_path: str, fix: bool, format: bool):
    """代码质量检查 (使用 ruff)"""
    print_banner()

    checker = QualityChecker()

    if not checker.is_available():
        click.echo(click.style("错误: ruff 未安装", fg="red"))
        click.echo("请运行: pip install ruff")
        sys.exit(1)

    click.echo(f"检查文件: {file_path}")
    click.echo("-" * 50)

    # 执行检查
    report = checker.check_file(file_path)

    # 打印结果
    if report.has_issues:
        click.echo(click.style(f"\n发现 {report.total_issues} 个问题:", fg="yellow"))

        for issue in report.issues:
            severity_color = {
                "error": "red",
                "warning": "yellow",
                "info": "blue",
            }.get(issue.severity, "white")

            click.echo(
                f"  {click.style(issue.code, fg=severity_color)} "
                f"[{issue.line}:{issue.column}] {issue.message}"
            )
    else:
        click.echo(click.style("✓ 没有发现问题", fg="green"))

    # 打印摘要
    click.echo("\n" + "-" * 50)
    summary = checker.get_summary(report)
    click.echo(f"质量分数: {summary['score']}")
    click.echo(f"错误: {summary['error_count']}, 警告: {summary['warning_count']}, 信息: {summary['info_count']}")
    click.echo(f"可自动修复: {summary['fixable_count']}")

    # 自动修复
    if fix and report.fixable_count > 0:
        click.echo("\n正在自动修复...")
        if checker.fix_file(file_path):
            click.echo(click.style("✓ 修复完成", fg="green"))
        else:
            click.echo(click.style("✗ 修复失败", fg="red"))

    # 格式化
    if format:
        click.echo("\n正在格式化代码...")
        if checker.format_file(file_path):
            click.echo(click.style("✓ 格式化完成", fg="green"))
        else:
            click.echo(click.style("✗ 格式化失败", fg="red"))

    sys.exit(0 if not report.has_issues else 1)


@cli.command()
@click.argument("file_path", type=click.Path(exists=True))
@click.option(
    "--target-version", "-t",
    default="3.2",
    help="目标 Spark 版本 (默认: 3.2)"
)
@click.pass_context
def analyze(ctx, file_path: str, target_version: str):
    """分析 PySpark 文件结构"""
    print_banner()

    click.echo(f"分析文件: {file_path}")
    click.echo("-" * 50)

    parser = SparkParser()

    try:
        structure = parser.parse_file(file_path)
    except Exception as e:
        click.echo(click.style(f"解析错误: {e}", fg="red"))
        sys.exit(1)

    # 检查是否包含 Spark 代码
    if not parser.has_spark_code(structure):
        click.echo(click.style("该文件不包含 Spark 相关代码", fg="yellow"))
        sys.exit(0)

    # 打印分析结果
    click.echo(click.style("\n📦 导入", fg="cyan", bold=True))
    spark_imports = parser.get_spark_imports(structure)
    if spark_imports:
        for imp in spark_imports:
            if imp["type"] == "import":
                click.echo(f"  import {imp['module']}")
            else:
                click.echo(f"  from {imp['module']} import {imp['name']}")
    else:
        click.echo("  无 Spark 相关导入")

    click.echo(click.style("\n🔧 SparkSession 使用", fg="cyan", bold=True))
    if structure.spark_session_usage:
        for usage in structure.spark_session_usage:
            click.echo(f"  第 {usage['line']} 行: {usage['name']}")
    else:
        click.echo("  未检测到 SparkSession 使用")

    click.echo(click.style("\n📊 DataFrame 操作", fg="cyan", bold=True))
    if structure.dataframe_operations:
        click.echo(f"  共 {len(structure.dataframe_operations)} 个操作")
        for op in structure.dataframe_operations[:10]:
            click.echo(f"  第 {op['line']} 行: {op['name']}")
        if len(structure.dataframe_operations) > 10:
            click.echo(f"  ... 还有 {len(structure.dataframe_operations) - 10} 个")
    else:
        click.echo("  未检测到 DataFrame 操作")

    click.echo(click.style("\n⚙️ 配置", fg="cyan", bold=True))
    if structure.configurations:
        for conf in structure.configurations:
            click.echo(f"  第 {conf['line']} 行: {conf['name']}")
    else:
        click.echo("  未检测到配置")

    click.echo(click.style("\n⚠️ 弃用 API", fg="yellow", bold=True))
    deprecated = parser.get_deprecated_usage(structure)
    if deprecated:
        for dep in deprecated:
            click.echo(
                f"  第 {dep['line']} 行: {dep['deprecated_api']} "
                f"→ {dep['replacement']}"
            )
    else:
        click.echo("  未检测到弃用 API 使用")

    # 验证
    click.echo(click.style("\n✓ 验证结果", fg="cyan", bold=True))
    validator = SparkValidator(target_version)
    validation = validator.validate(structure.raw_code, file_path)

    summary = validator.get_summary(validation)
    click.echo(f"  语法有效: {'是' if summary['syntax_valid'] else '否'}")
    click.echo(f"  兼容性分数: {summary['compatibility_score']}")
    click.echo(f"  问题数: {summary['total_issues']} (错误: {summary['error_count']}, 警告: {summary['warning_count']})")


@cli.command()
@click.argument("file_path", type=click.Path(exists=True))
@click.option(
    "--target-version", "-t",
    default="3.2",
    help="目标 Spark 版本 (默认: 3.2)"
)
@click.pass_context
def preview(ctx, file_path: str, target_version: str):
    """预览迁移变更"""
    print_banner()

    click.echo(f"预览文件: {file_path}")
    click.echo(f"目标版本: Spark {target_version}")
    click.echo("-" * 50)

    migrator = SparkMigrator(
        target_version=target_version,
        dry_run=True,
    )

    preview_info = migrator.preview_migration(file_path)

    if "error" in preview_info:
        click.echo(click.style(f"错误: {preview_info['error']}", fg="red"))
        sys.exit(1)

    if not preview_info.get("has_spark_code"):
        click.echo(click.style("该文件不包含 Spark 相关代码", fg="yellow"))
        sys.exit(0)

    click.echo(click.style(f"\n将进行 {preview_info['total_changes']} 处变更:", fg="cyan"))

    for change in preview_info.get("changes", []):
        click.echo(f"\n  [{change['rule']}]")
        click.echo(f"  {change['description']}")
        click.echo(click.style(f"  - {change['original']}", fg="red"))
        click.echo(click.style(f"  + {change['replacement']}", fg="green"))

    if preview_info.get("warnings"):
        click.echo(click.style("\n⚠️ 警告:", fg="yellow"))
        for warning in preview_info["warnings"]:
            click.echo(f"  {warning}")

    if preview_info.get("errors"):
        click.echo(click.style("\n✗ 错误:", fg="red"))
        for error in preview_info["errors"]:
            click.echo(f"  {error}")


@cli.command()
@click.option(
    "--directory", "-d",
    type=click.Path(exists=True),
    help="搜索目录"
)
@click.option(
    "--cleanup",
    is_flag=True,
    help="清理过期备份"
)
@click.option(
    "--retention-days",
    default=30,
    type=int,
    help="保留天数 (默认: 30)"
)
@click.pass_context
def backup(ctx, directory: Optional[str], cleanup: bool, retention_days: int):
    """管理备份文件"""
    print_banner()

    config: SparkUpgradeConfig = ctx.obj["config"]
    manager = BackupManager(
        backup_dir=config.backup.directory,
        retention_days=retention_days,
    )

    if cleanup:
        click.echo("正在清理过期备份...")
        deleted = manager.cleanup_old_backups(directory, retention_days)
        click.echo(click.style(f"已删除 {deleted} 个过期备份", fg="green"))
        return

    # 列出备份
    click.echo("备份统计:")
    click.echo("-" * 50)

    stats = manager.get_backup_stats(directory)
    summary = manager.get_summary(stats)

    click.echo(f"总备份数: {summary['total_backups']}")
    click.echo(f"总大小: {summary['total_size']}")
    click.echo(f"最早备份: {summary['oldest_backup'] or '无'}")
    click.echo(f"最新备份: {summary['newest_backup'] or '无'}")

    if stats.files:
        click.echo("\n最近的备份:")
        for backup_info in stats.files[:10]:
            click.echo(
                f"  {backup_info.created_at.strftime('%Y-%m-%d %H:%M')} "
                f"- {Path(backup_info.backup_path).name}"
            )


@cli.command()
@click.argument("backup_path", type=click.Path(exists=True))
@click.option(
    "--target",
    type=click.Path(),
    help="恢复目标路径"
)
@click.pass_context
def restore(ctx, backup_path: str, target: Optional[str]):
    """从备份恢复文件"""
    print_banner()

    config: SparkUpgradeConfig = ctx.obj["config"]
    manager = BackupManager(backup_dir=config.backup.directory)

    click.echo(f"恢复备份: {backup_path}")

    if manager.restore_backup(backup_path, target):
        click.echo(click.style("✓ 恢复成功", fg="green"))
    else:
        click.echo(click.style("✗ 恢复失败", fg="red"))
        sys.exit(1)


@cli.command()
@click.pass_context
def init(ctx):
    """初始化配置文件"""
    print_banner()

    config_path = Path(".spark_upgrade.yml")

    if config_path.exists():
        if not click.confirm("配置文件已存在，是否覆盖?"):
            click.echo("已取消")
            return

    from ..config import save_config, SparkUpgradeConfig

    config = SparkUpgradeConfig()
    save_config(config, config_path)

    click.echo(click.style(f"✓ 配置文件已创建: {config_path}", fg="green"))
    click.echo("\n您可以编辑此文件来自定义配置。")


def main():
    """主入口"""
    cli(obj={})


if __name__ == "__main__":
    main()
