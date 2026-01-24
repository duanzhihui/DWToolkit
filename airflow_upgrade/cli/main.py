# -*- coding: utf-8 -*-
"""
AirflowUpdt 命令行接口
"""

import json
import sys
from pathlib import Path
from typing import Optional

import click

from airflow_upgrade.core.migrator import DAGMigrator, MigrationReport, BatchMigrationReport
from airflow_upgrade.core.parser import DAGParser
from airflow_upgrade.core.validator import DAGValidator
from airflow_upgrade.tools.ruff_integration import RuffChecker
from airflow_upgrade.tools.flake8_integration import Flake8Checker
from airflow_upgrade.tools.backup_manager import BackupManager


def print_banner():
    """打印工具横幅"""
    banner = """
╔═══════════════════════════════════════════════════════════╗
║        AirflowUpgrade - DAG 升级工具 v0.1.0              ║
║        Airflow 2.x → 3.x 自动化迁移工具                   ║
╚═══════════════════════════════════════════════════════════╝
"""
    click.echo(click.style(banner, fg='cyan'))


@click.group()
@click.version_option(version='0.1.0', prog_name='airflow-upgrade')
def cli():
    """AirflowUpgrade - Airflow DAG 升级工具
    
    将 Airflow 2.x DAG 文件自动升级到 Airflow 3.x 兼容版本
    """
    pass


@cli.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option('--target-version', '-t', default='3.0', help='目标 Airflow 版本')
@click.option('--backup/--no-backup', default=True, help='是否创建备份')
@click.option('--backup-dir', type=click.Path(), help='备份目录')
@click.option('--dry-run', is_flag=True, help='仅分析,不实际修改')
@click.option('--output', '-o', type=click.Path(), help='输出文件路径')
@click.option('--format', 'output_format', type=click.Choice(['text', 'json']), default='text', help='输出格式')
def upgrade(file_path, target_version, backup, backup_dir, dry_run, output, output_format):
    """升级单个 DAG 文件
    
    示例:
        airflow-upgrade upgrade my_dag.py
        airflow-upgrade upgrade my_dag.py --target-version 3.0 --backup
        airflow-upgrade upgrade my_dag.py --dry-run
    """
    print_banner()
    
    click.echo(f"📁 文件: {file_path}")
    click.echo(f"🎯 目标版本: Airflow {target_version}")
    click.echo(f"💾 备份: {'是' if backup else '否'}")
    click.echo(f"🔍 模式: {'仅分析' if dry_run else '执行升级'}")
    click.echo()
    
    migrator = DAGMigrator(
        target_version=target_version,
        backup_enabled=backup,
        backup_dir=backup_dir,
        dry_run=dry_run
    )
    
    report = migrator.migrate_file(file_path)
    
    if output_format == 'json':
        result = format_report_json(report)
        if output:
            Path(output).write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding='utf-8')
            click.echo(f"📄 报告已保存到: {output}")
        else:
            click.echo(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print_migration_report(report)
    
    sys.exit(0 if report.success else 1)


@cli.command('upgrade-dir')
@click.argument('directory', type=click.Path(exists=True))
@click.option('--target-version', '-t', default='3.0', help='目标 Airflow 版本')
@click.option('--recursive/--no-recursive', '-r', default=True, help='是否递归处理子目录')
@click.option('--backup/--no-backup', default=True, help='是否创建备份')
@click.option('--backup-dir', type=click.Path(), help='备份目录')
@click.option('--dry-run', is_flag=True, help='仅分析,不实际修改')
@click.option('--pattern', default='*.py', help='文件匹配模式')
@click.option('--output', '-o', type=click.Path(), help='输出报告路径')
@click.option('--format', 'output_format', type=click.Choice(['text', 'json']), default='text', help='输出格式')
def upgrade_dir(directory, target_version, recursive, backup, backup_dir, dry_run, pattern, output, output_format):
    """批量升级目录中的 DAG 文件
    
    示例:
        airflow-upgrade upgrade-dir /dags/
        airflow-upgrade upgrade-dir /dags/ --recursive --backup
        airflow-upgrade upgrade-dir /dags/ --pattern "dag_*.py"
    """
    print_banner()
    
    click.echo(f"📁 目录: {directory}")
    click.echo(f"🎯 目标版本: Airflow {target_version}")
    click.echo(f"🔄 递归: {'是' if recursive else '否'}")
    click.echo(f"💾 备份: {'是' if backup else '否'}")
    click.echo(f"🔍 模式: {'仅分析' if dry_run else '执行升级'}")
    click.echo()
    
    migrator = DAGMigrator(
        target_version=target_version,
        backup_enabled=backup,
        backup_dir=backup_dir,
        dry_run=dry_run
    )
    
    with click.progressbar(length=100, label='处理中') as bar:
        report = migrator.migrate_directory(directory, recursive=recursive, pattern=pattern)
        bar.update(100)
    
    if output_format == 'json':
        result = format_batch_report_json(report)
        if output:
            Path(output).write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding='utf-8')
            click.echo(f"📄 报告已保存到: {output}")
        else:
            click.echo(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print_batch_report(report)
    
    # 生成回滚脚本
    if backup and not dry_run and report.successful > 0:
        rollback_script = migrator.generate_rollback_script(report)
        rollback_path = Path(backup_dir or directory) / 'rollback.sh'
        rollback_path.write_text(rollback_script, encoding='utf-8')
        click.echo(f"\n📜 回滚脚本已生成: {rollback_path}")
    
    sys.exit(0 if report.failed == 0 else 1)


@cli.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option('--format', 'output_format', type=click.Choice(['text', 'json']), default='text', help='输出格式')
def analyze(file_path, output_format):
    """分析 DAG 文件,不进行修改
    
    示例:
        airflow-upgrade analyze my_dag.py
        airflow-upgrade analyze my_dag.py --format json
    """
    print_banner()
    
    click.echo(f"📁 分析文件: {file_path}")
    click.echo()
    
    parser = DAGParser()
    dag_structure = parser.parse_file(file_path)
    
    if output_format == 'json':
        result = {
            'file_path': dag_structure.file_path,
            'airflow_version': dag_structure.airflow_version,
            'dag_config': {
                'dag_id': dag_structure.dag_config.dag_id if dag_structure.dag_config else None,
                'schedule_interval': dag_structure.dag_config.schedule_interval if dag_structure.dag_config else None,
                'schedule': dag_structure.dag_config.schedule if dag_structure.dag_config else None,
            } if dag_structure.dag_config else None,
            'imports': [{'module': i.module, 'names': i.names} for i in dag_structure.imports],
            'operators': [{'name': o.name, 'task_id': o.task_id, 'type': o.operator_type} for o in dag_structure.operators],
            'dependencies': [{'upstream': d.upstream, 'downstream': d.downstream} for d in dag_structure.dependencies],
            'decorators': dag_structure.decorators,
            'variables': dag_structure.variables,
            'connections': dag_structure.connections,
            'issues': dag_structure.issues,
        }
        click.echo(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print_analysis(dag_structure)


@cli.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option('--target-version', '-t', default='3.0', help='目标 Airflow 版本')
@click.option('--format', 'output_format', type=click.Choice(['text', 'json']), default='text', help='输出格式')
def validate(file_path, target_version, output_format):
    """验证 DAG 文件的 Airflow 3.x 兼容性
    
    示例:
        airflow-upgrade validate my_dag.py
        airflow-upgrade validate my_dag.py --format json
    """
    print_banner()
    
    click.echo(f"📁 验证文件: {file_path}")
    click.echo(f"🎯 目标版本: Airflow {target_version}")
    click.echo()
    
    validator = DAGValidator(target_version=target_version)
    result = validator.validate_file(file_path)
    score = validator.get_compatibility_score(result)
    
    if output_format == 'json':
        output = {
            'is_valid': result.is_valid,
            'airflow3_compatible': result.airflow3_compatible,
            'score': score,
            'errors': result.errors,
            'warnings': result.warnings,
            'info': result.info,
        }
        click.echo(json.dumps(output, indent=2, ensure_ascii=False))
    else:
        print_validation_result(result, score)
    
    sys.exit(0 if result.is_valid else 1)


@cli.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option('--fix', is_flag=True, help='自动修复问题')
@click.option('--tools', default='ruff,flake8', help='使用的检查工具 (逗号分隔)')
@click.option('--format', 'output_format', type=click.Choice(['text', 'json']), default='text', help='输出格式')
def lint(file_path, fix, tools, output_format):
    """代码质量检查
    
    示例:
        airflow-upgrade lint my_dag.py
        airflow-upgrade lint my_dag.py --fix
        airflow-upgrade lint my_dag.py --tools ruff
    """
    print_banner()
    
    click.echo(f"📁 检查文件: {file_path}")
    click.echo(f"🔧 工具: {tools}")
    click.echo(f"🔨 自动修复: {'是' if fix else '否'}")
    click.echo()
    
    tool_list = [t.strip().lower() for t in tools.split(',')]
    results = {}
    
    if 'ruff' in tool_list:
        click.echo("运行 Ruff 检查...")
        ruff = RuffChecker()
        ruff_report = ruff.check_file(file_path, fix=fix)
        results['ruff'] = ruff_report
        
        if output_format == 'text':
            click.echo(ruff.generate_report_text(ruff_report))
            click.echo()
    
    if 'flake8' in tool_list:
        click.echo("运行 Flake8 检查...")
        flake8 = Flake8Checker()
        flake8_report = flake8.check_file(file_path)
        results['flake8'] = flake8_report
        
        if output_format == 'text':
            click.echo(flake8.generate_report_text(flake8_report))
            click.echo()
    
    if output_format == 'json':
        output = {}
        if 'ruff' in results:
            output['ruff'] = {
                'success': results['ruff'].success,
                'issue_count': results['ruff'].issue_count,
                'issues': [
                    {'code': i.code, 'message': i.message, 'line': i.line}
                    for i in results['ruff'].issues
                ]
            }
        if 'flake8' in results:
            output['flake8'] = {
                'success': results['flake8'].success,
                'issue_count': results['flake8'].issue_count,
                'issues': [
                    {'code': i.code, 'message': i.message, 'line': i.line}
                    for i in results['flake8'].issues
                ]
            }
        click.echo(json.dumps(output, indent=2, ensure_ascii=False))
    
    # 计算总问题数
    total_issues = sum(r.issue_count for r in results.values() if hasattr(r, 'issue_count'))
    sys.exit(0 if total_issues == 0 else 1)


@cli.command()
@click.argument('backup_path', type=click.Path(exists=True))
@click.argument('original_path', type=click.Path())
def rollback(backup_path, original_path):
    """从备份恢复文件
    
    示例:
        airflow-upgrade rollback my_dag.20240101_120000.bak my_dag.py
    """
    print_banner()
    
    click.echo(f"📁 备份文件: {backup_path}")
    click.echo(f"📁 目标文件: {original_path}")
    
    backup_manager = BackupManager()
    
    if click.confirm('确认要恢复文件吗?'):
        import shutil
        try:
            shutil.copy2(backup_path, original_path)
            click.echo(click.style("✓ 恢复成功!", fg='green'))
        except Exception as e:
            click.echo(click.style(f"✗ 恢复失败: {e}", fg='red'))
            sys.exit(1)


@cli.command('init-config')
@click.option('--output', '-o', type=click.Path(), default='.airflowupdt.yml', help='配置文件路径')
def init_config(output):
    """生成默认配置文件
    
    示例:
        airflow-upgrade init-config
        airflow-upgrade init-config -o config.yml
    """
    config_content = """# AirflowUpdt 配置文件
# https://github.com/DWToolkit/airflowupdt

# 目标 Airflow 版本
target_version: "3.0"

# 备份设置
backup:
  enabled: true
  directory: ".airflow_upgrade_backup"
  keep_count: 5

# 代码质量检查
quality_checks:
  ruff:
    enabled: true
    auto_fix: false
    line_length: 120
    rules:
      - "AIR"
      - "E"
      - "F"
      - "I"
      - "W"
    ignore:
      - "E501"
  
  flake8:
    enabled: true
    max_line_length: 120
    max_complexity: 10
    ignore:
      - "E501"
      - "W503"

# 升级规则
upgrade_rules:
  # 导入迁移
  import_migration: true
  # 操作符弃用处理
  operator_deprecation: true
  # 配置更新
  config_update: true
  # 参数重命名
  param_rename: true

# 排除模式
exclude_patterns:
  - "__pycache__"
  - ".git"
  - "test_*"
  - "*_test.py"
  - ".venv"
  - "venv"

# 输出设置
output:
  format: "text"  # text 或 json
  verbose: false
  color: true
"""
    
    Path(output).write_text(config_content, encoding='utf-8')
    click.echo(f"✓ 配置文件已生成: {output}")


def print_migration_report(report: MigrationReport):
    """打印迁移报告"""
    click.echo("=" * 60)
    click.echo("迁移报告")
    click.echo("=" * 60)
    
    status = click.style("✓ 成功", fg='green') if report.success else click.style("✗ 失败", fg='red')
    click.echo(f"状态: {status}")
    click.echo(f"源版本: {report.source_version or '未知'}")
    click.echo(f"目标版本: {report.target_version}")
    
    if report.backup_path:
        click.echo(f"备份路径: {report.backup_path}")
    
    if report.transform_result:
        click.echo(f"\n变更数量: {len(report.transform_result.transformations)}")
        
        if report.transform_result.transformations:
            click.echo("\n变更详情:")
            for t in report.transform_result.transformations:
                click.echo(f"  - [{t.category}] {t.description}")
    
    if report.errors:
        click.echo(click.style("\n错误:", fg='red'))
        for error in report.errors:
            click.echo(f"  ✗ {error}")
    
    if report.warnings:
        click.echo(click.style("\n警告:", fg='yellow'))
        for warning in report.warnings:
            click.echo(f"  ⚠ {warning}")


def print_batch_report(report: BatchMigrationReport):
    """打印批量迁移报告"""
    click.echo("=" * 60)
    click.echo("批量迁移报告")
    click.echo("=" * 60)
    
    click.echo(f"总文件数: {report.total_files}")
    click.echo(click.style(f"成功: {report.successful}", fg='green'))
    click.echo(click.style(f"失败: {report.failed}", fg='red') if report.failed > 0 else f"失败: {report.failed}")
    click.echo(f"跳过: {report.skipped}")
    click.echo(f"成功率: {report.success_rate:.1f}%")
    
    if report.failed > 0:
        click.echo("\n失败的文件:")
        for r in report.reports:
            if not r.success:
                click.echo(f"  ✗ {r.file_path}")
                for error in r.errors:
                    click.echo(f"      {error}")


def print_analysis(dag_structure):
    """打印分析结果"""
    click.echo("=" * 60)
    click.echo("DAG 分析结果")
    click.echo("=" * 60)
    
    click.echo(f"文件: {dag_structure.file_path}")
    click.echo(f"检测版本: {dag_structure.airflow_version or '未知'}")
    
    if dag_structure.dag_config:
        click.echo(f"\nDAG 配置:")
        click.echo(f"  DAG ID: {dag_structure.dag_config.dag_id}")
        if dag_structure.dag_config.schedule_interval:
            click.echo(f"  schedule_interval: {dag_structure.dag_config.schedule_interval}")
        if dag_structure.dag_config.schedule:
            click.echo(f"  schedule: {dag_structure.dag_config.schedule}")
    
    if dag_structure.imports:
        click.echo(f"\n导入语句: {len(dag_structure.imports)} 个")
        for imp in dag_structure.imports[:5]:
            click.echo(f"  - {imp.module}")
        if len(dag_structure.imports) > 5:
            click.echo(f"  ... 还有 {len(dag_structure.imports) - 5} 个")
    
    if dag_structure.operators:
        click.echo(f"\n操作符: {len(dag_structure.operators)} 个")
        for op in dag_structure.operators:
            click.echo(f"  - {op.name} (task_id: {op.task_id})")
    
    if dag_structure.dependencies:
        click.echo(f"\n依赖关系: {len(dag_structure.dependencies)} 个")
    
    if dag_structure.decorators:
        click.echo(f"\n装饰器: {', '.join(dag_structure.decorators)}")
    
    if dag_structure.variables:
        click.echo(f"\nAirflow 变量: {len(dag_structure.variables)} 个")
    
    if dag_structure.connections:
        click.echo(f"\n连接: {', '.join(dag_structure.connections)}")
    
    if dag_structure.issues:
        click.echo(click.style("\n问题:", fg='yellow'))
        for issue in dag_structure.issues:
            click.echo(f"  ⚠ {issue}")


def print_validation_result(result, score):
    """打印验证结果"""
    click.echo("=" * 60)
    click.echo("验证结果")
    click.echo("=" * 60)
    
    status = click.style("✓ 通过", fg='green') if result.is_valid else click.style("✗ 未通过", fg='red')
    click.echo(f"状态: {status}")
    
    compat = click.style("✓ 兼容", fg='green') if result.airflow3_compatible else click.style("✗ 不兼容", fg='red')
    click.echo(f"Airflow 3.x 兼容性: {compat}")
    
    # 评分
    grade_colors = {'A': 'green', 'B': 'green', 'C': 'yellow', 'D': 'yellow', 'F': 'red'}
    grade = score['grade']
    score_value = score['score']
    click.echo(f"\n兼容性评分: {click.style(f'{score_value}/100 ({grade})', fg=grade_colors.get(grade, 'white'))}")
    
    click.echo(f"\n问题统计:")
    click.echo(f"  错误: {score['errors']}")
    click.echo(f"  警告: {score['warnings']}")
    click.echo(f"  提示: {score['info']}")
    
    if result.errors:
        click.echo(click.style("\n错误:", fg='red'))
        for error in result.errors:
            click.echo(f"  ✗ {error}")
    
    if result.warnings:
        click.echo(click.style("\n警告:", fg='yellow'))
        for warning in result.warnings:
            click.echo(f"  ⚠ {warning}")
    
    if result.info:
        click.echo(click.style("\n提示:", fg='cyan'))
        for info in result.info:
            click.echo(f"  ℹ {info}")
    
    click.echo(f"\n建议: {score['recommendation']}")


def format_report_json(report: MigrationReport) -> dict:
    """格式化迁移报告为 JSON"""
    return {
        'file_path': report.file_path,
        'success': report.success,
        'source_version': report.source_version,
        'target_version': report.target_version,
        'backup_path': report.backup_path,
        'transformations': [
            {
                'rule': t.rule_name,
                'description': t.description,
                'category': t.category,
                'line': t.line_number
            }
            for t in (report.transform_result.transformations if report.transform_result else [])
        ],
        'errors': report.errors,
        'warnings': report.warnings,
        'timestamp': report.timestamp
    }


def format_batch_report_json(report: BatchMigrationReport) -> dict:
    """格式化批量迁移报告为 JSON"""
    return {
        'total_files': report.total_files,
        'successful': report.successful,
        'failed': report.failed,
        'skipped': report.skipped,
        'success_rate': report.success_rate,
        'reports': [format_report_json(r) for r in report.reports],
        'timestamp': report.timestamp
    }


def main():
    """主入口"""
    cli()


if __name__ == '__main__':
    main()
