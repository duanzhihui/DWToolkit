"""报告生成器 - 生成迁移报告"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from jinja2 import Environment, BaseLoader

from ..core.migrator import BatchMigrationResult, MigrationResult


@dataclass
class MigrationReport:
    """迁移报告"""
    title: str
    generated_at: datetime
    target_version: str
    summary: Dict[str, Any]
    file_results: List[Dict[str, Any]]
    recommendations: List[str] = field(default_factory=list)


class ReportGenerator:
    """报告生成器"""

    HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ title }}</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin-bottom: 20px; }
        .header h1 { font-size: 24px; margin-bottom: 10px; }
        .header .meta { opacity: 0.9; font-size: 14px; }
        .card { background: white; border-radius: 10px; padding: 20px; margin-bottom: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .card h2 { font-size: 18px; margin-bottom: 15px; color: #444; border-bottom: 2px solid #667eea; padding-bottom: 10px; }
        .summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }
        .summary-item { background: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center; }
        .summary-item .value { font-size: 28px; font-weight: bold; color: #667eea; }
        .summary-item .label { font-size: 12px; color: #666; margin-top: 5px; }
        .file-list { list-style: none; }
        .file-item { border: 1px solid #eee; border-radius: 8px; margin-bottom: 10px; overflow: hidden; }
        .file-header { padding: 15px; background: #f8f9fa; cursor: pointer; display: flex; justify-content: space-between; align-items: center; }
        .file-header:hover { background: #e9ecef; }
        .file-name { font-weight: 500; word-break: break-all; }
        .file-status { padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: 500; }
        .status-success { background: #d4edda; color: #155724; }
        .status-warning { background: #fff3cd; color: #856404; }
        .status-error { background: #f8d7da; color: #721c24; }
        .file-details { padding: 15px; border-top: 1px solid #eee; display: none; }
        .file-item.expanded .file-details { display: block; }
        .change-list { list-style: none; margin-top: 10px; }
        .change-item { padding: 10px; background: #f8f9fa; border-radius: 5px; margin-bottom: 8px; font-size: 14px; }
        .change-item .rule { font-weight: 500; color: #667eea; }
        .change-item .desc { color: #666; margin-top: 5px; }
        .warning-list, .error-list { margin-top: 10px; }
        .warning-item { padding: 8px 12px; background: #fff3cd; border-radius: 5px; margin-bottom: 5px; font-size: 13px; }
        .error-item { padding: 8px 12px; background: #f8d7da; border-radius: 5px; margin-bottom: 5px; font-size: 13px; }
        .recommendations { list-style: none; }
        .recommendations li { padding: 10px 15px; background: #e7f3ff; border-left: 4px solid #667eea; margin-bottom: 10px; border-radius: 0 5px 5px 0; }
        .score-bar { height: 8px; background: #e9ecef; border-radius: 4px; overflow: hidden; margin-top: 10px; }
        .score-fill { height: 100%; background: linear-gradient(90deg, #28a745, #ffc107, #dc3545); border-radius: 4px; }
        .footer { text-align: center; padding: 20px; color: #666; font-size: 12px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{{ title }}</h1>
            <div class="meta">
                <span>生成时间: {{ generated_at }}</span> |
                <span>目标版本: Spark {{ target_version }}</span>
            </div>
        </div>

        <div class="card">
            <h2>📊 迁移摘要</h2>
            <div class="summary-grid">
                <div class="summary-item">
                    <div class="value">{{ summary.total_files }}</div>
                    <div class="label">总文件数</div>
                </div>
                <div class="summary-item">
                    <div class="value">{{ summary.successful_files }}</div>
                    <div class="label">成功迁移</div>
                </div>
                <div class="summary-item">
                    <div class="value">{{ summary.failed_files }}</div>
                    <div class="label">迁移失败</div>
                </div>
                <div class="summary-item">
                    <div class="value">{{ summary.skipped_files }}</div>
                    <div class="label">跳过处理</div>
                </div>
                <div class="summary-item">
                    <div class="value">{{ summary.total_changes }}</div>
                    <div class="label">总变更数</div>
                </div>
                <div class="summary-item">
                    <div class="value">{{ summary.success_rate }}</div>
                    <div class="label">成功率</div>
                </div>
            </div>
        </div>

        <div class="card">
            <h2>📁 文件详情</h2>
            <ul class="file-list">
                {% for file in file_results %}
                <li class="file-item" onclick="this.classList.toggle('expanded')">
                    <div class="file-header">
                        <span class="file-name">{{ file.file_path }}</span>
                        <span class="file-status {% if file.success %}status-success{% elif file.warnings %}status-warning{% else %}status-error{% endif %}">
                            {% if file.success %}✓ 成功{% elif file.warnings %}⚠ 警告{% else %}✗ 失败{% endif %}
                        </span>
                    </div>
                    <div class="file-details">
                        {% if file.changes %}
                        <strong>变更内容:</strong>
                        <ul class="change-list">
                            {% for change in file.changes %}
                            <li class="change-item">
                                <div class="rule">{{ change.rule }}</div>
                                <div class="desc">{{ change.description }}</div>
                            </li>
                            {% endfor %}
                        </ul>
                        {% endif %}
                        {% if file.warnings %}
                        <div class="warning-list">
                            <strong>警告:</strong>
                            {% for warning in file.warnings %}
                            <div class="warning-item">⚠ {{ warning }}</div>
                            {% endfor %}
                        </div>
                        {% endif %}
                        {% if file.errors %}
                        <div class="error-list">
                            <strong>错误:</strong>
                            {% for error in file.errors %}
                            <div class="error-item">✗ {{ error }}</div>
                            {% endfor %}
                        </div>
                        {% endif %}
                        {% if file.compatibility_score %}
                        <div style="margin-top: 15px;">
                            <strong>兼容性分数: {{ file.compatibility_score }}</strong>
                            <div class="score-bar">
                                <div class="score-fill" style="width: {{ file.compatibility_score }}"></div>
                            </div>
                        </div>
                        {% endif %}
                    </div>
                </li>
                {% endfor %}
            </ul>
        </div>

        {% if recommendations %}
        <div class="card">
            <h2>💡 建议</h2>
            <ul class="recommendations">
                {% for rec in recommendations %}
                <li>{{ rec }}</li>
                {% endfor %}
            </ul>
        </div>
        {% endif %}

        <div class="footer">
            由 Spark Upgrade Tool 生成 | {{ generated_at }}
        </div>
    </div>
</body>
</html>
"""

    MARKDOWN_TEMPLATE = """# {{ title }}

**生成时间:** {{ generated_at }}  
**目标版本:** Spark {{ target_version }}

## 📊 迁移摘要

| 指标 | 值 |
|------|-----|
| 总文件数 | {{ summary.total_files }} |
| 成功迁移 | {{ summary.successful_files }} |
| 迁移失败 | {{ summary.failed_files }} |
| 跳过处理 | {{ summary.skipped_files }} |
| 总变更数 | {{ summary.total_changes }} |
| 成功率 | {{ summary.success_rate }} |

## 📁 文件详情

{% for file in file_results %}
### {{ file.file_path }}

**状态:** {% if file.success %}✓ 成功{% elif file.warnings %}⚠ 警告{% else %}✗ 失败{% endif %}

{% if file.changes %}
**变更内容:**
{% for change in file.changes %}
- **{{ change.rule }}**: {{ change.description }}
{% endfor %}
{% endif %}

{% if file.warnings %}
**警告:**
{% for warning in file.warnings %}
- ⚠ {{ warning }}
{% endfor %}
{% endif %}

{% if file.errors %}
**错误:**
{% for error in file.errors %}
- ✗ {{ error }}
{% endfor %}
{% endif %}

{% if file.compatibility_score %}
**兼容性分数:** {{ file.compatibility_score }}
{% endif %}

---
{% endfor %}

{% if recommendations %}
## 💡 建议

{% for rec in recommendations %}
- {{ rec }}
{% endfor %}
{% endif %}

---
*由 Spark Upgrade Tool 生成*
"""

    def __init__(self, target_version: str = "3.2"):
        self.target_version = target_version
        self.env = Environment(loader=BaseLoader())

    def generate_report(
        self,
        batch_result: BatchMigrationResult,
        title: str = "Spark 代码迁移报告",
    ) -> MigrationReport:
        """
        生成迁移报告
        
        Args:
            batch_result: 批量迁移结果
            title: 报告标题
            
        Returns:
            MigrationReport 报告对象
        """
        # 构建摘要
        summary = self._build_summary(batch_result)

        # 构建文件结果
        file_results = self._build_file_results(batch_result.results)

        # 生成建议
        recommendations = self._generate_recommendations(batch_result)

        return MigrationReport(
            title=title,
            generated_at=datetime.now(),
            target_version=self.target_version,
            summary=summary,
            file_results=file_results,
            recommendations=recommendations,
        )

    def generate_single_report(
        self,
        result: MigrationResult,
        title: str = "Spark 代码迁移报告",
    ) -> MigrationReport:
        """
        为单个文件生成报告
        
        Args:
            result: 迁移结果
            title: 报告标题
            
        Returns:
            MigrationReport 报告对象
        """
        summary = {
            "total_files": 1,
            "successful_files": 1 if result.success else 0,
            "failed_files": 0 if result.success else 1,
            "skipped_files": 0,
            "total_changes": len(result.changes_applied),
            "success_rate": "100%" if result.success else "0%",
        }

        file_results = self._build_file_results([result])
        recommendations = self._generate_single_recommendations(result)

        return MigrationReport(
            title=title,
            generated_at=datetime.now(),
            target_version=self.target_version,
            summary=summary,
            file_results=file_results,
            recommendations=recommendations,
        )

    def render_html(self, report: MigrationReport) -> str:
        """
        渲染 HTML 报告
        
        Args:
            report: 报告对象
            
        Returns:
            HTML 字符串
        """
        template = self.env.from_string(self.HTML_TEMPLATE)
        return template.render(
            title=report.title,
            generated_at=report.generated_at.strftime("%Y-%m-%d %H:%M:%S"),
            target_version=report.target_version,
            summary=report.summary,
            file_results=report.file_results,
            recommendations=report.recommendations,
        )

    def render_markdown(self, report: MigrationReport) -> str:
        """
        渲染 Markdown 报告
        
        Args:
            report: 报告对象
            
        Returns:
            Markdown 字符串
        """
        template = self.env.from_string(self.MARKDOWN_TEMPLATE)
        return template.render(
            title=report.title,
            generated_at=report.generated_at.strftime("%Y-%m-%d %H:%M:%S"),
            target_version=report.target_version,
            summary=report.summary,
            file_results=report.file_results,
            recommendations=report.recommendations,
        )

    def render_json(self, report: MigrationReport) -> str:
        """
        渲染 JSON 报告
        
        Args:
            report: 报告对象
            
        Returns:
            JSON 字符串
        """
        data = {
            "title": report.title,
            "generated_at": report.generated_at.isoformat(),
            "target_version": report.target_version,
            "summary": report.summary,
            "file_results": report.file_results,
            "recommendations": report.recommendations,
        }
        return json.dumps(data, ensure_ascii=False, indent=2)

    def save_report(
        self,
        report: MigrationReport,
        output_path: str,
        format: str = "html",
    ) -> None:
        """
        保存报告到文件
        
        Args:
            report: 报告对象
            output_path: 输出路径
            format: 格式 (html, markdown, json)
        """
        if format == "html":
            content = self.render_html(report)
        elif format == "markdown" or format == "md":
            content = self.render_markdown(report)
        elif format == "json":
            content = self.render_json(report)
        else:
            raise ValueError(f"不支持的格式: {format}")

        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

    def _build_summary(self, batch_result: BatchMigrationResult) -> Dict[str, Any]:
        """构建摘要"""
        total_changes = sum(
            len(r.changes_applied) for r in batch_result.results
        )

        return {
            "total_files": batch_result.total_files,
            "successful_files": batch_result.successful_files,
            "failed_files": batch_result.failed_files,
            "skipped_files": batch_result.skipped_files,
            "total_changes": total_changes,
            "success_rate": f"{batch_result.success_rate:.1%}",
            "total_time": batch_result.total_time,
        }

    def _build_file_results(
        self, results: List[MigrationResult]
    ) -> List[Dict[str, Any]]:
        """构建文件结果列表"""
        file_results = []

        for result in results:
            file_results.append({
                "file_path": result.file_path,
                "success": result.success,
                "changes": result.changes_applied,
                "warnings": result.warnings,
                "errors": result.errors,
                "compatibility_score": f"{result.compatibility_score:.1%}",
                "migration_time": result.migration_time,
            })

        return file_results

    def _generate_recommendations(
        self, batch_result: BatchMigrationResult
    ) -> List[str]:
        """生成建议"""
        recommendations = []

        # 检查失败率
        if batch_result.failed_files > 0:
            recommendations.append(
                f"有 {batch_result.failed_files} 个文件迁移失败，请检查错误信息并手动处理"
            )

        # 检查警告
        total_warnings = sum(len(r.warnings) for r in batch_result.results)
        if total_warnings > 0:
            recommendations.append(
                f"共有 {total_warnings} 条警告，建议检查并确认这些变更是否符合预期"
            )

        # 检查兼容性分数
        low_score_files = [
            r for r in batch_result.results
            if r.compatibility_score < 0.8 and r.compatibility_score > 0
        ]
        if low_score_files:
            recommendations.append(
                f"有 {len(low_score_files)} 个文件兼容性分数较低，建议重点检查"
            )

        # 通用建议
        recommendations.extend([
            "建议在测试环境中验证迁移后的代码",
            "检查是否需要更新 Spark 配置以启用 Spark 3.x 新特性",
            "考虑启用自适应查询执行 (AQE) 以提升性能",
        ])

        return recommendations

    def _generate_single_recommendations(
        self, result: MigrationResult
    ) -> List[str]:
        """为单个文件生成建议"""
        recommendations = []

        if not result.success:
            recommendations.append("迁移失败，请检查错误信息并手动处理")

        if result.warnings:
            recommendations.append(
                f"有 {len(result.warnings)} 条警告，建议检查并确认"
            )

        if result.compatibility_score < 0.8:
            recommendations.append("兼容性分数较低，建议仔细检查代码")

        recommendations.append("建议在测试环境中验证迁移后的代码")

        return recommendations
