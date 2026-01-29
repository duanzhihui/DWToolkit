# Spark Upgrade Tool

自动将 PySpark 代码从 Spark 2.x 升级到 Spark 3.x 的工具。

## 功能特性

- **自动代码解析**: 使用 Python AST 模块解析 PySpark 代码
- **智能转换**: 自动识别并转换已弃用的 API
- **代码质量检查**: 集成 ruff 进行代码质量检查
- **备份管理**: 自动备份原始文件，支持回滚
- **详细报告**: 生成完整的升级报告

## 安装

```bash
# 从源码安装
pip install -e .

# 或安装开发依赖
pip install -e ".[dev]"
```

## 快速开始

### 升级单个文件

```bash
spark-upgrade upgrade path/to/your_spark_code.py
```

### 升级整个目录

```bash
spark-upgrade upgrade-dir path/to/spark_project/ --recursive
```

### 代码质量检查

```bash
spark-upgrade lint path/to/your_spark_code.py
```

### 预览模式（不修改文件）

```bash
spark-upgrade upgrade path/to/your_spark_code.py --dry-run
```

## 命令行选项

### upgrade 命令

| 选项 | 说明 |
|------|------|
| `--target-version` | 目标 Spark 版本（默认: 3.2）|
| `--dry-run` | 仅分析不修改文件 |
| `--no-backup` | 不创建备份 |
| `--verbose` | 详细输出 |

### upgrade-dir 命令

| 选项 | 说明 |
|------|------|
| `--recursive` | 递归处理子目录 |
| `--pattern` | 文件匹配模式（默认: *.py）|
| `--parallel` | 并行处理数量（默认: 4）|

## 配置文件

创建 `.spark_upgrade.yml` 配置文件：

```yaml
upgrade:
  target_version: "3.2"
  dry_run: false
  parallel_jobs: 4
  file_pattern: "*.py"

backup:
  enabled: true
  directory: ".spark_upgrade_backup"
  retention_days: 30

quality:
  enabled: true
  ruff_config: "pyproject.toml"
  auto_fix: false
  fail_on_error: false
```

## 支持的转换规则

### API 变更

- SparkContext → SparkSession
- RDD 操作 → DataFrame API
- 弃用的配置项更新
- 类型转换语法更新

### 配置更新

- `spark.sql.shuffle.partitions` → 自适应查询执行
- 弃用的配置项替换

### 语法变更

- 导入语句规范化
- 函数调用方式更新

## 开发

### 运行测试

```bash
pytest tests/ -v --cov=spark_upgrade
```

### 代码格式化

```bash
black spark_upgrade/
ruff check spark_upgrade/ --fix
```

## 项目结构

```
spark_upgrade/
├── core/                   # 核心功能模块
│   ├── parser.py          # 代码解析器
│   ├── transformer.py     # 代码转换器
│   ├── migrator.py        # 版本迁移器
│   └── validator.py       # 验证器
├── rules/                  # 转换规则
│   ├── api_changes.py     # API变更规则
│   ├── config_updates.py  # 配置更新规则
│   ├── deprecations.py    # 弃用处理规则
│   └── syntax_changes.py  # 语法变更规则
├── tools/                  # 工具集成
│   ├── quality_checker.py # 代码质量检查
│   ├── backup_manager.py  # 备份管理
│   └── report_generator.py # 报告生成
├── cli/                    # 命令行接口
│   └── main.py            # 主命令行入口
├── config/                 # 配置管理
│   ├── settings.py        # 配置处理
│   └── defaults.py        # 默认配置
└── tests/                  # 测试模块
```

## 许可证

MIT License
