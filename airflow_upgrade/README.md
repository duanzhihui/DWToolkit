# AirflowUpgrade

Airflow 2.x to 3.x DAG 自动化升级工具

## 功能特性

- **自动化升级**: 将 Airflow 2.x DAG 文件自动转换为 Airflow 3.x 兼容版本
- **代码质量检查**: 集成 Ruff 和 Flake8 进行代码质量检查
- **备份管理**: 自动备份原文件，支持一键回滚
- **批量处理**: 支持单文件和目录批量处理
- **详细报告**: 生成详细的迁移报告和兼容性评分

## 安装

```bash
# 从源码安装
cd airflow_upgrade
pip install -e .

# 或者直接安装依赖
pip install -r requirements.txt
```

## 快速开始

### 升级单个文件

```bash
# 基本升级
airflow-upgrade upgrade my_dag.py

# 指定目标版本
airflow-upgrade upgrade my_dag.py --target-version 3.0

# 仅分析不修改
airflow-upgrade upgrade my_dag.py --dry-run

# 不创建备份
airflow-upgrade upgrade my_dag.py --no-backup
```

### 批量升级目录

```bash
# 升级目录中的所有 DAG 文件
airflow-upgrade upgrade-dir /path/to/dags/

# 递归处理子目录
airflow-upgrade upgrade-dir /path/to/dags/ --recursive

# 指定文件模式
airflow-upgrade upgrade-dir /path/to/dags/ --pattern "dag_*.py"

# 指定备份目录
airflow-upgrade upgrade-dir /path/to/dags/ --backup-dir /path/to/backup/
```

### 分析 DAG 文件

```bash
# 分析文件结构
airflow-upgrade analyze my_dag.py

# JSON 格式输出
airflow-upgrade analyze my_dag.py --format json
```

### 验证兼容性

```bash
# 验证 Airflow 3.x 兼容性
airflow-upgrade validate my_dag.py

# JSON 格式输出
airflow-upgrade validate my_dag.py --format json
```

### 代码质量检查

```bash
# 运行代码检查
airflow-upgrade lint my_dag.py

# 自动修复问题
airflow-upgrade lint my_dag.py --fix

# 指定检查工具
airflow-upgrade lint my_dag.py --tools ruff
airflow-upgrade lint my_dag.py --tools flake8
airflow-upgrade lint my_dag.py --tools ruff,flake8
```

### 回滚

```bash
# 从备份恢复
airflow-upgrade rollback my_dag.20240101_120000.bak my_dag.py
```

### 生成配置文件

```bash
# 生成默认配置
airflow-upgrade init-config

# 指定输出路径
airflow-upgrade init-config -o my_config.yml
```

## 配置文件

### 创建配置文件

使用 `init-config` 命令生成默认配置文件：

```bash
# 生成默认配置文件 .airflow_upgrade.yml
airflow-upgrade init-config

# 指定配置文件路径
airflow-upgrade init-config -o my_config.yml
```

### 使用配置文件

工具会自动查找以下配置文件（按优先级）：
1. `.airflow_upgrade.yml`
2. `.airflow_upgrade.yaml`
3. `airflow_upgrade.yml`
4. `airflow_upgrade.yaml`

或者使用 `--config` 选项指定配置文件：

```bash
# 使用指定的配置文件
airflow-upgrade --config my_config.yml upgrade my_dag.py

# 配置文件中的设置会作为默认值，命令行参数会覆盖配置文件
airflow-upgrade --config my_config.yml upgrade my_dag.py --target-version 3.1
```

### 配置文件示例

创建 `.airflow_upgrade.yml` 配置文件：

```yaml
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
```

**配置说明**：
- 命令行参数优先级高于配置文件
- 如果未指定配置文件且当前目录没有配置文件，将使用默认值
- 配置文件支持 YAML 格式

## 主要升级规则

### 导入路径迁移

| Airflow 2.x | Airflow 3.x |
|-------------|-------------|
| `airflow.operators.python_operator` | `airflow.operators.python` |
| `airflow.operators.bash_operator` | `airflow.operators.bash` |
| `airflow.operators.dummy_operator` | `airflow.operators.empty` |
| `airflow.contrib.operators.*` | `airflow.providers.*` |

### 操作符重命名

| Airflow 2.x | Airflow 3.x |
|-------------|-------------|
| `DummyOperator` | `EmptyOperator` |
| `BigQueryOperator` | `BigQueryInsertJobOperator` |

### 参数变更

| Airflow 2.x | Airflow 3.x |
|-------------|-------------|
| `schedule_interval` | `schedule` |
| `concurrency` | `max_active_tasks` |
| `task_concurrency` | `max_active_tis_per_dag` |
| `provide_context` | (已移除，自动提供) |

### Context 变量变更

| Airflow 2.x | Airflow 3.x |
|-------------|-------------|
| `execution_date` | `logical_date` |
| `next_execution_date` | `data_interval_end` |
| `prev_execution_date` | `data_interval_start` |

## 项目结构

```
airflow_upgrade/
├── __init__.py
├── core/
│   ├── __init__.py
│   ├── parser.py          # DAG 解析器
│   ├── transformer.py     # 代码转换器
│   ├── migrator.py        # 版本迁移器
│   └── validator.py       # 验证器
├── rules/
│   ├── __init__.py
│   ├── operators.py       # 操作符转换规则
│   ├── imports.py         # 导入语句规则
│   └── config.py          # 配置更新规则
├── tools/
│   ├── __init__.py
│   ├── ruff_integration.py
│   ├── flake8_integration.py
│   └── backup_manager.py
├── cli/
│   ├── __init__.py
│   └── main.py            # 命令行接口
└── tests/
    └── ...
```

## Python API

```python
from airflow_upgrade.core import DAGParser, DAGMigrator, DAGValidator

# 解析 DAG 文件
parser = DAGParser()
dag_structure = parser.parse_file("my_dag.py")
print(f"DAG ID: {dag_structure.dag_config.dag_id}")
print(f"操作符数量: {len(dag_structure.operators)}")

# 迁移 DAG 文件
migrator = DAGMigrator(target_version="3.0", backup_enabled=True)
report = migrator.migrate_file("my_dag.py")
print(f"迁移状态: {'成功' if report.success else '失败'}")

# 验证兼容性
validator = DAGValidator()
result = validator.validate_file("my_dag.py")
score = validator.get_compatibility_score(result)
print(f"兼容性评分: {score['score']}/100")
```

## 开发

```bash
# 安装开发依赖
pip install -e ".[dev]"

# 运行测试
pytest

# 代码检查
ruff check .
flake8 .
```

## 许可证

MIT License
