# DAG Parser

Airflow DAG文件解析工具,用于提取DAG文件中的任务信息并导出到Excel。

## 功能特性

- 支持解析单个DAG文件或整个目录
- 自动提取DAG和任务信息
- 解析任务命令中的目录和执行文件
- 支持变量解析
- 结果导出到Excel文件

## 安装依赖

```bash
pip install pyyaml pandas openpyxl
```

## 配置说明

配置文件: `config.yml`

### 输入配置

```yaml
input:
  dag_path: "dags/"           # DAG文件路径(文件或目录)
  file_extensions:            # 文件扩展名过滤
    - ".py"
  recursive: true             # 是否递归搜索子目录
```

### 输出配置

```yaml
output:
  excel_file: "dag_parse_result.xlsx"  # 输出Excel文件
  sheet_name: "DAG任务信息"             # Sheet名称
  columns:                              # 输出字段
    - dag_path      # DAG文件目录
    - dag_file      # DAG文件名
    - dag_id        # DAG ID
    - task_id       # 任务ID
    - command       # 任务命令
    - command_dir   # 命令中的目录
    - command_file  # 命令执行的文件
```

### 解析配置

```yaml
parse:
  resolve_variables: true     # 是否解析变量
  extract_env_vars: true      # 是否提取环境变量
  task_types:                 # 支持的任务类型
    - BashOperator
    - PythonOperator
    - SparkSubmitOperator
    - SSHOperator
    - DummyOperator
```

## 使用方法

1. 修改 `config.yml` 配置文件,设置DAG文件路径和输出文件路径

2. 运行解析程序:

```bash
python dag_parser.py
```

3. 查看生成的Excel文件

## 输出字段说明

| 字段 | 说明 |
|------|------|
| dag_path | DAG文件所在目录的绝对路径 |
| dag_file | DAG文件名 |
| dag_id | DAG的唯一标识符 |
| task_id | 任务的唯一标识符 |
| command | 任务执行的完整命令 |
| command_dir | 命令中cd切换的目录,支持变量解析 |
| command_file | 命令最终执行的.py或.sh文件,多个取最后一个 |

## 示例

### 输入DAG文件示例

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

BASE_DIR = "/data/etl"

with DAG(
    dag_id='example_dag',
    start_date=datetime(2024, 1, 1),
) as dag:
    
    task1 = BashOperator(
        task_id='run_etl',
        bash_command=f'cd {BASE_DIR}/scripts && python process.py'
    )
    
    task2 = BashOperator(
        task_id='run_backup',
        bash_command='cd /data/backup && sh backup.sh && python notify.py'
    )
```

### 输出结果示例

| dag_path | dag_file | dag_id | task_id | command | command_dir | command_file |
|----------|----------|--------|---------|---------|-------------|--------------|
| /path/to/dags | example_dag.py | example_dag | run_etl | cd /data/etl/scripts && python process.py | /data/etl/scripts | process.py |
| /path/to/dags | example_dag.py | example_dag | run_backup | cd /data/backup && sh backup.sh && python notify.py | /data/backup | notify.py |

## 注意事项

- 确保DAG文件语法正确,否则可能解析失败
- 变量解析仅支持简单的字符串变量
- 复杂的Python表达式可能无法完全解析
- 建议先在小范围测试后再批量处理

## 许可证

MIT License
