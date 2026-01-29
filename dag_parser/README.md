# DAG Parser

Airflow DAG文件解析工具,用于提取DAG文件中的任务信息并导出到Excel。

## 功能特性

- 支持解析单个DAG文件或整个目录
- 自动提取DAG和任务信息
- **支持变量递归解析**(包括变量嵌套、f-string、字符串拼接等)
- 解析任务命令中的目录和执行文件
- **可作为Python模块使用**
- **支持命令行参数**
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

### 方式1: 使用配置文件

1. 修改 `config.yml` 配置文件,设置DAG文件路径和输出文件路径

2. 运行解析程序:

```bash
python dag_parser.py
```

### 方式2: 命令行参数

```bash
# 查看帮助
python dag_parser.py -h

# 指定DAG路径
python dag_parser.py -d ./dags

# 指定输出文件
python dag_parser.py -o result.xlsx

# 指定配置文件
python dag_parser.py -c custom_config.yml

# 组合使用
python dag_parser.py -c config.yml -d ./dags -o output.xlsx

# 显示详细日志
python dag_parser.py -v
```

### 方式3: 作为模块使用

```python
from dag_parser import parse_dag, DAGParser

# 方式1: 使用便捷函数
results = parse_dag(
    config_path='config.yml',
    dag_path='./dags',
    output_file='output.xlsx'
)

# 方式2: 使用类
parser = DAGParser('config.yml', dag_path='./dags')
results = parser.parse()

# 处理结果
for task in results:
    print(f"DAG: {task['dag_id']}, Task: {task['task_id']}")
    print(f"Command: {task['command']}")
```

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

## 变量解析功能

工具支持多种变量解析方式:

### 1. 简单变量引用
```python
BASE_DIR = "/data/etl"
task_id = "extract_data"

task = BashOperator(
    task_id=task_id,  # 解析为: extract_data
    bash_command=f'cd {BASE_DIR}'  # 解析为: cd /data/etl
)
```

### 2. 变量嵌套
```python
ENV = "prod"
BASE_DIR = f"/data/{ENV}"
SCRIPT_DIR = f"{BASE_DIR}/scripts"  # 递归解析

task = BashOperator(
    task_id='run_script',
    bash_command=f'cd {SCRIPT_DIR}'  # 解析为: cd /data/prod/scripts
)
```

### 3. 字符串拼接
```python
PREFIX = "etl"
SUFFIX = "task"
task_name = PREFIX + "_" + SUFFIX  # 解析为: etl_task

task = BashOperator(
    task_id=task_name,
    bash_command='echo hello'
)
```

### 4. format方法
```python
template = "cd {dir} && python {script}"
command = template.format(dir="/data", script="run.py")

task = BashOperator(
    task_id='task1',
    bash_command=command  # 解析为: cd /data && python run.py
)
```

## 示例

### 输入DAG文件示例

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

ENV = "prod"
BASE_DIR = f"/data/{ENV}/etl"
SCRIPT_DIR = f"{BASE_DIR}/scripts"

dag_name = "etl_" + ENV
task_prefix = "run"

with DAG(
    dag_id=dag_name,  # 解析为: etl_prod
    start_date=datetime(2024, 1, 1),
) as dag:
    
    task1 = BashOperator(
        task_id=f'{task_prefix}_extract',  # 解析为: run_extract
        bash_command=f'cd {SCRIPT_DIR} && python extract.py'  # 解析为: cd /data/prod/etl/scripts && python extract.py
    )
    
    task2 = BashOperator(
        task_id=f'{task_prefix}_backup',  # 解析为: run_backup
        bash_command='cd /data/backup && sh backup.sh && python notify.py'
    )
```

### 输出结果示例

| dag_path | dag_file | dag_id | task_id | command | command_dir | command_file |
|----------|----------|--------|---------|---------|-------------|--------------|
| /path/to/dags | example_dag.py | etl_prod | run_extract | cd /data/prod/etl/scripts && python extract.py | /data/prod/etl/scripts | extract.py |
| /path/to/dags | example_dag.py | etl_prod | run_backup | cd /data/backup && sh backup.sh && python notify.py | /data/backup | notify.py |

## 命令行参数说明

| 参数 | 简写 | 说明 | 默认值 |
|------|------|------|--------|
| --config | -c | 配置文件路径 | config.yml |
| --dag-path | -d | DAG文件或目录路径 | 配置文件中的设置 |
| --output | -o | 输出Excel文件路径 | 配置文件中的设置 |
| --verbose | -v | 显示详细日志 | False |
| --help | -h | 显示帮助信息 | - |

## 注意事项

- 确保DAG文件语法正确,否则可能解析失败
- 支持递归解析变量嵌套(最大深度10层)
- 支持f-string、字符串拼接、format等多种方式
- 无法解析的复杂表达式会保留原始变量名
- 建议先在小范围测试后再批量处理

## 许可证

MIT License
