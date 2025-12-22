# JobGen - Script 文件到 Job 文件转换工具

将 `.script` 文件转换为 `.job` 文件的 Python 工具程序。支持自定义模板、配置文件和命令行参数。

## 功能特性

- **批量转换**: 支持目录批量转换，保持原有目录结构
- **单文件转换**: 支持转换单个 script 文件
- **文件过滤**: 支持自定义文件过滤规则 (如 `*.script`)
- **自定义模板**: 支持自定义 job 文件模板
- **变量替换**: 支持从 script 文件提取变量并替换到模板中
- **自增 ID**: 支持 ID 自增长，可配置初始值
- **自定义变量**: 支持在配置文件中定义自定义变量
- **命令行支持**: 支持命令行参数覆盖配置
- **模块化**: 可作为 Python 模块导入使用

## 安装

### 依赖

```bash
pip install pyyaml
```

## 文件结构

```
jobgen/
├── jobgen.py       # 主程序
├── config.yml      # 配置文件
├── template.job    # Job 模板文件
└── readme.md       # 说明文档
```

## 快速开始

### 1. 配置文件 (config.yml)

```yaml
# 输入配置
input:
  script_dir: "./script"      # script 文件目录
  pattern: "*.script"         # 文件过滤规则

# 输出配置
output:
  job_dir: "./job"            # job 文件输出目录

# 模板配置
template:
  file: "./template.job"      # job 模板文件路径

# 变量配置
variables:
  id_start: 1                 # id 自增长初始值
  custom:                     # 自定义变量
    logPath: "/tmp/logs"
    priority: 0

# 转换规则 (可选)
mappings:
  # database: currentDatabase
```

### 2. 模板文件 (template.job)

模板文件使用 `<变量名>` 格式定义占位符：

```json
{
    "id": "<id>",
    "basicConfig": {
        "name": "<name>",
        "description": "<description>",
        "owner": "<owner>",
        "priority": <priority>,
        "logPath": "<logPath>"
    },
    "nodes": [
        {
            "name": "<name>",
            "type": "SparkSQL",
            "properties": {
                "connectionName": "<connectionName>",
                "database": "<currentDatabase>",
                "scriptPath": "<directory>/<name>.script",
                "content": "<content>"
            }
        }
    ],
    "directory": "<directory>",
    "processType": "BATCH"
}
```

### 3. Script 文件格式

输入的 script 文件为 JSON 格式：

```json
{
    "configuration": {},
    "connectionName": "mrs-spark",
    "content": "SELECT * FROM users",
    "currentDatabase": "",
    "description": "",
    "directory": "/etl",
    "name": "query1",
    "owner": "datalake",
    "templateVersion": "1.0",
    "type": "SparkSQL"
}
```

## 使用方法

### 命令行使用

```bash
# 使用默认配置转换目录
python jobgen.py

# 指定配置文件
python jobgen.py -c /path/to/config.yml

# 转换指定目录
python jobgen.py -i ./scripts -o ./jobs

# 转换单个文件
python jobgen.py -f ./script/query.script

# 使用自定义过滤规则
python jobgen.py -p "*.script"

# 指定 ID 初始值
python jobgen.py --id-start 100

# 输出详细信息
python jobgen.py -v
```

### 命令行参数

| 参数 | 简写 | 说明 | 默认值 |
|------|------|------|--------|
| `--config` | `-c` | 配置文件路径 | `config.yml` |
| `--input` | `-i` | Script 文件目录 | 配置文件中的值 |
| `--output` | `-o` | Job 文件输出目录 | 配置文件中的值 |
| `--file` | `-f` | 转换单个 script 文件 | - |
| `--pattern` | `-p` | 文件过滤规则 | `*.script` |
| `--template` | `-t` | Job 模板文件路径 | 配置文件中的值 |
| `--id-start` | - | ID 自增长初始值 | `1` |
| `--verbose` | `-v` | 输出详细信息 | `False` |

### 作为模块使用

```python
from jobgen import JobGenerator

# 创建实例
generator = JobGenerator(config_path="config.yml")

# 转换目录
results = generator.convert_directory(
    script_dir="./scripts",
    output_dir="./jobs",
    pattern="*.script",
    verbose=True
)
print(f"转换了 {len(results)} 个文件")

# 转换单个文件
result = generator.convert_single_file(
    script_path="./script/query.script",
    output_dir="./jobs",
    verbose=True
)
```

## 变量说明

### 内置变量

| 变量名 | 来源 | 说明 |
|--------|------|------|
| `<id>` | 自动生成 | 自增长 ID，初始值可配置 |
| `<name>` | script 文件 | 脚本名称 |
| `<description>` | script 文件 | 描述信息 |
| `<owner>` | script 文件 | 所有者 |
| `<connectionName>` | script 文件 | 连接名称 |
| `<currentDatabase>` | script 文件 | 当前数据库 |
| `<directory>` | script 文件 | 目录路径 |
| `<content>` | script 文件 | SQL 内容 |
| `<type>` | script 文件 | 脚本类型 |

### 自定义变量

在 `config.yml` 中的 `variables.custom` 下定义：

```yaml
variables:
  custom:
    logPath: "/custom/logs"
    priority: 10
    myVar: "custom_value"
```

然后在模板中使用 `<logPath>`、`<priority>`、`<myVar>` 引用。

### 变量映射

在 `config.yml` 中的 `mappings` 下定义变量映射规则：

```yaml
mappings:
  # 将 script 中的 currentDatabase 映射为 database
  database: currentDatabase
  # 固定值
  env: "production"
```

## 示例

### 输入文件

`script/etl/load_data.script`:

```json
{
    "configuration": {},
    "connectionName": "mrs-spark",
    "content": "INSERT INTO target_table SELECT * FROM source_table",
    "currentDatabase": "mydb",
    "description": "Load data job",
    "directory": "/etl",
    "name": "load_data",
    "owner": "datalake",
    "templateVersion": "1.0",
    "type": "SparkSQL"
}
```

### 输出文件

`job/etl/load_data.job`:

```json
{
    "id": "1",
    "basicConfig": {
        "name": "load_data",
        "description": "Load data job",
        "owner": "datalake",
        "priority": 0,
        "logPath": "/tmp/logs"
    },
    "nodes": [
        {
            "name": "load_data",
            "type": "SparkSQL",
            "properties": {
                "connectionName": "mrs-spark",
                "database": "mydb",
                "scriptPath": "/etl/load_data.script",
                "content": "INSERT INTO target_table SELECT * FROM source_table"
            }
        }
    ],
    "directory": "/etl",
    "processType": "BATCH"
}
```

## 注意事项

1. **编码**: 所有文件使用 UTF-8 编码
2. **路径**: 配置文件中的相对路径相对于配置文件所在目录
3. **JSON 验证**: 程序会验证生成的 job 文件是否为有效 JSON
4. **目录结构**: 输出目录会自动创建，并保持与输入目录相同的结构
5. **特殊字符**: content 中的换行符、引号等特殊字符会自动转义

## License

MIT License
