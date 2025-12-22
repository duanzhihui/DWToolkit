# Code2Script

将代码文件批量转换为指定格式的脚本文件，支持自定义模板和灵活的YAML配置。

## 功能特性

- **批量转换**: 支持目录递归扫描，批量转换代码文件
- **文件过滤**: 支持通配符模式过滤文件 (如 `*.sql`, `*.hql`)
- **自定义模板**: 支持JSON模板，可在配置文件或外部文件中定义
- **变量替换**: 支持多种变量类型，灵活配置转换规则
- **目录结构保持**: 输出目录结构与输入保持一致
- **命令行支持**: 支持命令行参数覆盖配置
- **模块化设计**: 可作为Python模块导入使用

## 安装

### 依赖

```bash
pip install pyyaml
```

### 下载

将 `code2script.py` 和 `config.yml` 放置到项目目录中。

## 快速开始

### 1. 准备目录结构

```
project/
├── code2script.py
├── config.yml
├── code/                 # 源代码目录
│   ├── query1.sql
│   └── subdir/
│       └── query2.sql
└── script/               # 输出目录 (自动创建)
```

### 2. 运行转换

```bash
# 使用默认配置文件
python code2script.py

# 指定配置文件
python code2script.py -c config.yml

# 指定输入输出目录
python code2script.py -i ./code -o ./script

# 指定文件过滤规则
python code2script.py -p "*.sql" "*.hql"
```

## 配置说明

### 配置文件结构 (config.yml)

```yaml
# 输入配置
input:
  code_dir: "./code"          # 代码文件目录
  patterns:                    # 文件过滤规则
    - "*.sql"
    - "*.hql"

# 输出配置
output:
  script_dir: "./script"       # 脚本输出目录
  extension: ".script"         # 输出文件扩展名

# 模板配置
template:
  file: null                   # 外部模板文件路径 (可选)
  content:                     # 模板内容
    configuration: {}
    connectionName: "mrs-spark"
    content: "<sql_content>"
    directory: "<file_dir>"
    name: "<file_name>"
    owner: "datalake"
    templateVersion: "1.0"
    type: "SparkSQL"

# 变量转换规则
variables:
  sql_content:
    type: "file_content"
    escape_newline: true
  file_dir:
    type: "file_path"
    strip_prefix: ""
    add_prefix: "/"
    use_posix: true
  file_name:
    type: "file_name"
    include_extension: false
```

### 变量类型说明

| 类型 | 说明 | 配置项 |
|------|------|--------|
| `file_content` | 文件内容 | `escape_newline`: 是否转义换行符 |
| `file_path` | 文件路径 | `strip_prefix`: 剔除前缀<br>`add_prefix`: 添加前缀<br>`use_posix`: 使用POSIX路径 |
| `file_name` | 文件名 | `include_extension`: 是否包含扩展名 |
| `literal` | 字面量 | `value`: 固定值 |

### 变量使用

在模板中使用 `<变量名>` 格式引用变量：

```json
{
    "content": "<sql_content>",
    "directory": "<file_dir>",
    "name": "<file_name>"
}
```

## 命令行参数

| 参数 | 简写 | 说明 | 默认值 |
|------|------|------|--------|
| `--config` | `-c` | 配置文件路径 | `config.yml` |
| `--input` | `-i` | 输入代码目录 | 配置文件中的值 |
| `--output` | `-o` | 输出脚本目录 | 配置文件中的值 |
| `--patterns` | `-p` | 文件过滤规则 | 配置文件中的值 |
| `--template` | `-t` | 模板文件路径 | 配置文件中的值 |
| `--file` | `-f` | 转换单个文件 | - |

### 使用示例

```bash
# 基本使用
python code2script.py -c config.yml

# 覆盖输入输出目录
python code2script.py -i /path/to/code -o /path/to/script

# 多种文件类型
python code2script.py -p "*.sql" "*.hql" "*.py"

# 使用自定义模板
python code2script.py -t custom_template.json

# 转换单个文件并输出到控制台
python code2script.py -f ./code/query.sql
```

## 作为模块使用

```python
from code2script import Code2Script

# 使用配置文件初始化
converter = Code2Script(config_path="config.yml")

# 批量转换
output_files = converter.convert()
print(f"转换了 {len(output_files)} 个文件")

# 转换单个文件
result = converter.convert_file("./code/query.sql", code_dir="./code")
print(result)
```

### 编程方式覆盖配置

```python
from code2script import Code2Script

converter = Code2Script(
    config_path="config.yml",
    code_dir="./my_code",
    script_dir="./my_script",
    patterns=["*.sql", "*.hql"]
)

converter.convert()
```

### 自定义变量规则

```python
from code2script import Code2Script

converter = Code2Script(config_path="config.yml")

# 修改变量配置
converter.config["variables"]["file_dir"]["add_prefix"] = "/custom/prefix"
converter.config["variables"]["file_dir"]["strip_prefix"] = "src/"

converter.convert()
```

## 输出示例

### 输入文件: `code/etl/load_data.sql`

```sql
SELECT *
FROM users
WHERE status = 'active';
```

### 输出文件: `script/etl/load_data.script`

```json
{
	"configuration": {},
	"connectionName": "mrs-spark",
	"content": "SELECT *\nFROM users\nWHERE status = 'active';",
	"currentDatabase": "",
	"description": "",
	"directory": "/etl",
	"name": "load_data",
	"owner": "datalake",
	"templateVersion": "1.0",
	"type": "SparkSQL"
}
```

## 高级用法

### 自定义模板文件

创建 `template.json`:

```json
{
    "name": "<file_name>",
    "path": "<file_dir>",
    "sql": "<sql_content>",
    "metadata": {
        "author": "auto-generated",
        "version": "1.0"
    }
}
```

使用:

```bash
python code2script.py -t template.json
```

### 添加自定义变量

在 `config.yml` 中添加:

```yaml
variables:
  custom_owner:
    type: "literal"
    value: "my_team"
```

在模板中使用:

```json
{
    "owner": "<custom_owner>"
}
```

## 注意事项

1. **编码**: 所有文件使用 UTF-8 编码
2. **路径**: 输出路径使用 POSIX 风格 (/)，可通过配置修改
3. **转义**: 文件内容中的特殊字符会自动转义 (`\n`, `\t`, `"`, `\`)
4. **目录**: 输出目录不存在时会自动创建

## License

MIT License
