# SQL代码提取模块 (sql_parser)

从 `*.py` `*.sh` `*.hql` `*.sql` 等代码文件中提取SQL语句，支持 Hive SQL, Spark SQL, Impala SQL。

## 功能特性

- **多文件类型支持**: Python (.py), Shell (.sh), SQL (.sql), HQL (.hql)
- **SQL类型自动检测**: 自动识别 Hive/Spark/Impala/Generic SQL
- **保持目录结构**: 输出文件保持与输入相同的目录结构
- **双格式输出**: 每个输入文件生成 `.sql` 和 `.json` 两个输出文件
- **命令行工具**: 支持命令行参数，方便集成到脚本中

## 安装

无需安装额外依赖，使用Python标准库即可运行。

```bash
# Python 3.6+
python sql_parser.py --help
```

## 命令行用法

### 基本用法

```bash
# 解析目录
python sql_parser.py -i <输入目录> -o <输出目录>

# 解析单个文件
python sql_parser.py -f <输入文件> -o <输出目录>
```

### 参数说明

| 参数 | 简写 | 说明 | 必需 |
|------|------|------|------|
| `--input-dir` | `-i` | 输入目录路径 | 与 `-f` 二选一 |
| `--input-file` | `-f` | 输入文件路径 | 与 `-i` 二选一 |
| `--output-dir` | `-o` | 输出目录路径 | 是 |
| `--extensions` | `-e` | 文件扩展名列表 | 否 (默认: .py .sh .sql .hql) |
| `--no-recursive` | | 不递归处理子目录 | 否 |
| `--verbose` | `-v` | 显示详细输出 | 否 |

### 示例

```bash
# 解析 input 目录，输出到 output 目录
python sql_parser.py -i ./input -o ./output

# 解析单个Python文件
python sql_parser.py -f ./input/sample_code.py -o ./output

# 只处理 .py 和 .sql 文件
python sql_parser.py -i ./input -o ./output -e .py .sql

# 不递归处理子目录
python sql_parser.py -i ./input -o ./output --no-recursive
```

## 输出文件说明

### 目录结构

输出文件保持与输入相同的目录结构：

```
input/                          output/
├── sample_queries.sql    ->    ├── sample_queries.sql
├── sample_script.sh      ->    ├── sample_queries.json
└── folder/               ->    ├── sample_script.sql
    ├── sample_code.py    ->    ├── sample_script.json
    └── sample_etl.hql    ->    ├── _summary.json
                                └── folder/
                                    ├── sample_code.sql
                                    ├── sample_code.json
                                    ├── sample_etl.sql
                                    └── sample_etl.json
```

### .sql 文件格式

```sql
-- SQL #1 from D:\path\to\file.py:18-29
-- Type: hive
CREATE EXTERNAL TABLE IF NOT EXISTS db_test.user_info (...);

-- SQL #2 from D:\path\to\file.py:32-59
-- Type: generic
SELECT * FROM users WHERE id = 1;
```

### .json 文件格式

```json
[
  {
    "sql": "CREATE EXTERNAL TABLE ...",
    "source_file": "D:\\path\\to\\file.py",
    "line_start": 18,
    "line_end": 29,
    "sql_type": "hive",
    "context": "create_table_sql"
  },
  {
    "sql": "SELECT * FROM users ...",
    "source_file": "D:\\path\\to\\file.py",
    "line_start": 32,
    "line_end": 59,
    "sql_type": "generic",
    "context": "query_sql"
  }
]
```

### _summary.json 文件

处理目录时会生成汇总文件：

```json
{
  "total_files": 4,
  "total_sqls": 50,
  "processed_files": [
    {
      "input": "D:\\path\\input\\sample.py",
      "sql_output": "D:\\path\\output\\sample.sql",
      "json_output": "D:\\path\\output\\sample.json",
      "sql_count": 10
    }
  ],
  "errors": []
}
```

## 作为模块使用

### 导入方式

将 `sql_parser.py` 放入项目目录或 Python 路径中，然后导入：

```python
# 方式1: 直接导入（sql_parser.py 在当前目录或 PYTHONPATH 中）
from sql_parser import SQLParser, SQLExtractor, ExtractedSQL, SQLType

# 方式2: 从包导入（如果 sql_parser 是一个包目录）
import sys
sys.path.append("D:/github/duanzhihui/PyToolkit")
from sql_parser.sql_parser import SQLParser
```

### 核心类说明

| 类名 | 说明 |
|------|------|
| `SQLParser` | 主入口类，提供高层API |
| `SQLExtractor` | 底层提取器，包含具体的提取逻辑 |
| `ExtractedSQL` | 数据类，存储提取的SQL信息 |
| `SQLType` | 枚举类，SQL类型 (HIVE/SPARK/IMPALA/GENERIC) |

### ExtractedSQL 数据结构

```python
@dataclass
class ExtractedSQL:
    sql: str              # SQL语句内容
    source_file: str      # 来源文件路径
    line_start: int       # 起始行号
    line_end: int         # 结束行号
    sql_type: SQLType     # SQL类型 (HIVE/SPARK/IMPALA/GENERIC)
    context: str          # 上下文信息（如变量名）
```

### SQLParser 类方法

#### parse_file(file_path) -> List[ExtractedSQL]
解析单个文件，返回提取的SQL列表。

```python
from sql_parser import SQLParser

parser = SQLParser()
sqls = parser.parse_file("path/to/file.py")

for sql in sqls:
    print(f"SQL: {sql.sql[:50]}...")
    print(f"类型: {sql.sql_type.value}")
    print(f"位置: {sql.source_file}:{sql.line_start}-{sql.line_end}")
```

#### parse_directory(dir_path, extensions, recursive) -> List[ExtractedSQL]
解析目录下的所有文件。

```python
sqls = parser.parse_directory(
    "path/to/dir",
    extensions=['.py', '.sql', '.sh', '.hql'],  # 可选，默认全部
    recursive=True  # 是否递归子目录
)
print(f"共提取 {len(sqls)} 条SQL")
```

#### parse_string(content, file_type) -> List[ExtractedSQL]
解析字符串内容。

```python
python_code = '''
sql = """
SELECT * FROM users WHERE id = 1
"""
spark.sql("INSERT INTO logs SELECT * FROM events")
'''

sqls = parser.parse_string(python_code, file_type='py')
# file_type: 'py', 'sh', 'sql', 'hql'
```

#### process_file_to_output(input_file, output_dir, input_base_dir) -> Tuple
处理单个文件并生成 .sql 和 .json 输出文件。

```python
sql_count, sql_file, json_file = parser.process_file_to_output(
    "input/sample.py",
    "output/"
)
print(f"提取 {sql_count} 条SQL")
print(f"SQL文件: {sql_file}")
print(f"JSON文件: {json_file}")
```

#### process_directory_to_output(input_dir, output_dir, extensions, recursive) -> Dict
处理目录并生成输出文件，保持目录结构。

```python
results = parser.process_directory_to_output(
    "input/",
    "output/",
    extensions=['.py', '.sql', '.sh', '.hql'],
    recursive=True
)

print(f"处理文件数: {results['total_files']}")
print(f"提取SQL数: {results['total_sqls']}")
print(f"错误数: {len(results['errors'])}")

# 遍历处理结果
for file_info in results['processed_files']:
    print(f"{file_info['input']} -> {file_info['sql_count']} 条SQL")
```

#### write_to_file(sqls, output_path, format) -> None
将SQL列表写入文件。

```python
# 写入SQL格式
parser.write_to_file(sqls, "output.sql", format='sql')

# 写入JSON格式
parser.write_to_file(sqls, "output.json", format='json')

# 写入TXT格式（带详细信息）
parser.write_to_file(sqls, "output.txt", format='txt')
```

#### format_result(sqls) -> str
格式化结果为可读字符串。

```python
print(parser.format_result(sqls))
# 输出:
# ============================================================
# SQL提取结果 (共 10 条)
# ============================================================
# 【按类型统计】
#   - generic: 5 条
#   - hive: 3 条
#   - impala: 2 条
# ...
```

### 完整示例

```python
#!/usr/bin/env python3
from sql_parser import SQLParser, SQLType

def main():
    parser = SQLParser()
    
    # 示例1: 解析单个文件
    print("=== 解析单个文件 ===")
    sqls = parser.parse_file("input/sample_code.py")
    print(f"提取到 {len(sqls)} 条SQL")
    
    # 按类型过滤
    hive_sqls = [s for s in sqls if s.sql_type == SQLType.HIVE]
    print(f"其中 Hive SQL: {len(hive_sqls)} 条")
    
    # 示例2: 解析目录
    print("\n=== 解析目录 ===")
    all_sqls = parser.parse_directory("input/", recursive=True)
    print(f"目录共提取 {len(all_sqls)} 条SQL")
    
    # 示例3: 解析字符串
    print("\n=== 解析字符串 ===")
    code = '''
    query = """
    SELECT user_id, COUNT(*) as cnt
    FROM orders
    GROUP BY user_id
    """
    '''
    sqls = parser.parse_string(code, file_type='py')
    for sql in sqls:
        print(f"SQL: {sql.sql}")
    
    # 示例4: 批量处理并输出
    print("\n=== 批量处理 ===")
    results = parser.process_directory_to_output(
        "input/",
        "output/",
        extensions=['.py', '.sql'],
        recursive=True
    )
    print(f"处理完成: {results['total_files']} 个文件, {results['total_sqls']} 条SQL")

if __name__ == "__main__":
    main()
```

### 与其他模块集成

```python
# 与 sql_table_parser 配合使用
from sql_parser import SQLParser
from sql_table_parser import SQLTableParser

# 1. 先提取SQL
parser = SQLParser()
sqls = parser.parse_file("etl_script.py")

# 2. 再解析表名
table_parser = SQLTableParser()
for sql in sqls:
    result = table_parser.parse_sql(sql.sql)
    print(f"SQL类型: {sql.sql_type.value}")
    print(f"涉及表: {result.get('all_tables', set())}")
```

## 支持的SQL提取场景

### Python文件 (.py)

```python
# 三引号字符串
sql = """
SELECT * FROM users
"""

# f-string
sql = f"""
SELECT * FROM {table_name}
"""

# spark.sql() 调用
df = spark.sql("SELECT * FROM users")

# cursor.execute()
cursor.execute("INSERT INTO logs VALUES (%s)")
```

### Shell脚本 (.sh)

```bash
# hive -e
hive -e "SELECT * FROM users"

# beeline
beeline -u "jdbc:hive2://..." -e "SELECT * FROM users"

# impala-shell
impala-shell -q "SELECT * FROM users"

# heredoc
hive << EOF
SELECT * FROM users
EOF

# 变量赋值
SQL="SELECT * FROM users"
```

### SQL/HQL文件 (.sql, .hql)

直接按分号分割提取SQL语句。

## SQL类型检测

自动检测SQL类型：

| 类型 | 特征关键字 |
|------|-----------|
| Hive | MSCK, LOAD DATA, PARTITIONED BY, STORED AS |
| Spark | CACHE TABLE, UNCACHE TABLE, REFRESH TABLE |
| Impala | INVALIDATE METADATA, COMPUTE STATS |
| Generic | 其他通用SQL |

## License

MIT
