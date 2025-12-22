# SQL表名解析器

## 功能介绍

这是一个使用正则表达式从SQL代码中提取所有表名的Python程序，可以解析**SQL字符串**以及**SQL文件 / 含有 SQL 语句的脚本文件**（`.sql` / `.py` / `.sh`），支持多种SQL语句类型，并自动移除注释和常见的非SQL代码片段。

## 文件说明

- **sql_table_parser.py** - 主程序，包含 `SQLTableParser` 类和 `main` 演示入口
- **sample_queries.sql** - 示例SQL文件，包含各种SQL语句
- **sample_script.py** - 示例Python脚本，演示从脚本中提取SQL表名
- **sample_script.sh** - 示例Shell脚本，演示从脚本中提取SQL表名
- **parse_result.txt** - 解析结果输出文件（运行后生成）

## 支持的SQL语句类型

- ✅ SELECT ... FROM
- ✅ JOIN (INNER/LEFT/RIGHT/FULL/CROSS)
- ✅ INSERT INTO
- ✅ UPDATE
- ✅ DELETE FROM
- ✅ CREATE TABLE
- ✅ DROP TABLE
- ✅ ALTER TABLE
- ✅ TRUNCATE TABLE
- ✅ WITH (CTE - Common Table Expression，CTE 临时表会从最终表名结果中剔除)
- ✅ 子查询
- ✅ UNION查询

## 支持的表名格式

- `table_name` - 普通表名
- `schema.table_name` - 带schema的表名
- `` `table_name` `` - 反引号包裹的表名
- `"table_name"` - 双引号包裹的表名

## 使用方法

### 方法1：直接运行主程序

```bash
python sql_table_parser.py
```

程序会：
1. 解析内置的示例SQL字符串
2. 解析 `sample_queries.sql` 文件
3. 将结果保存到 `parse_result.txt`

### 方法2：在代码中使用

```python
from sql_table_parser import SQLTableParser

# 创建解析器实例
parser = SQLTableParser()

# 解析SQL字符串
sql = """
SELECT u.name, o.order_id
FROM users u
JOIN orders o ON u.id = o.user_id
"""
result = parser.parse_sql(sql)

# 打印格式化结果
print(parser.format_result(result))

# 或者直接获取所有表名
all_tables = result['all_tables']
print(f"找到的表名: {all_tables}")
```

### 方法3：解析SQL文件

```python
from sql_table_parser import SQLTableParser

parser = SQLTableParser()

# 解析 .sql 文件
result = parser.parse_file("your_sql_file.sql")

# 查看结果
print(parser.format_result(result))
```

### 方法4：解析包含 SQL 的脚本文件（.py / .sh）

```python
from sql_table_parser import SQLTableParser

parser = SQLTableParser()

# 解析包含 SQL 语句的 Python 脚本
result_py = parser.parse_file("sample_script.py")
print(parser.format_result(result_py))

# 解析包含 SQL 语句的 Shell 脚本
result_sh = parser.parse_file("sample_script.sh")
print(parser.format_result(result_sh))
```

> 提示：对 `.py` 和 `.sh` 文件，解析前会自动调用 `remove_non_sql_code` 预处理，移除 `import` / shebang / Shell 注释等非 SQL 代码，只保留真正的 SQL 片段再做解析。

## 核心类和方法

### SQLTableParser类

#### 主要方法

- **`parse_sql(sql: str, preprocess: bool = True)`**  
  解析SQL字符串，返回表名字典。默认会根据需要进行预处理：
  - `preprocess=True` 时，会调用 `remove_non_sql_code` 移除常见非SQL代码（适合直接传入脚本内容）。
  - 如果你确认传入的字符串是「纯 SQL」，可以设置 `preprocess=False`，略过额外处理。

- **`parse_file(file_path: str)`**  
  解析SQL或脚本文件，返回表名字典：
  - 支持扩展名：`.sql` / `.py` / `.sh`
  - 对 `.py` 和 `.sh` 文件会自动进行非SQL代码预处理

- **`format_result(result: dict)`** - 格式化解析结果为可读字符串

- **`clean_table_name(table_name: str)`** - 清理表名（去除引号等）

- **`is_valid_table_name(table_name: str)`** - 验证表名是否有效（过滤关键字、非法标识符等）

- **`remove_comments(sql: str)`** - 移除SQL注释（支持 `--` 单行注释和 `/* ... */` 多行注释）

- **`remove_non_sql_code(content: str)`** - 移除常见非SQL代码（例如 Python 的 `import` / `from ... import ...`、脚本 shebang、`#` 行注释等），为混合脚本场景提取 SQL 做准备

- **`extract_cte_names(sql: str)`** - 从 SQL 中提取所有 CTE 临时表名（`WITH xxx AS (...)`）

- **`extract_tables_from_pattern(sql: str, pattern: Pattern)`** - 使用指定正则表达式模式从 SQL 中提取表名，是各类语句解析的底层工具方法

#### 返回结果格式

```python
{
    'select_from': {'users', 'customers'},
    'join': {'orders', 'products'},
    'insert': {'logs'},
    'update': {'products'},
    # CTE 临时表（仅做记录，不会出现在 all_tables 中）
    'cte_tables': {'sales_summary'},
    # 汇总的真实表名（不包含 CTE 临时表）
    'all_tables': {'users', 'customers', 'orders', 'products', 'logs'}
}
```

## 示例输出

```
============================================================
SQL表名解析结果
============================================================

【所有表名】 (共 19 个)
  - customers
  - customers_2023
  - customers_2024
  - db_schema.table_name
  - departments
  - employees
  - error_table
  - logs
  - old_backup_table
  - order_details
  - orders
  - performance_reviews
  - products
  - raw_data
  - sales_records
  - temp_data
  - temporary_cache
  - user_logs
  - users

【按SQL语句类型分类】

SELECT FROM: 13 个表
  - customers
  - customers_2023
  - customers_2024
  - db_schema.table_name
  - departments
  - employees
  - error_table
  - performance_reviews
  - products
  - raw_data
  - sales_records
  - temp_data
  - users

JOIN: 3 个表
  - order_details
  - orders
  - products

INSERT INTO: 2 个表
  - logs
  - users

UPDATE: 1 个表
  - products

DELETE FROM: 1 个表
  - temp_data

CREATE TABLE: 1 个表
  - user_logs

DROP TABLE: 1 个表
  - old_backup_table

ALTER TABLE: 1 个表
  - users

TRUNCATE TABLE: 1 个表
  - temporary_cache

CTE临时表（已剔除）: 1 个表
  - sales_summary

============================================================
```

## 技术特点

1. **正则表达式匹配** - 使用精心设计的正则表达式模式匹配各种SQL语句
2. **注释处理** - 自动移除单行注释(`--`)和多行注释(`/* */`)
3. **关键字过滤** - 自动过滤SQL关键字，避免误识别
4. **多格式支持** - 支持带schema、引号等多种表名格式
5. **分类统计** - 按SQL语句类型分类统计表名

## 正则表达式说明

核心表名匹配模式：
```python
table_pattern = r'(?:`[^`]+`|"[^"]+"|[\w]+\.[\w]+|[\w]+)'
```

这个模式可以匹配：
- `` `table` `` - 反引号表名
- `"table"` - 双引号表名
- `schema.table` - 带schema的表名
- `table` - 普通表名

## 注意事项

1. 程序会自动过滤SQL关键字，避免将关键字识别为表名
2. 对于复杂的子查询和CTE，可能需要进一步优化正则表达式
3. 建议在使用前先用示例SQL测试，确保满足需求
4. 程序使用UTF-8编码读取文件，确保SQL文件编码正确

## 依赖要求

- Python 3.6+
- 标准库：re, pathlib, typing

无需安装第三方依赖包。

## 运行环境

- Windows / Linux / macOS
- Python 3.6 或更高版本

## 扩展建议

如需扩展功能，可以：
1. 添加更多SQL方言的支持（如Oracle、PostgreSQL特有语法）
2. 支持存储过程和函数中的表名提取
3. 添加表依赖关系分析
4. 支持输出为JSON、CSV等格式
5. 添加GUI界面

## 作者

高级Python工程师

## 许可

MIT License
