# SQL CTE 解析器

## 项目简介

这是一个使用正则表达式解析SQL代码中WITH语句（CTE - Common Table Expression）临时表名的Python程序。

## 文件说明

- **sample.sql**: 示例SQL文件，包含多种WITH语句场景
- **cte_parser.py**: CTE解析器主程序
- **README_CTE_PARSER.md**: 本说明文档

## 功能特性

✅ 支持单个CTE解析  
✅ 支持多个CTE解析  
✅ 支持递归CTE（WITH RECURSIVE）  
✅ 自动移除SQL注释（单行 `--` 和多行 `/* */`）  
✅ 提供详细的统计信息  
✅ 支持文件和字符串两种输入方式  

## 使用方法

### 1. 作为模块使用

```python
from cte_parser import CTEParser

# 创建解析器实例
parser = CTEParser()

# 解析SQL字符串
sql = """
WITH sales_data AS (
    SELECT product_id, SUM(amount) as total
    FROM sales
    GROUP BY product_id
)
SELECT * FROM sales_data;
"""

result = parser.analyze_sql(sql)
print(f"CTE表名: {result['cte_names']}")
print(f"总数: {result['total_count']}")

# 解析SQL文件
file_result = parser.parse_file('sample.sql')
print(f"找到的表名: {file_result['unique_names']}")
```

### 2. 直接运行

```bash
python cte_parser.py
```

程序会自动运行三个示例：
1. 解析SQL字符串
2. 解析sample.sql文件
3. 解析递归CTE

## API 文档

### CTEParser 类

#### 方法

**`extract_cte_names(sql: str) -> List[str]`**
- 提取所有CTE表名（保持出现顺序）
- 参数: SQL代码字符串
- 返回: 表名列表

**`extract_cte_names_unique(sql: str) -> Set[str]`**
- 提取所有唯一的CTE表名（去重）
- 参数: SQL代码字符串
- 返回: 表名集合

**`parse_file(file_path: str) -> Dict`**
- 解析SQL文件
- 参数: 文件路径
- 返回: 包含以下键的字典
  - `file`: 文件路径
  - `cte_names`: 所有表名列表
  - `unique_names`: 唯一表名列表（排序）
  - `total_count`: 总数量
  - `unique_count`: 唯一数量

**`analyze_sql(sql: str) -> Dict`**
- 分析SQL代码中的CTE使用情况
- 参数: SQL代码字符串
- 返回: 包含以下键的字典
  - `cte_names`: 所有表名列表
  - `unique_names`: 唯一表名列表（排序）
  - `total_count`: 总数量
  - `unique_count`: 唯一数量
  - `name_counts`: 每个表名出现次数
  - `has_recursive`: 是否包含递归CTE
  - `has_multiple_ctes`: 是否有多个CTE

## 正则表达式说明

### 主要正则模式

1. **WITH语句匹配**
```python
r'\bWITH\s+(?:RECURSIVE\s+)?(.*?)(?=\bSELECT\b|\bINSERT\b|\bUPDATE\b|\bDELETE\b)'
```
- 匹配 `WITH` 或 `WITH RECURSIVE`
- 捕获到主查询语句之前的所有内容

2. **CTE表名匹配**
```python
r'([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\('
```
- 匹配格式: `table_name AS (`
- 表名必须以字母或下划线开头
- 后续可包含字母、数字、下划线

## 示例输出

```
============================================================
示例1：解析SQL字符串
============================================================
找到的CTE表名: ['customer_summary', 'top_customers']
唯一表名: ['customer_summary', 'top_customers']
总数: 2, 唯一数: 2
是否包含递归CTE: False

============================================================
示例2：解析SQL文件
============================================================
文件: d:\gitee\wisdomduan\BigDataTool\sqlexp\sample.sql

所有CTE表名（按出现顺序）:
  1. customer_orders
  2. sales_data
  3. product_info
  4. top_products
  5. employee_hierarchy
  6. regional_sales
  7. top_regions
  8. active_users
  9. user_orders

唯一CTE表名（按字母排序）:
  - active_users
  - customer_orders
  - employee_hierarchy
  - product_info
  - regional_sales
  - sales_data
  - top_products
  - top_regions
  - user_orders

统计信息:
  总CTE数量: 9
  唯一CTE数量: 9
```

## 支持的SQL场景

### 1. 单个CTE
```sql
WITH customer_orders AS (
    SELECT customer_id, COUNT(*) as order_count
    FROM orders
    GROUP BY customer_id
)
SELECT * FROM customer_orders;
```

### 2. 多个CTE
```sql
WITH 
    sales_data AS (...),
    product_info AS (...),
    top_products AS (...)
SELECT * FROM top_products;
```

### 3. 递归CTE
```sql
WITH RECURSIVE employee_hierarchy AS (
    SELECT id, name, manager_id
    FROM employees
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.id, e.name, e.manager_id
    FROM employees e
    JOIN employee_hierarchy eh ON e.manager_id = eh.id
)
SELECT * FROM employee_hierarchy;
```

### 4. 带注释的CTE
```sql
WITH 
    -- 这是注释
    active_users AS (...),
    /* 多行注释 */
    user_orders AS (...)
SELECT * FROM active_users;
```

## 技术特点

- **纯Python实现**: 无需额外依赖
- **正则表达式驱动**: 高效的模式匹配
- **注释处理**: 自动过滤SQL注释
- **灵活的API**: 支持多种使用场景
- **详细的分析**: 提供丰富的统计信息

## 注意事项

1. 表名必须符合SQL标识符规范（字母、数字、下划线，不能以数字开头）
2. 程序会自动移除注释，避免误匹配
3. 支持大小写不敏感的匹配
4. 文件需使用UTF-8编码

## 扩展建议

如需扩展功能，可以考虑：
- 添加CTE依赖关系分析
- 支持更复杂的SQL方言
- 生成CTE关系图
- 添加语法验证功能

## 作者

高级Python工程师

## 许可

MIT License
