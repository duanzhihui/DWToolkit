#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL表名解析器
使用正则表达式从SQL代码中提取所有表名
支持多种SQL语句类型：SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER等
"""

import re
from typing import Set, List, Dict
from pathlib import Path


class SQLTableParser:
    """SQL表名解析器类"""
    
    def __init__(self):
        """初始化解析器，定义各种SQL语句的正则表达式模式"""
        
        # 表名模式：支持多种变量格式
        # - 普通表名: table, schema.table
        # - 引号表名: `table`, "table"
        # - Python变量: {var}, {schema}.{table}
        # - Shell变量: ${var}, $var, ${schema}.${table}
        IDENT = r'(?:`[^`]+`|"[^"]+"|\w+)'     # `..` 或 ".." 或 \w+
        PY_VAR = r'\{[A-Za-z_]\w*\}'            # Python: {var_name}
        SH_VAR = r'(?:\$\{[A-Za-z_]\w*\}|\$[A-Za-z_]\w*)'  # Shell: ${var} 或 $var
        PART = rf'(?:{IDENT}|{PY_VAR}|{SH_VAR})'  # 单段：标识符或变量
        self.table_pattern = rf'(?:{PART})(?:\.(?:{PART}))?'
        
        # 匹配单个CTE名称的正则表达式 (参考 cte_parser.py)
        self.single_cte_pattern = re.compile(
            r'([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\(',
            re.IGNORECASE
        )
        
        # 各种SQL语句的正则表达式模式
        self.patterns = {
            # SELECT ... FROM table
            'select_from': re.compile(
                r'\bFROM\s+' + self.table_pattern,
                re.IGNORECASE
            ),
            
            # JOIN table
            'join': re.compile(
                r'\b(?:INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|CROSS\s+JOIN|JOIN)\s+' + self.table_pattern,
                re.IGNORECASE
            ),
            
            # INSERT INTO table
            'insert': re.compile(
                r'\bINSERT\s+(?:INTO\s+(?:TABLE\s+)?|OVERWRITE\s+TABLE\s+)' + self.table_pattern,
                re.IGNORECASE
            ),
            
            # UPDATE table
            'update': re.compile(
                r'\bUPDATE\s+' + self.table_pattern,
                re.IGNORECASE
            ),
            
            # DELETE FROM table
            'delete': re.compile(
                r'\bDELETE\s+FROM\s+' + self.table_pattern,
                re.IGNORECASE
            ),
            
            # CREATE TABLE table (支持 Hive EXTERNAL TABLE)
            'create': re.compile(
                r'\bCREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?' + self.table_pattern,
                re.IGNORECASE
            ),
            
            # Hive: LOAD DATA [LOCAL] INPATH ... INTO TABLE table
            'load_data': re.compile(
                r'\bLOAD\s+DATA\s+(?:LOCAL\s+)?INPATH\s+[^\s]+\s+(?:OVERWRITE\s+)?INTO\s+TABLE\s+' + self.table_pattern,
                re.IGNORECASE
            ),
            
            # Hive: MSCK REPAIR TABLE table
            'msck_repair': re.compile(
                r'\bMSCK\s+(?:REPAIR\s+)?TABLE\s+' + self.table_pattern,
                re.IGNORECASE
            ),
            
            # Hive: EXPORT TABLE table
            'export_table': re.compile(
                r'\bEXPORT\s+TABLE\s+' + self.table_pattern,
                re.IGNORECASE
            ),
            
            # Hive: IMPORT TABLE table
            'import_table': re.compile(
                r'\bIMPORT\s+(?:EXTERNAL\s+)?TABLE\s+' + self.table_pattern,
                re.IGNORECASE
            ),
            
            # Hive: ANALYZE TABLE table
            'analyze': re.compile(
                r'\bANALYZE\s+TABLE\s+' + self.table_pattern,
                re.IGNORECASE
            ),
            
            # Hive: DESCRIBE [EXTENDED|FORMATTED] table
            'describe': re.compile(
                r'\b(?:DESCRIBE|DESC)\s+(?:EXTENDED\s+|FORMATTED\s+)?' + self.table_pattern,
                re.IGNORECASE
            ),
            
            # Hive: SHOW PARTITIONS table / SHOW CREATE TABLE table
            'show_table': re.compile(
                r'\bSHOW\s+(?:PARTITIONS|CREATE\s+TABLE|COLUMNS\s+(?:IN|FROM)|TBLPROPERTIES)\s+' + self.table_pattern,
                re.IGNORECASE
            ),
            
            # DROP TABLE table
            'drop': re.compile(
                r'\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?' + self.table_pattern,
                re.IGNORECASE
            ),
            
            # ALTER TABLE table
            'alter': re.compile(
                r'\bALTER\s+TABLE\s+' + self.table_pattern,
                re.IGNORECASE
            ),
            
            # TRUNCATE TABLE table
            'truncate': re.compile(
                r'\bTRUNCATE\s+(?:TABLE\s+)?' + self.table_pattern,
                re.IGNORECASE
            ),
            
            # WITH cte_name AS (...)
            'cte': re.compile(
                r'\bWITH\s+(\w+)\s+AS',
                re.IGNORECASE
            ),
        }
        
        # SQL关键字集合，用于过滤非表名
        self.sql_keywords = {
            'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 
            'CROSS', 'ON', 'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE',
            'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE', 'CREATE', 'DROP',
            'ALTER', 'TABLE', 'INDEX', 'VIEW', 'DATABASE', 'SCHEMA', 'AS', 'WITH',
            'UNION', 'INTERSECT', 'EXCEPT', 'ORDER', 'BY', 'GROUP', 'HAVING',
            'LIMIT', 'OFFSET', 'DISTINCT', 'ALL', 'ANY', 'SOME', 'CASE', 'WHEN',
            'THEN', 'ELSE', 'END', 'IF', 'NULL', 'IS', 'TRUE', 'FALSE', 'TRUNCATE',
            'ASC', 'DESC', 'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'CONSTRAINT',
            'UNIQUE', 'CHECK', 'DEFAULT', 'AUTO_INCREMENT', 'CASCADE',
            # Hive 常见关键字（避免被误识别为表名）
            'OVERWRITE', 'DIRECTORY', 'PARTITION', 'PARTITIONED', 'CLUSTERED',
            'DISTRIBUTED', 'SORTED', 'ROW', 'FORMAT', 'DELIMITED', 'FIELDS',
            'TERMINATED', 'COLLECTION', 'ITEMS', 'MAP', 'STORED', 'TEXTFILE',
            'SEQUENCEFILE', 'ORC', 'PARQUET', 'RCFILE', 'INPUTFORMAT', 'OUTPUTFORMAT',
            'EXTERNAL', 'LOCATION', 'TBLPROPERTIES', 'SERDEPROPERTIES', 'SERDE',
            'LOAD', 'DATA', 'LOCAL', 'INPATH', 'MSCK', 'REPAIR', 'EXPORT', 'IMPORT',
            'ANALYZE', 'COMPUTE', 'STATISTICS', 'DESCRIBE', 'DESC', 'EXTENDED',
            'FORMATTED', 'SHOW', 'PARTITIONS', 'COLUMNS', 'LATERAL', 'EXPLODE',
            'POSEXPLODE', 'INLINE', 'STACK', 'BUCKETS', 'SKEWED'
        }
    
    def clean_table_name(self, table_name: str) -> str:
        """
        清理表名，去除引号、反引号等
        
        Args:
            table_name: 原始表名
            
        Returns:
            清理后的表名
        """
        # 去除前后空格
        table_name = table_name.strip()
        
        # 如果是 schema.table 格式，分别清理每个部分
        if '.' in table_name:
            parts = table_name.split('.')
            cleaned_parts = [p.strip().strip('`"') for p in parts]
            return '.'.join(cleaned_parts)
        
        # 去除反引号和双引号
        table_name = table_name.strip('`"')
        
        return table_name
    
    def is_variable_name(self, name: str) -> bool:
        """
        检查是否为变量名格式
        
        Args:
            name: 名称
            
        Returns:
            是否为变量名
        """
        if not name:
            return False
        # Python变量: {var_name}
        if re.match(r'^\{[A-Za-z_]\w*\}$', name):
            return True
        # Shell变量: ${var_name} 或 $var_name
        if re.match(r'^\$\{[A-Za-z_]\w*\}$', name) or re.match(r'^\$[A-Za-z_]\w*$', name):
            return True
        return False
    
    def contains_variable(self, table_name: str) -> bool:
        """
        检查表名是否包含变量
        
        Args:
            table_name: 表名
            
        Returns:
            是否包含变量
        """
        # Python变量: {var}
        if re.search(r'\{[A-Za-z_]\w*\}', table_name):
            return True
        # Shell变量: ${var} 或 $var
        if re.search(r'\$\{?[A-Za-z_]\w*\}?', table_name):
            return True
        return False
    
    def extract_variables(self, table_name: str) -> List[Dict[str, str]]:
        """
        从表名中提取变量信息
        
        Args:
            table_name: 表名（可能包含变量）
            
        Returns:
            变量信息列表，每个元素包含 {"type": "python"|"shell", "name": "var_name", "full": "{var}"}
        """
        variables = []
        matched_positions = set()  # 记录已匹配的位置，避免重复
        
        # Shell变量: ${var_name} (优先匹配，因为包含 {})
        for match in re.finditer(r'\$\{([A-Za-z_]\w*)\}', table_name):
            variables.append({
                "type": "shell",
                "name": match.group(1),
                "full": match.group(0)
            })
            # 记录花括号内容的位置
            matched_positions.add((match.start() + 1, match.end() - 1))
        
        # Shell变量: $var_name (不带花括号)
        for match in re.finditer(r'\$([A-Za-z_]\w*)(?![\w{])', table_name):
            # 排除已经匹配的 ${var} 格式
            if f'${{{match.group(1)}}}' not in table_name:
                variables.append({
                    "type": "shell",
                    "name": match.group(1),
                    "full": match.group(0)
                })
        
        # Python变量: {var_name} (排除已被 ${var} 匹配的)
        for match in re.finditer(r'\{([A-Za-z_]\w*)\}', table_name):
            # 检查是否是 ${var} 的一部分
            if (match.start(), match.end()) not in matched_positions:
                # 检查前面是否有 $
                if match.start() == 0 or table_name[match.start() - 1] != '$':
                    variables.append({
                        "type": "python",
                        "name": match.group(1),
                        "full": match.group(0)
                    })
        
        return variables
    
    def replace_variables(self, table_name: str, var_values: Dict[str, str]) -> str:
        """
        替换表名中的变量为实际值
        
        Args:
            table_name: 包含变量的表名
            var_values: 变量名到值的映射，如 {"schema": "dw", "table": "users"}
            
        Returns:
            替换后的表名
        """
        result = table_name
        
        for var_name, var_value in var_values.items():
            # 替换 Shell 变量 ${var_name} (优先处理，避免被 $var 部分匹配)
            result = result.replace(f'${{{var_name}}}', var_value)
            # 替换 Shell 变量 $var_name (使用正则确保完整匹配变量名)
            result = re.sub(rf'\${re.escape(var_name)}(?![\w{{])', var_value, result)
            # 替换 Python 变量 {var_name}
            result = result.replace(f'{{{var_name}}}', var_value)
        
        return result
    
    def is_valid_table_name(self, table_name: str) -> bool:
        """
        验证是否为有效的表名（包括变量表名）
        
        Args:
            table_name: 表名
            
        Returns:
            是否有效
        """
        if not table_name:
            return False
        
        # 清理表名
        clean_name = self.clean_table_name(table_name)
        
        # 如果包含变量，认为是有效的
        if self.contains_variable(clean_name):
            return True
        
        # 如果是schema.table格式，只检查table部分
        if '.' in clean_name:
            parts = clean_name.split('.')
            clean_name = parts[-1].strip('`"')
        
        # 检查是否为SQL关键字
        if clean_name.upper() in self.sql_keywords:
            return False
        
        # 检查是否为有效的标识符（字母、数字、下划线）
        if not re.match(r'^[a-zA-Z_]\w*$', clean_name):
            return False
        
        return True
    
    def remove_comments(self, sql: str) -> str:
        """
        移除SQL中的注释
        
        Args:
            sql: SQL代码
            
        Returns:
            移除注释后的SQL
        """
        # 移除单行注释 --
        sql = re.sub(r'--[^\n]*', '', sql)
        
        # 移除多行注释 /* */
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        
        return sql
    
    def remove_non_sql_code(self, content: str) -> str:
        """
        移除非SQL代码（如Python import语句、shell命令等）
        
        Args:
            content: 文件内容
            
        Returns:
            移除非SQL代码后的内容
        """
        # 移除Python的import语句: import xxx 或 from xxx import yyy
        content = re.sub(r'^\s*import\s+[^\n]+', '', content, flags=re.MULTILINE)
        content = re.sub(r'^\s*from\s+[a-zA-Z_][a-zA-Z0-9_.]*\s+import\s+[^\n]+', '', content, flags=re.MULTILINE)
        
        # 移除Python的shebang和编码声明
        content = re.sub(r'^#!.*python[^\n]*', '', content, flags=re.MULTILINE)
        content = re.sub(r'^#.*coding[=:]\s*[^\n]+', '', content, flags=re.MULTILINE)
        
        # 移除shell的shebang
        content = re.sub(r'^#!/bin/(ba)?sh[^\n]*', '', content, flags=re.MULTILINE)
        content = re.sub(r'^#!/usr/bin/env\s+(ba)?sh[^\n]*', '', content, flags=re.MULTILINE)
        
        # 移除shell的纯注释行（不是SQL注释）
        # 保留 -- 开头的SQL注释，移除 # 开头的shell/python注释
        content = re.sub(r'^\s*#[^!][^\n]*', '', content, flags=re.MULTILINE)
        content = re.sub(r'^\s*#$', '', content, flags=re.MULTILINE)
        
        return content
    
    def extract_tables_from_pattern(self, sql: str, pattern: re.Pattern) -> Set[str]:
        """
        使用指定的正则表达式模式提取表名
        
        Args:
            sql: SQL代码
            pattern: 正则表达式模式
            
        Returns:
            表名集合
        """
        tables = set()
        matches = pattern.finditer(sql)
        
        for match in matches:
            # 获取匹配的完整文本
            matched_text = match.group(0)
            
            # 提取表名部分（去除关键字）
            # 分割匹配文本，取最后一个非空白部分
            parts = matched_text.split()
            if parts:
                table_name = parts[-1]
                
                # 清理并验证表名
                clean_name = self.clean_table_name(table_name)
                if self.is_valid_table_name(clean_name):
                    tables.add(clean_name)
        
        return tables
    
    def extract_cte_names(self, sql: str) -> Set[str]:
        """
        从SQL代码中提取所有CTE临时表名 (参考 cte_parser.py)
        
        Args:
            sql: SQL代码字符串
            
        Returns:
            CTE表名集合
        """
        cte_names = set()
        
        with_pattern = re.compile(r'\bWITH\s+(?:RECURSIVE\s+)?', re.IGNORECASE)
        
        for with_match in with_pattern.finditer(sql):
            pos = with_match.end()
            
            while pos < len(sql):
                while pos < len(sql) and sql[pos].isspace():
                    pos += 1
                
                if pos >= len(sql):
                    break
                
                name_match = re.match(r'([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\(', 
                                    sql[pos:], re.IGNORECASE)
                
                if not name_match:
                    break
                
                cte_names.add(name_match.group(1))
                pos += name_match.end()
                
                # 跳过括号内的内容
                paren_count = 1
                while pos < len(sql) and paren_count > 0:
                    if sql[pos] == '(':
                        paren_count += 1
                    elif sql[pos] == ')':
                        paren_count -= 1
                    pos += 1
                
                while pos < len(sql) and sql[pos].isspace():
                    pos += 1
                
                if pos < len(sql) and sql[pos] == ',':
                    pos += 1
                else:
                    break
        
        return cte_names
    
    def parse_sql(self, sql: str, preprocess: bool = True) -> Dict[str, Set[str]]:
        """
        解析SQL代码，提取所有表名（剔除CTE临时表）
        
        Args:
            sql: SQL代码
            preprocess: 是否预处理移除非SQL代码（如Python import）
            
        Returns:
            字典，键为SQL语句类型，值为表名集合
        """
        # 移除非SQL代码（如Python import语句）
        if preprocess:
            sql = self.remove_non_sql_code(sql)
        
        # 移除注释
        sql = self.remove_comments(sql)
        
        # 提取CTE临时表名
        cte_names = self.extract_cte_names(sql)
        
        result = {}
        all_tables = set()
        variable_tables = set()  # 包含变量的表名
        
        # 使用各种模式提取表名
        for pattern_name, pattern in self.patterns.items():
            tables = self.extract_tables_from_pattern(sql, pattern)
            if tables:
                # 剔除CTE临时表
                tables = tables - cte_names
                if tables:
                    result[pattern_name] = tables
                    all_tables.update(tables)
                    # 收集包含变量的表名
                    for table in tables:
                        if self.contains_variable(table):
                            variable_tables.add(table)
        
        # 添加所有表名的汇总
        result['all_tables'] = all_tables
        
        # 添加包含变量的表名
        if variable_tables:
            result['variable_tables'] = variable_tables
        
        # 添加CTE临时表信息（供参考）
        if cte_names:
            result['cte_tables'] = cte_names
        
        return result
    
    def parse_file(self, file_path: str) -> Dict[str, Set[str]]:
        """
        解析文件中的SQL代码，支持 *.sql, *.py, *.sh 文件
        
        Args:
            file_path: 文件路径（支持 .sql, .py, .sh 扩展名）
            
        Returns:
            字典，键为SQL语句类型，值为表名集合
        """
        path = Path(file_path)
        
        if not path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        # 检查文件扩展名
        suffix = path.suffix.lower()
        supported_extensions = {'.sql', '.py', '.sh'}
        if suffix not in supported_extensions:
            raise ValueError(f"不支持的文件类型: {suffix}，支持的类型: {supported_extensions}")
        
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 对于 .py 和 .sh 文件，需要预处理移除非SQL代码
        preprocess = suffix in {'.py', '.sh'}
        
        return self.parse_sql(content, preprocess=preprocess)
    
    def format_result(self, result: Dict[str, Set[str]]) -> str:
        """
        格式化解析结果为可读字符串
        
        Args:
            result: 解析结果
            
        Returns:
            格式化后的字符串
        """
        lines = []
        lines.append("=" * 60)
        lines.append("SQL表名解析结果")
        lines.append("=" * 60)
        
        # 显示所有表名
        if 'all_tables' in result:
            lines.append(f"\n【所有表名】 (共 {len(result['all_tables'])} 个)")
            for table in sorted(result['all_tables']):
                lines.append(f"  - {table}")
        
        # 按语句类型显示
        lines.append("\n【按SQL语句类型分类】")
        
        type_names = {
            'select_from': 'SELECT FROM',
            'join': 'JOIN',
            'insert': 'INSERT INTO/OVERWRITE',
            'update': 'UPDATE',
            'delete': 'DELETE FROM',
            'create': 'CREATE TABLE',
            'drop': 'DROP TABLE',
            'alter': 'ALTER TABLE',
            'truncate': 'TRUNCATE TABLE',
            'cte': 'WITH (CTE)',
            'cte_tables': 'CTE临时表（已剔除）',
            'variable_tables': '变量表名',
            # Hive 特有语句类型
            'load_data': 'LOAD DATA (Hive)',
            'msck_repair': 'MSCK REPAIR TABLE (Hive)',
            'export_table': 'EXPORT TABLE (Hive)',
            'import_table': 'IMPORT TABLE (Hive)',
            'analyze': 'ANALYZE TABLE (Hive)',
            'describe': 'DESCRIBE (Hive)',
            'show_table': 'SHOW (Hive)',
        }
        
        for pattern_name, tables in result.items():
            if pattern_name != 'all_tables' and tables:
                type_name = type_names.get(pattern_name, pattern_name)
                lines.append(f"\n{type_name}: {len(tables)} 个表")
                for table in sorted(tables):
                    lines.append(f"  - {table}")
        
        lines.append("\n" + "=" * 60)
        
        return "\n".join(lines)


def main():
    """主函数，演示如何使用SQL表名解析器"""
    
    # 创建解析器实例
    parser = SQLTableParser()
    
    # 示例1：解析SQL字符串
    print("示例1：解析SQL字符串")
    print("-" * 60)
    
    sample_sql = """
    WITH customer_summary AS (
        SELECT customer_id, SUM(amount) as total
        FROM orders
        GROUP BY customer_id
    ),
    top_customers AS (
        SELECT customer_id
        FROM customer_summary
        WHERE total > 1000
    )
    SELECT u.id, u.name, tc.customer_id
    FROM users u
    INNER JOIN top_customers tc ON u.id = tc.customer_id;
    
    INSERT INTO logs (message, created_at) VALUES ('test', NOW());
    
    UPDATE products SET price = price * 1.1 WHERE category = 'electronics';
    """
    
    result = parser.parse_sql(sample_sql)
    print(parser.format_result(result))
    
    # 示例2：解析SQL文件
    print("\n\n示例2：解析SQL文件")
    print("-" * 60)
    
    #sql_file = "sample_queries.sql"
    sql_file = "sample_script.py"
    
    try:
        result = parser.parse_file(sql_file)
        print(parser.format_result(result))
        
        # 保存结果到文件
        output_file = "parse_result.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(parser.format_result(result))
        print(f"\n解析结果已保存到: {output_file}")
        
    except FileNotFoundError as e:
        print(f"错误: {e}")
        print(f"请确保 {sql_file} 文件存在于当前目录")


if __name__ == "__main__":
    main()
