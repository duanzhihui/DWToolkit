#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL CTE (Common Table Expression) 解析器
使用正则表达式提取 WITH 语句中的临时表名
"""

import re
from typing import List, Set, Dict
from pathlib import Path


class CTEParser:
    """SQL WITH语句解析器"""
    
    def __init__(self):
        # 匹配 WITH 语句的正则表达式
        # 支持 WITH 和 WITH RECURSIVE
        # 匹配格式: WITH [RECURSIVE] table_name AS (...)
        self.cte_pattern = re.compile(
            r'\bWITH\s+(?:RECURSIVE\s+)?'  # WITH 或 WITH RECURSIVE
            r'(.*?)'                        # 捕获所有CTE定义
            r'(?=\bSELECT\b|\bINSERT\b|\bUPDATE\b|\bDELETE\b)',  # 直到主查询开始
            re.IGNORECASE | re.DOTALL
        )
        
        # 匹配单个CTE名称的正则表达式
        # 格式: table_name AS (...)
        self.single_cte_pattern = re.compile(
            r'([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\(',
            re.IGNORECASE
        )
        
    def remove_comments(self, sql: str) -> str:
        """
        移除SQL注释
        
        Args:
            sql: SQL代码字符串
            
        Returns:
            移除注释后的SQL代码
        """
        # 移除单行注释 --
        sql = re.sub(r'--[^\n]*', '', sql)
        # 移除多行注释 /* */
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        return sql
    
    def extract_cte_names(self, sql: str) -> List[str]:
        """
        从SQL代码中提取所有CTE临时表名
        
        Args:
            sql: SQL代码字符串
            
        Returns:
            CTE表名列表（保持出现顺序）
        """
        clean_sql = self.remove_comments(sql)
        cte_names = []
        
        with_pattern = re.compile(r'\bWITH\s+(?:RECURSIVE\s+)?', re.IGNORECASE)
        
        for with_match in with_pattern.finditer(clean_sql):
            pos = with_match.end()
            
            while pos < len(clean_sql):
                while pos < len(clean_sql) and clean_sql[pos].isspace():
                    pos += 1
                
                if pos >= len(clean_sql):
                    break
                
                name_match = re.match(r'([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\(', 
                                    clean_sql[pos:], re.IGNORECASE)
                
                if not name_match:
                    break
                
                cte_names.append(name_match.group(1))
                pos += name_match.end()
                
                paren_count = 1
                while pos < len(clean_sql) and paren_count > 0:
                    if clean_sql[pos] == '(':
                        paren_count += 1
                    elif clean_sql[pos] == ')':
                        paren_count -= 1
                    pos += 1
                
                while pos < len(clean_sql) and clean_sql[pos].isspace():
                    pos += 1
                
                if pos < len(clean_sql) and clean_sql[pos] == ',':
                    pos += 1
                else:
                    break
        
        return cte_names
    
    def extract_cte_names_unique(self, sql: str) -> Set[str]:
        """
        从SQL代码中提取所有唯一的CTE临时表名
        
        Args:
            sql: SQL代码字符串
            
        Returns:
            CTE表名集合（去重）
        """
        return set(self.extract_cte_names(sql))
    
    def parse_file(self, file_path: str) -> Dict[str, List[str]]:
        """
        解析SQL文件，提取所有WITH语句中的临时表名
        
        Args:
            file_path: SQL文件路径
            
        Returns:
            包含统计信息的字典
        """
        path = Path(file_path)
        
        if not path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        # 读取文件内容
        with open(path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # 提取CTE名称
        cte_names = self.extract_cte_names(sql_content)
        unique_names = self.extract_cte_names_unique(sql_content)
        
        return {
            'file': str(path),
            'cte_names': cte_names,
            'unique_names': sorted(unique_names),
            'total_count': len(cte_names),
            'unique_count': len(unique_names)
        }
    
    def analyze_sql(self, sql: str) -> Dict[str, any]:
        """
        分析SQL代码中的CTE使用情况
        
        Args:
            sql: SQL代码字符串
            
        Returns:
            分析结果字典
        """
        cte_names = self.extract_cte_names(sql)
        unique_names = self.extract_cte_names_unique(sql)
        
        # 统计每个CTE出现的次数
        name_counts = {}
        for name in cte_names:
            name_counts[name] = name_counts.get(name, 0) + 1
        
        # 检测是否有递归CTE
        has_recursive = bool(re.search(r'\bWITH\s+RECURSIVE\b', sql, re.IGNORECASE))
        
        return {
            'cte_names': cte_names,
            'unique_names': sorted(unique_names),
            'total_count': len(cte_names),
            'unique_count': len(unique_names),
            'name_counts': name_counts,
            'has_recursive': has_recursive,
            'has_multiple_ctes': len(unique_names) > 1
        }


def main():
    """主函数：演示解析器的使用"""
    
    parser = CTEParser()
    
    # 示例1：解析字符串
    print("=" * 60)
    print("示例1：解析SQL字符串")
    print("=" * 60)
    
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
    SELECT * FROM top_customers;
    """
    
    result = parser.analyze_sql(sample_sql)
    print(f"找到的CTE表名: {result['cte_names']}")
    print(f"唯一表名: {result['unique_names']}")
    print(f"总数: {result['total_count']}, 唯一数: {result['unique_count']}")
    print(f"是否包含递归CTE: {result['has_recursive']}")
    print()
    
    # 示例2：解析文件
    print("=" * 60)
    print("示例2：解析SQL文件")
    print("=" * 60)
    
    sql_file = "./cte_parser/sample.sql"
    
    try:
        file_result = parser.parse_file(sql_file)
        print(f"文件: {file_result['file']}")
        print(f"\n所有CTE表名（按出现顺序）:")
        for i, name in enumerate(file_result['cte_names'], 1):
            print(f"  {i}. {name}")
        
        print(f"\n唯一CTE表名（按字母排序）:")
        for name in file_result['unique_names']:
            print(f"  - {name}")
        
        print(f"\n统计信息:")
        print(f"  总CTE数量: {file_result['total_count']}")
        print(f"  唯一CTE数量: {file_result['unique_count']}")
        
    except FileNotFoundError as e:
        print(f"错误: {e}")
        print(f"请确保 {sql_file} 文件存在于当前目录")
    
    print()
    
    # 示例3：测试递归CTE
    print("=" * 60)
    print("示例3：解析递归CTE")
    print("=" * 60)
    
    recursive_sql = """
    WITH RECURSIVE employee_tree AS (
        SELECT id, name, manager_id
        FROM employees
        WHERE manager_id IS NULL
        UNION ALL
        SELECT e.id, e.name, e.manager_id
        FROM employees e
        JOIN employee_tree et ON e.manager_id = et.id
    )
    SELECT * FROM employee_tree;
    """
    
    recursive_result = parser.analyze_sql(recursive_sql)
    print(f"CTE表名: {recursive_result['cte_names']}")
    print(f"是否递归: {recursive_result['has_recursive']}")
    print()


if __name__ == "__main__":
    main()
