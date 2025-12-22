#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL代码提取模块
从 *.py *.sh *.hql *.sql 等代码文件中提取SQL语句
支持 Hive SQL, Spark SQL, Impala SQL

用法:
    # 解析目录
    python sql_parser.py -i <输入目录> -o <输出目录>
    
    # 解析单个文件
    python sql_parser.py -f <输入文件> -o <输出目录>
"""

import re
import os
import json
import argparse
import fnmatch
import yaml
import shutil
from typing import List, Dict, Optional, Tuple, Any, Set
from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum


def extract_extensions_from_patterns(patterns: List[str]) -> List[str]:
    """
    从include_patterns中提取文件扩展名
    
    Args:
        patterns: 包含规则列表，例如 ["*.py", "*.sh", "src/**/*.sql"]
        
    Returns:
        扩展名列表，例如 [".py", ".sh", ".sql"]
    """
    extensions: Set[str] = set()
    default_extensions = ['.py', '.sh', '.sql', '.hql']
    
    if not patterns:
        return default_extensions
    
    for pattern in patterns:
        # 提取模式中的扩展名
        # 匹配如 *.py, **/*.sql, src/*.sh 等模式
        if '*.' in pattern:
            # 找到最后一个 *. 后面的部分作为扩展名
            parts = pattern.split('*.')
            if len(parts) > 1:
                ext_part = parts[-1]
                # 处理可能的路径分隔符
                ext = ext_part.split('/')[0].split('\\')[0]
                if ext and not any(c in ext for c in '*?[]'):
                    extensions.add('.' + ext)
    
    return list(extensions) if extensions else default_extensions


class SQLType(Enum):
    """SQL类型枚举"""
    HIVE = "hive"
    SPARK = "spark"
    IMPALA = "impala"
    GENERIC = "generic"


@dataclass
class ExtractedSQL:
    """提取的SQL信息"""
    sql: str                          # SQL语句
    source_file: str                  # 来源文件
    line_start: int                   # 起始行号
    line_end: int                     # 结束行号
    sql_type: SQLType = SQLType.GENERIC  # SQL类型
    context: str = ""                 # 上下文信息（变量名等）
    
    def __str__(self) -> str:
        return f"[{self.source_file}:{self.line_start}-{self.line_end}] {self.sql[:50]}..."


class SQLExtractor:
    """SQL代码提取器"""
    
    # SQL语句起始关键字
    SQL_START_KEYWORDS = [
        'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER',
        'TRUNCATE', 'MERGE', 'WITH', 'EXPLAIN', 'DESCRIBE', 'DESC', 'SHOW',
        'USE', 'SET', 'LOAD', 'EXPORT', 'IMPORT', 'MSCK', 'ANALYZE',
        'GRANT', 'REVOKE', 'REFRESH', 'INVALIDATE', 'COMPUTE'
    ]
    
    # Hive/Spark/Impala 特有关键字
    HIVE_KEYWORDS = ['MSCK', 'LOAD DATA', 'EXPORT TABLE', 'IMPORT TABLE', 
                     'CLUSTERED BY', 'PARTITIONED BY', 'STORED AS']
    SPARK_KEYWORDS = ['CACHE TABLE', 'UNCACHE TABLE', 'CLEAR CACHE', 
                      'REFRESH TABLE', 'ADD JAR', 'ADD FILE']
    IMPALA_KEYWORDS = ['INVALIDATE METADATA', 'REFRESH', 'COMPUTE STATS',
                       'COMPUTE INCREMENTAL STATS']
    
    def __init__(self):
        """初始化提取器"""
        # Python字符串中的SQL模式
        self.python_patterns = [
            # 三引号字符串 (""" 或 ''')
            re.compile(r'(?:sql|query|hql|statement|cmd)\s*=\s*"""(.*?)"""', re.DOTALL | re.IGNORECASE),
            re.compile(r"(?:sql|query|hql|statement|cmd)\s*=\s*'''(.*?)'''", re.DOTALL | re.IGNORECASE),
            # 普通字符串拼接
            re.compile(r'(?:sql|query|hql|statement|cmd)\s*=\s*"([^"\\]*(?:\\.[^"\\]*)*)"', re.IGNORECASE),
            re.compile(r"(?:sql|query|hql|statement|cmd)\s*=\s*'([^'\\]*(?:\\.[^'\\]*)*)'", re.IGNORECASE),
            # f-string
            re.compile(r'(?:sql|query|hql|statement|cmd)\s*=\s*f"""(.*?)"""', re.DOTALL | re.IGNORECASE),
            re.compile(r"(?:sql|query|hql|statement|cmd)\s*=\s*f'''(.*?)'''", re.DOTALL | re.IGNORECASE),
            # spark.sql() / hive.execute() 等方法调用
            re.compile(r'\.(?:sql|execute|executeQuery|executeUpdate)\s*\(\s*"""(.*?)"""', re.DOTALL),
            re.compile(r"\.(?:sql|execute|executeQuery|executeUpdate)\s*\(\s*'''(.*?)'''", re.DOTALL),
            re.compile(r'\.(?:sql|execute|executeQuery|executeUpdate)\s*\(\s*"([^"\\]*(?:\\.[^"\\]*)*)"', re.DOTALL),
            re.compile(r"\.(?:sql|execute|executeQuery|executeUpdate)\s*\(\s*'([^'\\]*(?:\\.[^'\\]*)*)'", re.DOTALL),
            # cursor.execute()
            re.compile(r'cursor\.execute\s*\(\s*"""(.*?)"""', re.DOTALL),
            re.compile(r"cursor\.execute\s*\(\s*'''(.*?)'''", re.DOTALL),
        ]
        
        # Shell脚本中的SQL模式
        self.shell_patterns = [
            # hive -e "SQL"
            re.compile(r'hive\s+-e\s+"(.*?)"', re.DOTALL),
            re.compile(r"hive\s+-e\s+'(.*?)'", re.DOTALL),
            # beeline -e "SQL"
            re.compile(r'beeline\s+.*?-e\s+"(.*?)"', re.DOTALL),
            re.compile(r"beeline\s+.*?-e\s+'(.*?)'", re.DOTALL),
            # impala-shell -q "SQL"
            re.compile(r'impala-shell\s+.*?-q\s+"(.*?)"', re.DOTALL),
            re.compile(r"impala-shell\s+.*?-q\s+'(.*?)'", re.DOTALL),
            # spark-sql -e "SQL"
            re.compile(r'spark-sql\s+.*?-e\s+"(.*?)"', re.DOTALL),
            re.compile(r"spark-sql\s+.*?-e\s+'(.*?)'", re.DOTALL),
            re.compile(r'(?:hive|beeline|impala-shell|spark-sql)\s*<<\s*[\'"]?(\w+)[\'"]?\s*\n(.*?)\n\1', re.DOTALL),
            # heredoc: hive <<- EOF ... EOF (带缩进)
            re.compile(r'(?:hive|beeline|impala-shell|spark-sql)\s*<<-\s*[\'"]?(\w+)[\'"]?\s*\n(.*?)\n\s*\1', re.DOTALL),
            # 变量赋值: SQL="..."
            re.compile(r'(?:SQL|HQL|QUERY)\s*=\s*"(.*?)"', re.DOTALL | re.IGNORECASE),
            re.compile(r"(?:SQL|HQL|QUERY)\s*=\s*'(.*?)'", re.DOTALL | re.IGNORECASE),
        ]
        
        # SQL文件中的语句分隔模式
        self.sql_statement_pattern = re.compile(
            r'(?:^|\s)(' + '|'.join(self.SQL_START_KEYWORDS) + r')\b',
            re.IGNORECASE | re.MULTILINE
        )
    
    def detect_sql_type(self, sql: str) -> SQLType:
        """
        检测SQL类型（Hive/Spark/Impala）
        
        Args:
            sql: SQL语句
            
        Returns:
            SQL类型
        """
        sql_upper = sql.upper()
        
        for keyword in self.IMPALA_KEYWORDS:
            if keyword in sql_upper:
                return SQLType.IMPALA
        
        for keyword in self.SPARK_KEYWORDS:
            if keyword in sql_upper:
                return SQLType.SPARK
        
        for keyword in self.HIVE_KEYWORDS:
            if keyword in sql_upper:
                return SQLType.HIVE
        
        return SQLType.GENERIC
    
    def clean_sql(self, sql: str) -> str:
        """
        清理SQL语句
        
        Args:
            sql: 原始SQL
            
        Returns:
            清理后的SQL
        """
        # 去除首尾空白
        sql = sql.strip()
        
        # 处理转义字符
        sql = sql.replace('\\n', '\n')
        sql = sql.replace('\\t', '\t')
        sql = sql.replace('\\"', '"')
        sql = sql.replace("\\'", "'")
        
        # 规范化空白字符
        sql = re.sub(r'[ \t]+', ' ', sql)
        sql = re.sub(r'\n\s*\n', '\n', sql)
        
        return sql.strip()
    
    def is_valid_sql(self, sql: str) -> bool:
        """
        验证是否为有效的SQL语句
        
        Args:
            sql: SQL语句
            
        Returns:
            是否有效
        """
        if not sql or len(sql.strip()) < 10:
            return False
        
        sql_upper = sql.strip().upper()
        
        # 检查是否以SQL关键字开头
        for keyword in self.SQL_START_KEYWORDS:
            if sql_upper.startswith(keyword):
                return True
        
        # 检查是否包含SQL关键字
        if any(keyword in sql_upper for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE']):
            return True
        
        return False
    
    def get_line_number(self, content: str, position: int) -> int:
        """
        根据字符位置获取行号
        
        Args:
            content: 文件内容
            position: 字符位置
            
        Returns:
            行号（从1开始）
        """
        return content[:position].count('\n') + 1
    
    def extract_from_python(self, content: str, file_path: str) -> List[ExtractedSQL]:
        """
        从Python文件中提取SQL
        
        Args:
            content: 文件内容
            file_path: 文件路径
            
        Returns:
            提取的SQL列表
        """
        results = []
        
        for pattern in self.python_patterns:
            for match in pattern.finditer(content):
                sql = match.group(1) if match.lastindex >= 1 else match.group(0)
                sql = self.clean_sql(sql)
                
                if self.is_valid_sql(sql):
                    line_start = self.get_line_number(content, match.start())
                    line_end = self.get_line_number(content, match.end())
                    sql_type = self.detect_sql_type(sql)
                    
                    # 获取上下文（变量名）
                    context_match = re.search(r'(\w+)\s*=', content[max(0, match.start()-50):match.start()])
                    context = context_match.group(1) if context_match else ""
                    
                    results.append(ExtractedSQL(
                        sql=sql,
                        source_file=file_path,
                        line_start=line_start,
                        line_end=line_end,
                        sql_type=sql_type,
                        context=context
                    ))
        
        # 提取通用的多行字符串中的SQL
        multiline_patterns = [
            re.compile(r'"""(.*?)"""', re.DOTALL),
            re.compile(r"'''(.*?)'''", re.DOTALL),
        ]
        
        for pattern in multiline_patterns:
            for match in pattern.finditer(content):
                sql = match.group(1)
                sql = self.clean_sql(sql)
                
                if self.is_valid_sql(sql) and not any(sql == r.sql for r in results):
                    line_start = self.get_line_number(content, match.start())
                    line_end = self.get_line_number(content, match.end())
                    sql_type = self.detect_sql_type(sql)
                    
                    results.append(ExtractedSQL(
                        sql=sql,
                        source_file=file_path,
                        line_start=line_start,
                        line_end=line_end,
                        sql_type=sql_type
                    ))
        
        return results
    
    def extract_from_shell(self, content: str, file_path: str) -> List[ExtractedSQL]:
        """
        从Shell脚本中提取SQL
        
        Args:
            content: 文件内容
            file_path: 文件路径
            
        Returns:
            提取的SQL列表
        """
        results = []
        
        for pattern in self.shell_patterns:
            for match in pattern.finditer(content):
                # heredoc模式有两个捕获组
                if match.lastindex >= 2:
                    sql = match.group(2)
                else:
                    sql = match.group(1)
                
                sql = self.clean_sql(sql)
                
                if self.is_valid_sql(sql):
                    line_start = self.get_line_number(content, match.start())
                    line_end = self.get_line_number(content, match.end())
                    sql_type = self.detect_sql_type(sql)
                    
                    results.append(ExtractedSQL(
                        sql=sql,
                        source_file=file_path,
                        line_start=line_start,
                        line_end=line_end,
                        sql_type=sql_type
                    ))
        
        return results
    
    def extract_from_sql_file(self, content: str, file_path: str) -> List[ExtractedSQL]:
        """
        从SQL/HQL文件中提取SQL语句
        
        Args:
            content: 文件内容
            file_path: 文件路径
            
        Returns:
            提取的SQL列表
        """
        results = []
        
        # 移除注释
        content_no_comments = self._remove_comments(content)
        
        # 按分号分割语句
        statements = self._split_statements(content_no_comments)
        
        current_pos = 0
        for stmt in statements:
            stmt = stmt.strip()
            if self.is_valid_sql(stmt):
                # 查找语句在原始内容中的位置
                stmt_start = content.find(stmt[:min(50, len(stmt))], current_pos)
                if stmt_start == -1:
                    stmt_start = current_pos
                
                line_start = self.get_line_number(content, stmt_start)
                line_end = line_start + stmt.count('\n')
                sql_type = self.detect_sql_type(stmt)
                
                results.append(ExtractedSQL(
                    sql=stmt,
                    source_file=file_path,
                    line_start=line_start,
                    line_end=line_end,
                    sql_type=sql_type
                ))
                
                current_pos = stmt_start + len(stmt)
        
        return results
    
    def _remove_comments(self, sql: str) -> str:
        """移除SQL注释"""
        # 移除单行注释 --
        sql = re.sub(r'--[^\n]*', '', sql)
        # 移除多行注释 /* */
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        return sql
    
    def _split_statements(self, sql: str) -> List[str]:
        """按分号分割SQL语句，考虑字符串内的分号"""
        statements = []
        current = []
        in_string = False
        string_char = None
        
        i = 0
        while i < len(sql):
            char = sql[i]
            
            if char in ('"', "'") and (i == 0 or sql[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None
            
            if char == ';' and not in_string:
                stmt = ''.join(current).strip()
                if stmt:
                    statements.append(stmt)
                current = []
            else:
                current.append(char)
            
            i += 1
        
        # 处理最后一个语句（可能没有分号）
        stmt = ''.join(current).strip()
        if stmt:
            statements.append(stmt)
        
        return statements
    
    def extract_from_file(self, file_path: str) -> List[ExtractedSQL]:
        """
        从文件中提取SQL
        
        Args:
            file_path: 文件路径
            
        Returns:
            提取的SQL列表
        """
        path = Path(file_path)
        
        if not path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        # 读取文件内容
        try:
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
        except UnicodeDecodeError:
            with open(path, 'r', encoding='gbk') as f:
                content = f.read()
        
        suffix = path.suffix.lower()
        
        if suffix == '.py':
            return self.extract_from_python(content, file_path)
        elif suffix == '.sh':
            return self.extract_from_shell(content, file_path)
        elif suffix in ('.sql', '.hql'):
            return self.extract_from_sql_file(content, file_path)
        else:
            # 尝试通用提取
            results = []
            results.extend(self.extract_from_python(content, file_path))
            results.extend(self.extract_from_shell(content, file_path))
            if not results:
                results.extend(self.extract_from_sql_file(content, file_path))
            return results
    
    def extract_from_directory(self, dir_path: str, 
                                extensions: List[str] = None,
                                recursive: bool = True,
                                include_patterns: List[str] = None,
                                exclude_patterns: List[str] = None) -> List[ExtractedSQL]:
        """
        从目录中提取SQL
        
        Args:
            dir_path: 目录路径
            extensions: 文件扩展名列表，默认 ['.py', '.sh', '.sql', '.hql']
            recursive: 是否递归搜索子目录
            include_patterns: 包含规则（Unix通配符）
            exclude_patterns: 排除规则（Unix通配符）
            
        Returns:
            提取的SQL列表
        """
        if extensions is None:
            extensions = ['.py', '.sh', '.sql', '.hql']
        
        results = []
        path = Path(dir_path)
        
        if not path.exists():
            raise FileNotFoundError(f"目录不存在: {dir_path}")
        
        pattern = '**/*' if recursive else '*'
        
        for ext in extensions:
            for file_path in path.glob(pattern + ext):
                # 应用过滤规则
                if not self._should_process_file(file_path, path, include_patterns, exclude_patterns):
                    continue
                try:
                    file_results = self.extract_from_file(str(file_path))
                    results.extend(file_results)
                except Exception as e:
                    print(f"警告: 处理文件 {file_path} 时出错: {e}")
        
        return results
    
    def _should_process_file(self, file_path: Path, base_path: Path,
                              include_patterns: List[str] = None,
                              exclude_patterns: List[str] = None) -> bool:
        """
        判断文件是否应该被处理（基于包含/排除规则）
        
        Args:
            file_path: 文件路径
            base_path: 基础目录路径
            include_patterns: 包含规则
            exclude_patterns: 排除规则
            
        Returns:
            是否应该处理
        """
        try:
            relative_path = str(file_path.relative_to(base_path))
        except ValueError:
            relative_path = file_path.name
        
        # 将路径统一为Unix风格（用于匹配）
        relative_path_unix = relative_path.replace('\\', '/')
        file_name = file_path.name
        
        # 检查排除规则
        if exclude_patterns:
            for pattern in exclude_patterns:
                # 匹配文件名或相对路径
                if fnmatch.fnmatch(file_name, pattern) or fnmatch.fnmatch(relative_path_unix, pattern):
                    return False
                # 检查路径中的任何部分是否匹配（用于目录排除）
                for part in file_path.parts:
                    if fnmatch.fnmatch(part, pattern):
                        return False
        
        # 检查包含规则（如果指定了包含规则，则只处理匹配的文件）
        if include_patterns:
            for pattern in include_patterns:
                if fnmatch.fnmatch(file_name, pattern) or fnmatch.fnmatch(relative_path_unix, pattern):
                    return True
            return False
        
        return True


class SQLParser:
    """SQL解析器 - 主入口类"""
    
    def __init__(self):
        """初始化解析器"""
        self.extractor = SQLExtractor()
    
    def parse_file(self, file_path: str) -> List[ExtractedSQL]:
        """
        解析单个文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            提取的SQL列表
        """
        return self.extractor.extract_from_file(file_path)
    
    def parse_directory(self, dir_path: str, 
                        extensions: List[str] = None,
                        recursive: bool = True,
                        include_patterns: List[str] = None,
                        exclude_patterns: List[str] = None) -> List[ExtractedSQL]:
        """
        解析目录
        
        Args:
            dir_path: 目录路径
            extensions: 文件扩展名列表
            recursive: 是否递归
            include_patterns: 包含规则（Unix通配符）
            exclude_patterns: 排除规则（Unix通配符）
            
        Returns:
            提取的SQL列表
        """
        return self.extractor.extract_from_directory(
            dir_path, extensions, recursive, include_patterns, exclude_patterns
        )
    
    def parse_string(self, content: str, file_type: str = 'sql') -> List[ExtractedSQL]:
        """
        解析字符串内容
        
        Args:
            content: 内容字符串
            file_type: 文件类型 ('py', 'sh', 'sql', 'hql')
            
        Returns:
            提取的SQL列表
        """
        fake_path = f"<string>.{file_type}"
        
        if file_type == 'py':
            return self.extractor.extract_from_python(content, fake_path)
        elif file_type == 'sh':
            return self.extractor.extract_from_shell(content, fake_path)
        else:
            return self.extractor.extract_from_sql_file(content, fake_path)
    
    def write_to_file(self, sqls: List[ExtractedSQL], output_path: str, 
                      format: str = 'sql') -> None:
        """
        将提取的SQL写入文件
        
        Args:
            sqls: SQL列表
            output_path: 输出文件路径
            format: 输出格式 ('sql', 'json', 'txt')
        """
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        # 按源文件中的顺序排序（line_start升序）
        sqls = sorted(sqls, key=lambda x: x.line_start)
        
        if format == 'json':
            import json
            data = [
                {
                    'sql': sql.sql,
                    'source_file': sql.source_file,
                    'line_start': sql.line_start,
                    'line_end': sql.line_end,
                    'sql_type': sql.sql_type.value,
                    'context': sql.context
                }
                for sql in sqls
            ]
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        
        elif format == 'txt':
            with open(path, 'w', encoding='utf-8') as f:
                for i, sql in enumerate(sqls, 1):
                    f.write(f"{'='*60}\n")
                    f.write(f"SQL #{i}\n")
                    f.write(f"来源: {sql.source_file}:{sql.line_start}-{sql.line_end}\n")
                    f.write(f"类型: {sql.sql_type.value}\n")
                    if sql.context:
                        f.write(f"上下文: {sql.context}\n")
                    f.write(f"{'='*60}\n")
                    f.write(f"{sql.sql}\n\n")
        
        else:  # sql format
            with open(path, 'w', encoding='utf-8') as f:
                for i, sql in enumerate(sqls, 1):
                    f.write(f"-- SQL #{i} from {sql.source_file}:{sql.line_start}-{sql.line_end}\n")
                    f.write(f"-- Type: {sql.sql_type.value}\n")
                    f.write(f"{sql.sql};\n\n")
    
    def format_result(self, sqls: List[ExtractedSQL]) -> str:
        """
        格式化结果为可读字符串
        
        Args:
            sqls: SQL列表
            
        Returns:
            格式化后的字符串
        """
        lines = []
        lines.append("=" * 60)
        lines.append(f"SQL提取结果 (共 {len(sqls)} 条)")
        lines.append("=" * 60)
        
        # 按类型统计
        type_count = {}
        for sql in sqls:
            type_name = sql.sql_type.value
            type_count[type_name] = type_count.get(type_name, 0) + 1
        
        lines.append("\n【按类型统计】")
        for type_name, count in sorted(type_count.items()):
            lines.append(f"  - {type_name}: {count} 条")
        
        # 按文件统计
        file_count = {}
        for sql in sqls:
            file_count[sql.source_file] = file_count.get(sql.source_file, 0) + 1
        
        lines.append("\n【按文件统计】")
        for file_name, count in sorted(file_count.items()):
            lines.append(f"  - {file_name}: {count} 条")
        
        # 详细列表
        lines.append("\n【详细列表】")
        for i, sql in enumerate(sqls, 1):
            lines.append(f"\n--- SQL #{i} ---")
            lines.append(f"来源: {sql.source_file}:{sql.line_start}-{sql.line_end}")
            lines.append(f"类型: {sql.sql_type.value}")
            # 截取前200个字符
            preview = sql.sql[:200] + "..." if len(sql.sql) > 200 else sql.sql
            lines.append(f"内容: {preview}")
        
        lines.append("\n" + "=" * 60)
        
        return "\n".join(lines)


    def process_file_to_output(self, input_file: str, output_dir: str, 
                                 input_base_dir: str = None,
                                 skip_empty: bool = True,
                                 copy_source: bool = True) -> Tuple[int, Optional[str], Optional[str], Optional[str]]:
        """
        处理单个文件并输出到指定目录，保持目录结构
        
        Args:
            input_file: 输入文件路径
            output_dir: 输出目录路径
            input_base_dir: 输入基础目录（用于计算相对路径）
            skip_empty: 无SQL时不生成文件
            copy_source: 是否复制输入文件到输出目录
            
        Returns:
            (提取的SQL数量, SQL输出文件路径, JSON输出文件路径, 复制的源文件路径)
        """
        input_path = Path(input_file)
        output_path = Path(output_dir)
        
        # 解析文件
        sqls = self.parse_file(str(input_file))
        
        # 如果没有SQL且skip_empty为True，则不生成文件
        if not sqls and skip_empty:
            return 0, None, None, None
        
        # 计算相对路径
        if input_base_dir:
            base_path = Path(input_base_dir)
            try:
                relative_path = input_path.relative_to(base_path)
            except ValueError:
                relative_path = Path(input_path.name)
        else:
            relative_path = Path(input_path.name)
        
        # 构建输出文件路径（保持目录结构）
        output_subdir = output_path / relative_path.parent
        output_subdir.mkdir(parents=True, exist_ok=True)
        
        # 输出文件名（去掉原扩展名，添加新扩展名）
        base_name = relative_path.stem
        sql_output_file = output_subdir / f"{base_name}.sql"
        json_output_file = output_subdir / f"{base_name}.json"
        
        # 写入SQL文件
        self.write_to_file(sqls, str(sql_output_file), format='sql')
        
        # 写入JSON文件
        self.write_to_file(sqls, str(json_output_file), format='json')
        
        # 复制源文件到输出目录
        copied_source_file = None
        if copy_source:
            source_copy_file = output_subdir / relative_path.name
            shutil.copy2(str(input_path), str(source_copy_file))
            copied_source_file = str(source_copy_file)
        
        return len(sqls), str(sql_output_file), str(json_output_file), copied_source_file
    
    def process_directory_to_output(self, input_dir: str, output_dir: str,
                                     extensions: List[str] = None,
                                     recursive: bool = True,
                                     skip_empty: bool = True,
                                     copy_source: bool = True,
                                     include_patterns: List[str] = None,
                                     exclude_patterns: List[str] = None) -> Dict[str, Any]:
        """
        处理整个目录并输出到指定目录，保持目录结构
        
        Args:
            input_dir: 输入目录路径
            output_dir: 输出目录路径
            extensions: 文件扩展名列表（如果有include_patterns则自动提取）
            recursive: 是否递归处理子目录
            skip_empty: 无SQL时不生成文件
            copy_source: 是否复制输入文件到输出目录
            include_patterns: 包含规则（Unix通配符）
            exclude_patterns: 排除规则（Unix通配符）
            
        Returns:
            处理结果统计
        """
        # 从include_patterns自动提取扩展名，如果没有指定extensions
        if extensions is None:
            extensions = extract_extensions_from_patterns(include_patterns)
        
        input_path = Path(input_dir)
        output_path = Path(output_dir)
        
        if not input_path.exists():
            raise FileNotFoundError(f"输入目录不存在: {input_dir}")
        
        output_path.mkdir(parents=True, exist_ok=True)
        
        results = {
            'total_files': 0,           # 处理的文件总数
            'files_with_sql': 0,        # 有SQL的文件数量
            'files_without_sql': 0,     # 无SQL的文件数量
            'total_sqls': 0,            # SQL语句总数
            'processed_files': [],
            'skipped_files': [],        # 跳过的文件（无SQL）
            'errors': []
        }
        
        pattern = '**/*' if recursive else '*'
        
        for ext in extensions:
            for file_path in input_path.glob(pattern + ext):
                if file_path.is_file():
                    # 应用过滤规则
                    if not self.extractor._should_process_file(
                        file_path, input_path, include_patterns, exclude_patterns
                    ):
                        continue
                    
                    try:
                        sql_count, sql_file, json_file, copied_file = self.process_file_to_output(
                            str(file_path), 
                            str(output_path),
                            str(input_path),
                            skip_empty=skip_empty,
                            copy_source=copy_source
                        )
                        results['total_files'] += 1
                        results['total_sqls'] += sql_count
                        
                        if sql_count > 0:
                            results['files_with_sql'] += 1
                            results['processed_files'].append({
                                'input': str(file_path),
                                'sql_output': sql_file,
                                'json_output': json_file,
                                'source_copy': copied_file,
                                'sql_count': sql_count
                            })
                            print(f"[OK] {file_path.name} -> 提取 {sql_count} 条SQL")
                        else:
                            results['files_without_sql'] += 1
                            results['skipped_files'].append(str(file_path))
                            print(f"[SKIP] {file_path.name} -> 无SQL")
                    except Exception as e:
                        results['errors'].append({
                            'file': str(file_path),
                            'error': str(e)
                        })
                        print(f"[ERROR] {file_path.name}: {e}")
        
        return results


def load_config(config_path: str = None) -> Dict[str, Any]:
    """
    加载YAML配置文件
    
    Args:
        config_path: 配置文件路径，默认为同目录下的config.yaml
        
    Returns:
        配置字典
    """
    default_config = {
        'input_dir': '',
        'input_file': '',
        'output_dir': '',
        'recursive': True,
        'skip_empty': True,
        'copy_source': True,
        'verbose': False,
        'include_patterns': [],
        'exclude_patterns': ['__pycache__', '.git', '.svn', '*.pyc', '*.pyo', 'node_modules', '.venv', 'venv']
    }
    
    if config_path is None:
        # 默认配置文件路径
        script_dir = Path(__file__).parent
        config_path = script_dir / 'config.yaml'
    else:
        config_path = Path(config_path)
    
    if config_path.exists():
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                file_config = yaml.safe_load(f) or {}
            # 合并配置，文件配置覆盖默认配置
            for key, value in file_config.items():
                if value is not None and value != '':
                    default_config[key] = value
        except Exception as e:
            print(f"警告: 加载配置文件失败: {e}")
    
    return default_config


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='SQL代码提取工具 - 从代码文件中提取SQL语句',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 解析目录
  python sql_parser.py -i ./input -o ./output
  
  # 解析单个文件
  python sql_parser.py -f ./input/sample.py -o ./output
  
  # 指定文件扩展名
  python sql_parser.py -i ./input -o ./output -e .py .sql
  
  # 非递归模式
  python sql_parser.py -i ./input -o ./output --no-recursive
  
  # 使用配置文件
  python sql_parser.py -c config.yaml
  
  # 文件过滤
  python sql_parser.py -i ./input -o ./output --include "*.py" "src/**/*.sql" --exclude "*_test.py"
"""
    )
    
    # 配置文件参数
    parser.add_argument(
        '-c', '--config',
        help='YAML配置文件路径 (默认: config.yaml)'
    )
    
    # 输入参数组（不再强制互斥，因为可以从配置文件读取）
    parser.add_argument(
        '-i', '--input-dir',
        help='输入目录路径'
    )
    parser.add_argument(
        '-f', '--input-file',
        help='输入文件路径'
    )
    
    # 输出参数
    parser.add_argument(
        '-o', '--output-dir',
        help='输出目录路径'
    )
    
    # 可选参数
    parser.add_argument(
        '-e', '--extensions',
        nargs='+',
        help='要处理的文件扩展名 (从include_patterns自动提取)'
    )
    
    parser.add_argument(
        '--no-recursive',
        action='store_true',
        help='不递归处理子目录'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='显示详细输出'
    )
    
    # 无SQL时不生成文件的控制
    skip_group = parser.add_mutually_exclusive_group()
    skip_group.add_argument(
        '--skip-empty',
        action='store_true',
        default=None,
        help='无SQL时不生成.sql/.json文件 (默认)'
    )
    skip_group.add_argument(
        '--no-skip-empty',
        action='store_true',
        help='无SQL时也生成空的.sql/.json文件'
    )
    
    # 复制源文件控制
    copy_group = parser.add_mutually_exclusive_group()
    copy_group.add_argument(
        '--copy-source',
        action='store_true',
        default=None,
        help='复制输入文件到输出目录 (默认)'
    )
    copy_group.add_argument(
        '--no-copy-source',
        action='store_true',
        help='不复制输入文件到输出目录'
    )
    
    # 文件过滤参数
    parser.add_argument(
        '--include',
        nargs='+',
        help='包含规则 (Unix通配符, 例如: "*.py" "src/**/*.sql")'
    )
    parser.add_argument(
        '--exclude',
        nargs='+',
        help='排除规则 (Unix通配符, 例如: "*_test.py" "__pycache__")'
    )
    
    return parser.parse_args()


def main():
    """主函数 - 命令行入口"""
    args = parse_args()
    
    # 加载配置文件
    config = load_config(args.config)
    
    # 命令行参数覆盖配置文件
    input_dir = args.input_dir or config.get('input_dir', '')
    input_file = args.input_file or config.get('input_file', '')
    output_dir = args.output_dir or config.get('output_dir', '')
    recursive = not args.no_recursive and config.get('recursive', True)
    verbose = args.verbose or config.get('verbose', False)
    
    # 过滤规则
    include_patterns = args.include or config.get('include_patterns', [])
    exclude_patterns = args.exclude or config.get('exclude_patterns', [])
    
    # 扩展名：优先使用命令行参数，否则从include_patterns自动提取
    extensions = args.extensions
    if not extensions:
        extensions = extract_extensions_from_patterns(include_patterns)
    
    # skip_empty 参数处理
    if args.no_skip_empty:
        skip_empty = False
    elif args.skip_empty:
        skip_empty = True
    else:
        skip_empty = config.get('skip_empty', True)
    
    # copy_source 参数处理
    if args.no_copy_source:
        copy_source = False
    elif args.copy_source:
        copy_source = True
    else:
        copy_source = config.get('copy_source', True)
    
    # 验证必要参数
    if not input_dir and not input_file:
        print("错误: 必须指定输入目录(-i)或输入文件(-f)")
        return 1
    
    if not output_dir:
        print("错误: 必须指定输出目录(-o)")
        return 1
    
    print("=" * 60)
    print("SQL代码提取工具")
    print("=" * 60)
    
    parser = SQLParser()
    
    try:
        if input_file:
            # 处理单个文件
            input_file_path = Path(input_file)
            if not input_file_path.exists():
                print(f"错误: 输入文件不存在: {input_file}")
                return 1
            
            print(f"\n输入文件: {input_file}")
            print(f"输出目录: {output_dir}")
            print(f"跳过无SQL文件: {'是' if skip_empty else '否'}")
            print(f"复制源文件: {'是' if copy_source else '否'}")
            print("-" * 60)
            
            sql_count, sql_file, json_file, copied_file = parser.process_file_to_output(
                input_file,
                output_dir,
                skip_empty=skip_empty,
                copy_source=copy_source
            )
            
            print(f"\n处理完成!")
            print(f"  提取SQL数量: {sql_count}")
            if sql_file:
                print(f"  SQL输出文件: {sql_file}")
                print(f"  JSON输出文件: {json_file}")
                if copied_file:
                    print(f"  源文件副本: {copied_file}")
            else:
                print("  未生成输出文件 (无SQL)")
            
        else:
            # 处理目录
            input_dir_path = Path(input_dir)
            if not input_dir_path.exists():
                print(f"错误: 输入目录不存在: {input_dir}")
                return 1
            
            print(f"\n输入目录: {input_dir}")
            print(f"输出目录: {output_dir}")
            print(f"文件扩展名: {', '.join(extensions)}")
            print(f"递归处理: {'是' if recursive else '否'}")
            print(f"跳过无SQL文件: {'是' if skip_empty else '否'}")
            print(f"复制源文件: {'是' if copy_source else '否'}")
            if include_patterns:
                print(f"包含规则: {', '.join(include_patterns)}")
            if exclude_patterns:
                print(f"排除规则: {', '.join(exclude_patterns)}")
            print("-" * 60)
            
            results = parser.process_directory_to_output(
                input_dir,
                output_dir,
                extensions=extensions,
                recursive=recursive,
                skip_empty=skip_empty,
                copy_source=copy_source,
                include_patterns=include_patterns,
                exclude_patterns=exclude_patterns
            )
            
            print("\n" + "=" * 60)
            print("处理完成!")
            print(f"  处理文件总数: {results['total_files']}")
            print(f"  有SQL的文件: {results['files_with_sql']}")
            print(f"  无SQL的文件: {results['files_without_sql']}")
            print(f"  提取SQL总数: {results['total_sqls']}")
            
            if results['errors']:
                print(f"  错误数: {len(results['errors'])}")
                for err in results['errors']:
                    print(f"    - {err['file']}: {err['error']}")
            
            # 保存处理结果摘要
            summary_file = Path(output_dir) / "_summary.json"
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            print(f"  结果摘要: {summary_file}")
        
        print("=" * 60)
        return 0
        
    except Exception as e:
        print(f"\n错误: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
