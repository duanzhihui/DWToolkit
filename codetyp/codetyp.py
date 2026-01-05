# -*- coding: utf-8 -*-
"""
代码分类程序 - 识别大数据代码类型（Hive, Spark, Flink等）

功能:
1. 扫描指定目录下的代码文件
2. 支持文件过滤（包含/排除规则，Unix通配符）
3. 使用正则表达式分类代码类型
4. 支持识别隐藏的函数调用方式
5. 输出结果到Excel文件

作者: DWToolkit
"""

import os
import re
import sys
import argparse
import fnmatch
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Set
from dataclasses import dataclass, field

import yaml
import pandas as pd


@dataclass
class ClassificationRule:
    """分类规则"""
    name: str
    patterns: List[str]
    description: str = ""
    priority: int = 0


@dataclass
class ClassificationResult:
    """分类结果"""
    file_dir: str
    file_name: str
    code_type: str
    evidence: str


@dataclass
class FileFilter:
    """文件过滤器"""
    include_patterns: List[str] = field(default_factory=list)
    exclude_patterns: List[str] = field(default_factory=list)
    
    def match(self, file_path: str) -> bool:
        """检查文件是否匹配过滤规则"""
        file_name = os.path.basename(file_path)
        
        # 如果有包含规则，必须匹配至少一个
        if self.include_patterns:
            included = any(fnmatch.fnmatch(file_name, p) for p in self.include_patterns)
            if not included:
                return False
        
        # 检查排除规则
        if self.exclude_patterns:
            excluded = any(fnmatch.fnmatch(file_name, p) for p in self.exclude_patterns)
            if excluded:
                return False
        
        return True


class CodeClassifier:
    """代码分类器"""
    
    # 默认分类规则
    DEFAULT_RULES = {
        "hive": {
            "patterns": [
                # 直接SQL关键字
                r"\bCREATE\s+(EXTERNAL\s+)?TABLE\b",
                r"\bPARTITIONED\s+BY\b",
                r"\bSTORED\s+AS\s+(ORC|PARQUET|TEXTFILE|AVRO|SEQUENCEFILE)\b",
                r"\bLOCATION\s+['\"]hdfs://",
                r"\bMSCK\s+REPAIR\s+TABLE\b",
                r"\bLOAD\s+DATA\s+(LOCAL\s+)?INPATH\b",
                r"\bINSERT\s+(INTO|OVERWRITE)\s+TABLE\b",
                r"\bALTER\s+TABLE\s+\w+\s+ADD\s+PARTITION\b",
                r"\bSET\s+hive\.\w+",
                r"\bUSE\s+\w+\s*;",
                # Hive函数
                r"\bget_json_object\s*\(",
                r"\blateral\s+view\s+explode\b",
                r"\bcollect_set\s*\(",
                r"\bcollect_list\s*\(",
                r"\bmap_keys\s*\(",
                r"\bmap_values\s*\(",
                r"\bposexplode\s*\(",
                r"\bstack\s*\(",
                # 隐藏调用方式
                r"hive\s*\.\s*execute",
                r"hive_client\s*\.\s*query",
                r"HiveContext",
                r"hiveql",
                r"beeline\s+-e",
                r"hive\s+-e\s+['\"]",
                r"spark\.sql\s*\(['\"].*?(CREATE|INSERT|ALTER)\s+TABLE",
                r"execute_hive_query",
                r"run_hive_sql",
                r"HiveServer2",
                r"pyhive\.hive",
                r"from\s+pyhive\s+import\s+hive",
                r"impyla.*connect",
            ],
            "description": "Hive SQL及相关调用",
            "priority": 10
        },
        "spark": {
            "patterns": [
                # SparkSession/SparkContext
                r"\bSparkSession\b",
                r"\bSparkContext\b",
                r"\bSparkConf\b",
                r"spark\s*=\s*SparkSession",
                r"sc\s*=\s*SparkContext",
                # DataFrame API
                r"\.createDataFrame\s*\(",
                r"\.read\s*\.\s*(csv|json|parquet|orc|jdbc|format)\s*\(",
                r"\.write\s*\.\s*(csv|json|parquet|orc|jdbc|format|mode)\s*\(",
                r"\.select\s*\(",
                r"\.filter\s*\(",
                r"\.groupBy\s*\(",
                r"\.agg\s*\(",
                r"\.join\s*\(",
                r"\.withColumn\s*\(",
                r"\.drop\s*\(",
                r"\.dropDuplicates\s*\(",
                r"\.distinct\s*\(",
                r"\.orderBy\s*\(",
                r"\.repartition\s*\(",
                r"\.coalesce\s*\(",
                r"\.cache\s*\(",
                r"\.persist\s*\(",
                r"\.unpersist\s*\(",
                # RDD API
                r"\.parallelize\s*\(",
                r"\.map\s*\(",
                r"\.flatMap\s*\(",
                r"\.reduceByKey\s*\(",
                r"\.groupByKey\s*\(",
                r"\.sortByKey\s*\(",
                r"\.collect\s*\(\s*\)",
                r"\.take\s*\(\d+\)",
                r"\.count\s*\(\s*\)",
                r"\.saveAsTextFile\s*\(",
                # Spark SQL
                r"spark\.sql\s*\(",
                r"\.createOrReplaceTempView\s*\(",
                r"\.createTempView\s*\(",
                r"\.registerTempTable\s*\(",
                # PySpark imports
                r"from\s+pyspark\b",
                r"import\s+pyspark\b",
                r"pyspark\.sql",
                r"pyspark\.ml",
                r"pyspark\.mllib",
                r"pyspark\.streaming",
                # 隐藏调用方式
                r"spark-submit",
                r"spark2-submit",
                r"SparkSubmit",
                r"spark\.jars",
                r"spark\.executor",
                r"spark\.driver",
                r"--master\s+(yarn|local|spark://|mesos://|k8s://)",
                r"findspark\.init",
            ],
            "description": "Spark及PySpark代码",
            "priority": 20
        },
        "flink": {
            "patterns": [
                # Flink环境
                r"\bStreamExecutionEnvironment\b",
                r"\bExecutionEnvironment\b",
                r"\bTableEnvironment\b",
                r"\bStreamTableEnvironment\b",
                r"\bBatchTableEnvironment\b",
                r"\.getExecutionEnvironment\s*\(",
                r"\.createLocalEnvironment\s*\(",
                # Flink DataStream API
                r"\.addSource\s*\(",
                r"\.addSink\s*\(",
                r"\.keyBy\s*\(",
                r"\.window\s*\(",
                r"\.trigger\s*\(",
                r"\.process\s*\(",
                r"\.timeWindow\s*\(",
                r"\.countWindow\s*\(",
                r"\.assignTimestampsAndWatermarks\s*\(",
                r"\.getSideOutput\s*\(",
                r"\.connect\s*\(",
                r"\.union\s*\(",
                # Flink Table API
                r"\.executeSql\s*\(",
                r"\.sqlQuery\s*\(",
                r"\.createTemporaryView\s*\(",
                r"\.from\s*\(['\"]",
                # Flink SQL
                r"CREATE\s+TABLE.*WITH\s*\(",
                r"'connector'\s*=",
                r"'format'\s*=",
                r"WATERMARK\s+FOR\b",
                r"TUMBLE\s*\(",
                r"HOP\s*\(",
                r"SESSION\s*\(",
                r"MATCH_RECOGNIZE\b",
                # Flink imports
                r"org\.apache\.flink\b",
                r"from\s+pyflink\b",
                r"import\s+pyflink\b",
                r"pyflink\.datastream",
                r"pyflink\.table",
                # Flink connectors
                r"FlinkKafkaConsumer",
                r"FlinkKafkaProducer",
                r"JdbcSink",
                r"FileSink",
                r"FileSource",
                # 隐藏调用方式
                r"flink\s+run\b",
                r"flink-run",
                r"FlinkRunner",
                r"flink\.execution",
                r"flink\.parallelism",
                r"flink\.checkpointing",
                r"state\.backend",
                r"checkpoint\.interval",
            ],
            "description": "Flink流处理及批处理代码",
            "priority": 30
        },
        "presto": {
            "patterns": [
                r"\bpresto\b.*\bquery\b",
                r"presto://",
                r"prestodb",
                r"from\s+prestodb\b",
                r"PrestoClient",
                r"presto-cli",
                r"--catalog\s+\w+\s+--schema",
            ],
            "description": "Presto查询",
            "priority": 5
        },
        "trino": {
            "patterns": [
                r"\btrino\b.*\bquery\b",
                r"trino://",
                r"from\s+trino\b",
                r"TrinoClient",
                r"trino-cli",
            ],
            "description": "Trino查询",
            "priority": 5
        },
        "kafka": {
            "patterns": [
                r"KafkaProducer",
                r"KafkaConsumer",
                r"kafka-python",
                r"from\s+kafka\b",
                r"bootstrap\.servers",
                r"kafka://",
                r"confluent_kafka",
            ],
            "description": "Kafka消息队列",
            "priority": 5
        }
    }
    
    def __init__(self, config_path: Optional[str] = None):
        """
        初始化分类器
        
        Args:
            config_path: 配置文件路径，如果为None则使用默认规则
        """
        self.rules: Dict[str, ClassificationRule] = {}
        self.file_filter = FileFilter()
        self.output_path = "classification_result.xlsx"
        self.scan_extensions = [".py", ".sql", ".scala", ".java", ".sh", ".hql", ".hive"]
        
        if config_path:
            self.load_config(config_path)
        else:
            self._load_default_rules()
    
    def _load_default_rules(self):
        """加载默认规则"""
        for name, rule_data in self.DEFAULT_RULES.items():
            self.rules[name] = ClassificationRule(
                name=name,
                patterns=rule_data["patterns"],
                description=rule_data.get("description", ""),
                priority=rule_data.get("priority", 0)
            )
    
    def load_config(self, config_path: str):
        """
        从YAML配置文件加载配置
        
        Args:
            config_path: 配置文件路径
        """
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # 加载分类规则
        rules_config = config.get("classification_rules", {})
        for name, rule_data in rules_config.items():
            self.rules[name] = ClassificationRule(
                name=name,
                patterns=rule_data.get("patterns", []),
                description=rule_data.get("description", ""),
                priority=rule_data.get("priority", 0)
            )
        
        # 加载文件过滤规则
        filter_config = config.get("file_filter", {})
        self.file_filter = FileFilter(
            include_patterns=filter_config.get("include", []),
            exclude_patterns=filter_config.get("exclude", [])
        )
        
        # 加载其他配置
        self.output_path = config.get("output", {}).get("path", "classification_result.xlsx")
        self.scan_extensions = config.get("scan_extensions", self.scan_extensions)
    
    def classify_content(self, content: str) -> List[Tuple[str, str, int]]:
        """
        分类代码内容
        
        Args:
            content: 代码内容
            
        Returns:
            匹配的分类列表，每项为 (类型名, 匹配证据, 优先级)
        """
        matches = []
        
        for rule_name, rule in self.rules.items():
            matched_patterns = []
            for pattern in rule.patterns:
                try:
                    found = re.findall(pattern, content, re.IGNORECASE | re.MULTILINE)
                    if found:
                        # 获取匹配的文本作为证据
                        if isinstance(found[0], tuple):
                            evidence = found[0][0] if found[0] else pattern
                        else:
                            evidence = found[0] if found else pattern
                        matched_patterns.append(str(evidence)[:50])
                except re.error:
                    continue
            
            if matched_patterns:
                # 合并所有匹配证据
                evidence = "; ".join(matched_patterns[:5])  # 最多5个证据
                matches.append((rule_name, evidence, rule.priority))
        
        # 按优先级排序
        matches.sort(key=lambda x: x[2], reverse=True)
        return matches
    
    def classify_file(self, file_path: str) -> Optional[ClassificationResult]:
        """
        分类单个文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            分类结果，如果无法分类则返回None
        """
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
        except Exception as e:
            print(f"警告: 无法读取文件 {file_path}: {e}")
            return None
        
        matches = self.classify_content(content)
        
        if not matches:
            return ClassificationResult(
                file_dir=os.path.dirname(file_path),
                file_name=os.path.basename(file_path),
                code_type="unknown",
                evidence="无匹配规则"
            )
        
        # 获取所有匹配的类型
        code_types = [m[0] for m in matches]
        evidences = [f"{m[0]}:{m[1]}" for m in matches]
        
        return ClassificationResult(
            file_dir=os.path.dirname(file_path),
            file_name=os.path.basename(file_path),
            code_type=",".join(code_types),
            evidence=" | ".join(evidences)
        )
    
    def scan_directory(self, directory: str, recursive: bool = True) -> List[str]:
        """
        扫描目录获取文件列表
        
        Args:
            directory: 目录路径
            recursive: 是否递归扫描子目录
            
        Returns:
            文件路径列表
        """
        files = []
        directory = os.path.abspath(directory)
        
        if recursive:
            for root, _, filenames in os.walk(directory):
                for filename in filenames:
                    file_path = os.path.join(root, filename)
                    # 检查扩展名
                    ext = os.path.splitext(filename)[1].lower()
                    if ext in self.scan_extensions or not self.scan_extensions:
                        # 检查过滤规则
                        if self.file_filter.match(file_path):
                            files.append(file_path)
        else:
            for filename in os.listdir(directory):
                file_path = os.path.join(directory, filename)
                if os.path.isfile(file_path):
                    ext = os.path.splitext(filename)[1].lower()
                    if ext in self.scan_extensions or not self.scan_extensions:
                        if self.file_filter.match(file_path):
                            files.append(file_path)
        
        return files
    
    def classify_directory(self, directory: str, recursive: bool = True) -> List[ClassificationResult]:
        """
        分类目录下的所有代码文件
        
        Args:
            directory: 目录路径
            recursive: 是否递归扫描子目录
            
        Returns:
            分类结果列表
        """
        files = self.scan_directory(directory, recursive)
        results = []
        
        for file_path in files:
            result = self.classify_file(file_path)
            if result:
                results.append(result)
        
        return results
    
    def classify_files(self, file_paths: List[str]) -> List[ClassificationResult]:
        """
        分类指定的文件列表
        
        Args:
            file_paths: 文件路径列表
            
        Returns:
            分类结果列表
        """
        results = []
        for file_path in file_paths:
            if os.path.isfile(file_path):
                if self.file_filter.match(file_path):
                    result = self.classify_file(file_path)
                    if result:
                        results.append(result)
            elif os.path.isdir(file_path):
                results.extend(self.classify_directory(file_path))
        
        return results
    
    def export_to_excel(self, results: List[ClassificationResult], output_path: Optional[str] = None):
        """
        导出结果到Excel文件
        
        Args:
            results: 分类结果列表
            output_path: 输出文件路径，如果为None则使用配置的路径
        """
        if output_path is None:
            output_path = self.output_path
        
        data = []
        for r in results:
            data.append({
                "文件目录": r.file_dir,
                "文件名": r.file_name,
                "代码类型": r.code_type,
                "代码类型依据": r.evidence
            })
        
        df = pd.DataFrame(data)
        df.to_excel(output_path, index=False, engine='openpyxl')
        print(f"结果已导出到: {output_path}")
        return output_path


def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(
        description="代码分类程序 - 识别大数据代码类型（Hive, Spark, Flink等）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 扫描目录
  python codetyp.py -d ./code
  
  # 扫描指定文件
  python codetyp.py -f file1.py file2.sql
  
  # 使用配置文件
  python codetyp.py -d ./code -c config.yml
  
  # 指定输出文件
  python codetyp.py -d ./code -o result.xlsx
  
  # 包含/排除文件
  python codetyp.py -d ./code --include "*.py" "*.sql" --exclude "*_test.py"
        """
    )
    
    parser.add_argument("-d", "--directory", type=str, help="要扫描的目录路径")
    parser.add_argument("-f", "--files", nargs="+", type=str, help="要分类的文件路径列表")
    parser.add_argument("-c", "--config", type=str, help="配置文件路径 (YAML格式)")
    parser.add_argument("-o", "--output", type=str, default="classification_result.xlsx", 
                        help="输出Excel文件路径 (默认: classification_result.xlsx)")
    parser.add_argument("--include", nargs="+", type=str, help="包含的文件模式 (Unix通配符)")
    parser.add_argument("--exclude", nargs="+", type=str, help="排除的文件模式 (Unix通配符)")
    parser.add_argument("-r", "--recursive", action="store_true", default=True,
                        help="递归扫描子目录 (默认: True)")
    parser.add_argument("--no-recursive", action="store_false", dest="recursive",
                        help="不递归扫描子目录")
    parser.add_argument("-v", "--verbose", action="store_true", help="显示详细信息")
    
    args = parser.parse_args()
    
    # 检查输入参数
    if not args.directory and not args.files:
        parser.print_help()
        print("\n错误: 必须指定 -d/--directory 或 -f/--files 参数")
        sys.exit(1)
    
    # 初始化分类器
    classifier = CodeClassifier(args.config)
    
    # 应用命令行过滤规则
    if args.include:
        classifier.file_filter.include_patterns.extend(args.include)
    if args.exclude:
        classifier.file_filter.exclude_patterns.extend(args.exclude)
    
    # 执行分类
    results = []
    
    if args.directory:
        if args.verbose:
            print(f"扫描目录: {args.directory}")
        results.extend(classifier.classify_directory(args.directory, args.recursive))
    
    if args.files:
        if args.verbose:
            print(f"分类文件: {args.files}")
        results.extend(classifier.classify_files(args.files))
    
    # 输出结果
    if results:
        if args.verbose:
            print(f"\n共分类 {len(results)} 个文件:")
            for r in results:
                print(f"  {r.file_name}: {r.code_type}")
        
        classifier.export_to_excel(results, args.output)
    else:
        print("未找到匹配的文件")


if __name__ == "__main__":
    main()
