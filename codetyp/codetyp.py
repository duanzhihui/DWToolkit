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
class PatternRule:
    """模式规则"""
    pattern: str
    score: int = 1


@dataclass
class ClassificationRule:
    """分类规则"""
    name: str
    patterns: List[PatternRule]
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
    
    # 默认分类规则 - 只保留互斥规则（各类型独有的特征）
    DEFAULT_RULES = {
        "hive": {
            "patterns": [
                # Hive独有SQL关键字
                {"pattern": r"\bPARTITIONED\s+BY\b", "score": 10},
                {"pattern": r"\bSTORED\s+AS\s+(ORC|PARQUET|TEXTFILE|AVRO|SEQUENCEFILE)\b", "score": 10},
                {"pattern": r"\bLOCATION\s+['\"]hdfs://", "score": 8},
                {"pattern": r"\bMSCK\s+REPAIR\s+TABLE\b", "score": 15},
                {"pattern": r"\bLOAD\s+DATA\s+(LOCAL\s+)?INPATH\b", "score": 10},
                {"pattern": r"\bSET\s+hive\.\w+", "score": 15},
                # Hive独有函数
                {"pattern": r"\bget_json_object\s*\(", "score": 10},
                {"pattern": r"\blateral\s+view\s+explode\b", "score": 12},
                {"pattern": r"\bcollect_set\s*\(", "score": 8},
                {"pattern": r"\bcollect_list\s*\(", "score": 8},
                {"pattern": r"\bposexplode\s*\(", "score": 10},
                # Hive独有调用方式
                {"pattern": r"HiveContext", "score": 15},
                {"pattern": r"hiveql", "score": 12},
                {"pattern": r"beeline\s+-e", "score": 15},
                {"pattern": r"hive\s+-e\s+['\"]", "score": 15},
                {"pattern": r"execute_hive_query", "score": 12},
                {"pattern": r"run_hive_sql", "score": 12},
                {"pattern": r"HiveServer2", "score": 15},
                {"pattern": r"pyhive\.hive", "score": 15},
                {"pattern": r"from\s+pyhive\s+import\s+hive", "score": 15},
                {"pattern": r"impyla.*connect", "score": 10},
            ],
            "description": "Hive SQL及相关调用",
            "priority": 10
        },
        "spark": {
            "patterns": [
                # Spark独有核心类
                {"pattern": r"\bSparkSession\b", "score": 20},
                {"pattern": r"\bSparkContext\b", "score": 20},
                {"pattern": r"\bSparkConf\b", "score": 15},
                {"pattern": r"spark\s*=\s*SparkSession", "score": 20},
                {"pattern": r"sc\s*=\s*SparkContext", "score": 20},
                # Spark独有DataFrame API
                {"pattern": r"\.createDataFrame\s*\(", "score": 15},
                {"pattern": r"\.read\s*\.\s*(csv|json|parquet|orc|jdbc|format)\s*\(", "score": 12},
                {"pattern": r"\.write\s*\.\s*(csv|json|parquet|orc|jdbc|format|mode)\s*\(", "score": 12},
                {"pattern": r"\.withColumn\s*\(", "score": 10},
                {"pattern": r"\.dropDuplicates\s*\(", "score": 8},
                {"pattern": r"\.repartition\s*\(", "score": 8},
                {"pattern": r"\.coalesce\s*\(", "score": 8},
                {"pattern": r"\.cache\s*\(", "score": 8},
                {"pattern": r"\.persist\s*\(", "score": 8},
                # Spark独有RDD API
                {"pattern": r"\.parallelize\s*\(", "score": 12},
                {"pattern": r"\.reduceByKey\s*\(", "score": 12},
                {"pattern": r"\.groupByKey\s*\(", "score": 10},
                {"pattern": r"\.sortByKey\s*\(", "score": 10},
                {"pattern": r"\.saveAsTextFile\s*\(", "score": 10},
                # Spark SQL独有
                {"pattern": r"spark\.sql\s*\(", "score": 15},
                {"pattern": r"\.createOrReplaceTempView\s*\(", "score": 12},
                {"pattern": r"\.createTempView\s*\(", "score": 10},
                {"pattern": r"\.registerTempTable\s*\(", "score": 10},
                # PySpark独有imports
                {"pattern": r"from\s+pyspark\b", "score": 20},
                {"pattern": r"import\s+pyspark\b", "score": 20},
                {"pattern": r"pyspark\.sql", "score": 15},
                {"pattern": r"pyspark\.ml", "score": 15},
                {"pattern": r"pyspark\.mllib", "score": 15},
                {"pattern": r"pyspark\.streaming", "score": 15},
                # Spark独有调用方式
                {"pattern": r"spark-submit", "score": 20},
                {"pattern": r"spark2-submit", "score": 20},
                {"pattern": r"SparkSubmit", "score": 15},
                {"pattern": r"spark\.jars", "score": 10},
                {"pattern": r"spark\.executor", "score": 12},
                {"pattern": r"spark\.driver", "score": 12},
                {"pattern": r"--master\s+(yarn|local|spark://|mesos://|k8s://)", "score": 15},
                {"pattern": r"findspark\.init", "score": 15},
            ],
            "description": "Spark及PySpark代码",
            "priority": 20
        },
        "flink": {
            "patterns": [
                # Flink独有环境类
                {"pattern": r"\bStreamExecutionEnvironment\b", "score": 25},
                {"pattern": r"\bExecutionEnvironment\b", "score": 20},
                {"pattern": r"\bTableEnvironment\b", "score": 20},
                {"pattern": r"\bStreamTableEnvironment\b", "score": 25},
                {"pattern": r"\bBatchTableEnvironment\b", "score": 20},
                {"pattern": r"\.getExecutionEnvironment\s*\(", "score": 20},
                {"pattern": r"\.createLocalEnvironment\s*\(", "score": 15},
                # Flink独有DataStream API
                {"pattern": r"\.addSource\s*\(", "score": 15},
                {"pattern": r"\.addSink\s*\(", "score": 15},
                {"pattern": r"\.keyBy\s*\(", "score": 12},
                {"pattern": r"\.timeWindow\s*\(", "score": 15},
                {"pattern": r"\.countWindow\s*\(", "score": 15},
                {"pattern": r"\.assignTimestampsAndWatermarks\s*\(", "score": 18},
                {"pattern": r"\.getSideOutput\s*\(", "score": 12},
                # Flink独有Table API
                {"pattern": r"\.executeSql\s*\(", "score": 15},
                {"pattern": r"\.sqlQuery\s*\(", "score": 12},
                {"pattern": r"\.createTemporaryView\s*\(", "score": 10},
                # Flink独有SQL语法
                {"pattern": r"'connector'\s*=", "score": 15},
                {"pattern": r"'format'\s*=", "score": 10},
                {"pattern": r"WATERMARK\s+FOR\b", "score": 20},
                {"pattern": r"TUMBLE\s*\(", "score": 18},
                {"pattern": r"HOP\s*\(", "score": 18},
                {"pattern": r"SESSION\s*\(", "score": 18},
                {"pattern": r"MATCH_RECOGNIZE\b", "score": 20},
                # Flink独有imports
                {"pattern": r"org\.apache\.flink\b", "score": 25},
                {"pattern": r"from\s+pyflink\b", "score": 25},
                {"pattern": r"import\s+pyflink\b", "score": 25},
                {"pattern": r"pyflink\.datastream", "score": 20},
                {"pattern": r"pyflink\.table", "score": 20},
                # Flink独有connectors
                {"pattern": r"FlinkKafkaConsumer", "score": 20},
                {"pattern": r"FlinkKafkaProducer", "score": 20},
                {"pattern": r"JdbcSink", "score": 15},
                {"pattern": r"FileSink", "score": 12},
                {"pattern": r"FileSource", "score": 12},
                # Flink独有调用方式
                {"pattern": r"flink\s+run\b", "score": 20},
                {"pattern": r"flink-run", "score": 15},
                {"pattern": r"FlinkRunner", "score": 15},
                {"pattern": r"flink\.execution", "score": 12},
                {"pattern": r"flink\.parallelism", "score": 12},
                {"pattern": r"flink\.checkpointing", "score": 15},
                {"pattern": r"state\.backend", "score": 12},
                {"pattern": r"checkpoint\.interval", "score": 12},
            ],
            "description": "Flink流处理及批处理代码",
            "priority": 30
        },
        "presto": {
            "patterns": [
                {"pattern": r"\bpresto\b.*\bquery\b", "score": 15},
                {"pattern": r"presto://", "score": 20},
                {"pattern": r"prestodb", "score": 20},
                {"pattern": r"from\s+prestodb\b", "score": 20},
                {"pattern": r"PrestoClient", "score": 20},
                {"pattern": r"presto-cli", "score": 15},
                {"pattern": r"--catalog\s+\w+\s+--schema", "score": 10},
            ],
            "description": "Presto查询",
            "priority": 5
        },
        "trino": {
            "patterns": [
                {"pattern": r"\btrino\b.*\bquery\b", "score": 15},
                {"pattern": r"trino://", "score": 20},
                {"pattern": r"from\s+trino\b", "score": 20},
                {"pattern": r"TrinoClient", "score": 20},
                {"pattern": r"trino-cli", "score": 15},
            ],
            "description": "Trino查询",
            "priority": 5
        },
        "kafka": {
            "patterns": [
                {"pattern": r"from\s+kafka\b", "score": 20},
                {"pattern": r"kafka-python", "score": 15},
                {"pattern": r"kafka://", "score": 15},
                {"pattern": r"confluent_kafka", "score": 20},
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
            patterns = []
            for p in rule_data["patterns"]:
                if isinstance(p, dict):
                    patterns.append(PatternRule(pattern=p["pattern"], score=p.get("score", 1)))
                else:
                    patterns.append(PatternRule(pattern=p, score=1))
            self.rules[name] = ClassificationRule(
                name=name,
                patterns=patterns,
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
            patterns = []
            for p in rule_data.get("patterns", []):
                if isinstance(p, dict):
                    patterns.append(PatternRule(pattern=p["pattern"], score=p.get("score", 1)))
                else:
                    patterns.append(PatternRule(pattern=p, score=1))
            self.rules[name] = ClassificationRule(
                name=name,
                patterns=patterns,
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
    
    def classify_content(self, content: str) -> List[Tuple[str, int, str]]:
        """
        分类代码内容
        
        Args:
            content: 代码内容
            
        Returns:
            匹配的分类列表，每项为 (类型名, 总积分, 匹配证据)
        """
        matches = []
        
        for rule_name, rule in self.rules.items():
            total_score = 0
            matched_evidences = []
            
            for pattern_rule in rule.patterns:
                try:
                    found = re.findall(pattern_rule.pattern, content, re.IGNORECASE | re.MULTILINE)
                    if found:
                        # 获取匹配的文本作为证据
                        if isinstance(found[0], tuple):
                            evidence = found[0][0] if found[0] else pattern_rule.pattern
                        else:
                            evidence = found[0] if found else pattern_rule.pattern
                        # 累加积分（每个模式匹配一次计分）
                        total_score += pattern_rule.score
                        matched_evidences.append(f"{str(evidence)[:30]}(+{pattern_rule.score})")
                except re.error:
                    continue
            
            if total_score > 0:
                # 合并所有匹配证据
                evidence = "; ".join(matched_evidences[:8])  # 最多8个证据
                matches.append((rule_name, total_score, evidence))
        
        # 按积分降序排序
        matches.sort(key=lambda x: x[1], reverse=True)
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
        
        # 选择积分最高的类型
        best_type = matches[0][0]
        
        # 构建依据：按积分降序显示所有匹配类型的积分和证据
        evidences = [f"{m[0]}({m[1]}分): {m[2]}" for m in matches]
        
        return ClassificationResult(
            file_dir=os.path.dirname(file_path),
            file_name=os.path.basename(file_path),
            code_type=best_type,
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
