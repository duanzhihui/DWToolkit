"""代码解析器 - 使用 Python AST 解析 PySpark 代码"""

import ast
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set


@dataclass
class SparkCodeStructure:
    """Spark 代码结构"""
    file_path: str
    imports: List[Dict[str, Any]] = field(default_factory=list)
    spark_session_usage: List[Dict[str, Any]] = field(default_factory=list)
    dataframe_operations: List[Dict[str, Any]] = field(default_factory=list)
    rdd_operations: List[Dict[str, Any]] = field(default_factory=list)
    configurations: List[Dict[str, Any]] = field(default_factory=list)
    deprecated_apis: List[Dict[str, Any]] = field(default_factory=list)
    function_calls: List[Dict[str, Any]] = field(default_factory=list)
    raw_code: str = ""
    ast_tree: Optional[ast.AST] = None


class SparkASTVisitor(ast.NodeVisitor):
    """AST 访问器，用于提取 Spark 相关代码结构"""

    # Spark 相关的模块和类
    SPARK_MODULES = {
        "pyspark",
        "pyspark.sql",
        "pyspark.sql.functions",
        "pyspark.sql.types",
        "pyspark.sql.window",
        "pyspark.ml",
        "pyspark.mllib",
        "pyspark.streaming",
        "pyspark.context",
        "pyspark.conf",
    }

    # DataFrame 操作方法
    DATAFRAME_METHODS = {
        "select", "filter", "where", "groupBy", "agg", "join", "union",
        "withColumn", "withColumnRenamed", "drop", "distinct", "orderBy",
        "sort", "limit", "show", "collect", "count", "first", "head",
        "take", "toPandas", "createOrReplaceTempView", "write", "read",
        "cache", "persist", "unpersist", "repartition", "coalesce",
        "crossJoin", "alias", "toDF", "printSchema", "explain",
        "dropDuplicates", "fillna", "na", "replace", "sample",
        "randomSplit", "subtract", "intersect", "exceptAll",
    }

    # RDD 操作方法
    RDD_METHODS = {
        "map", "flatMap", "filter", "reduce", "reduceByKey", "groupByKey",
        "sortByKey", "join", "cogroup", "cartesian", "pipe", "coalesce",
        "repartition", "sample", "union", "intersection", "distinct",
        "groupBy", "aggregate", "fold", "foreach", "foreachPartition",
        "collect", "count", "first", "take", "top", "takeOrdered",
        "saveAsTextFile", "saveAsSequenceFile", "saveAsObjectFile",
        "countByKey", "countByValue", "collectAsMap", "lookup",
        "parallelize", "textFile", "wholeTextFiles",
    }

    # 已弃用的 API
    DEPRECATED_APIS = {
        # Spark 2.x -> 3.x 弃用
        "SparkContext": "SparkSession.builder.getOrCreate()",
        "SQLContext": "SparkSession",
        "HiveContext": "SparkSession.builder.enableHiveSupport()",
        "registerTempTable": "createOrReplaceTempView",
        "unionAll": "union",
        "na.fill": "fillna",
        "na.drop": "dropna",
        "toPandas": "toPandas (注意: 大数据集可能导致内存问题)",
    }

    def __init__(self):
        self.imports: List[Dict[str, Any]] = []
        self.spark_session_usage: List[Dict[str, Any]] = []
        self.dataframe_operations: List[Dict[str, Any]] = []
        self.rdd_operations: List[Dict[str, Any]] = []
        self.configurations: List[Dict[str, Any]] = []
        self.deprecated_apis: List[Dict[str, Any]] = []
        self.function_calls: List[Dict[str, Any]] = []
        self._spark_aliases: Set[str] = set()
        self._df_aliases: Set[str] = set()

    def visit_Import(self, node: ast.Import) -> None:
        """处理 import 语句"""
        for alias in node.names:
            module_name = alias.name
            as_name = alias.asname

            import_info = {
                "type": "import",
                "module": module_name,
                "alias": as_name,
                "line": node.lineno,
                "col": node.col_offset,
                "is_spark": self._is_spark_module(module_name),
            }
            self.imports.append(import_info)

            # 跟踪 Spark 相关的别名
            if self._is_spark_module(module_name):
                self._spark_aliases.add(as_name or module_name.split(".")[-1])

        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """处理 from ... import 语句"""
        module = node.module or ""

        for alias in node.names:
            name = alias.name
            as_name = alias.asname

            import_info = {
                "type": "from_import",
                "module": module,
                "name": name,
                "alias": as_name,
                "line": node.lineno,
                "col": node.col_offset,
                "is_spark": self._is_spark_module(module),
            }
            self.imports.append(import_info)

            # 跟踪 Spark 相关的别名
            if self._is_spark_module(module):
                self._spark_aliases.add(as_name or name)

                # 特殊处理 SparkSession
                if name == "SparkSession":
                    self._spark_aliases.add(as_name or "SparkSession")

        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        """处理函数调用"""
        call_info = self._extract_call_info(node)

        if call_info:
            self.function_calls.append(call_info)

            # 检查是否是 SparkSession 相关调用
            if self._is_spark_session_call(call_info):
                self.spark_session_usage.append(call_info)

            # 检查是否是 DataFrame 操作
            if self._is_dataframe_operation(call_info):
                self.dataframe_operations.append(call_info)

            # 检查是否是 RDD 操作
            if self._is_rdd_operation(call_info):
                self.rdd_operations.append(call_info)

            # 检查是否是配置相关
            if self._is_configuration_call(call_info):
                self.configurations.append(call_info)

            # 检查是否使用了弃用的 API
            deprecated_info = self._check_deprecated(call_info)
            if deprecated_info:
                self.deprecated_apis.append(deprecated_info)

        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        """处理赋值语句，跟踪 DataFrame 变量"""
        # 检查是否是 DataFrame 创建
        if isinstance(node.value, ast.Call):
            call_info = self._extract_call_info(node.value)
            if call_info and self._is_dataframe_creation(call_info):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        self._df_aliases.add(target.id)

        self.generic_visit(node)

    def _extract_call_info(self, node: ast.Call) -> Optional[Dict[str, Any]]:
        """提取函数调用信息"""
        func_name = self._get_func_name(node.func)
        if not func_name:
            return None

        # 提取参数
        args = []
        for arg in node.args:
            args.append(self._get_node_value(arg))

        # 提取关键字参数
        kwargs = {}
        for kw in node.keywords:
            if kw.arg:
                kwargs[kw.arg] = self._get_node_value(kw.value)

        return {
            "name": func_name,
            "args": args,
            "kwargs": kwargs,
            "line": node.lineno,
            "col": node.col_offset,
            "end_line": getattr(node, "end_lineno", node.lineno),
            "end_col": getattr(node, "end_col_offset", node.col_offset),
        }

    def _get_func_name(self, node: ast.expr) -> Optional[str]:
        """获取函数名称"""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            value_name = self._get_func_name(node.value)
            if value_name:
                return f"{value_name}.{node.attr}"
            return node.attr
        elif isinstance(node, ast.Call):
            # 链式调用
            return self._get_func_name(node.func)
        return None

    def _get_node_value(self, node: ast.expr) -> Any:
        """获取节点的值"""
        if isinstance(node, ast.Constant):
            return node.value
        elif isinstance(node, ast.Str):  # Python 3.7 兼容
            return node.s
        elif isinstance(node, ast.Num):  # Python 3.7 兼容
            return node.n
        elif isinstance(node, ast.Name):
            return f"<var:{node.id}>"
        elif isinstance(node, ast.Attribute):
            return f"<attr:{self._get_func_name(node)}>"
        elif isinstance(node, ast.Call):
            return f"<call:{self._get_func_name(node.func)}>"
        elif isinstance(node, ast.List):
            return [self._get_node_value(e) for e in node.elts]
        elif isinstance(node, ast.Dict):
            return {
                self._get_node_value(k): self._get_node_value(v)
                for k, v in zip(node.keys, node.values)
                if k is not None
            }
        return f"<{type(node).__name__}>"

    def _is_spark_module(self, module: str) -> bool:
        """检查是否是 Spark 相关模块"""
        if not module:
            return False
        return any(
            module == m or module.startswith(f"{m}.")
            for m in self.SPARK_MODULES
        )

    def _is_spark_session_call(self, call_info: Dict[str, Any]) -> bool:
        """检查是否是 SparkSession 相关调用"""
        name = call_info.get("name", "")
        spark_session_patterns = [
            "SparkSession", "builder", "getOrCreate", "appName",
            "master", "config", "enableHiveSupport",
        ]
        return any(p in name for p in spark_session_patterns)

    def _is_dataframe_operation(self, call_info: Dict[str, Any]) -> bool:
        """检查是否是 DataFrame 操作"""
        name = call_info.get("name", "")
        parts = name.split(".")
        method = parts[-1] if parts else ""
        return method in self.DATAFRAME_METHODS

    def _is_rdd_operation(self, call_info: Dict[str, Any]) -> bool:
        """检查是否是 RDD 操作"""
        name = call_info.get("name", "")
        # 检查是否包含 .rdd. 或以 rdd 相关方法结尾
        if ".rdd." in name:
            return True
        parts = name.split(".")
        method = parts[-1] if parts else ""
        # 只有在明确是 RDD 上下文时才标记
        if len(parts) >= 2 and parts[-2] == "rdd":
            return method in self.RDD_METHODS
        return False

    def _is_configuration_call(self, call_info: Dict[str, Any]) -> bool:
        """检查是否是配置相关调用"""
        name = call_info.get("name", "")
        config_patterns = [
            ".conf.set", ".conf.get", ".config(",
            "SparkConf", "setAppName", "setMaster",
        ]
        return any(p in name for p in config_patterns)

    def _is_dataframe_creation(self, call_info: Dict[str, Any]) -> bool:
        """检查是否是 DataFrame 创建操作"""
        name = call_info.get("name", "")
        creation_patterns = [
            ".read.", ".createDataFrame", ".sql(",
            ".table(", ".range(",
        ]
        return any(p in name for p in creation_patterns)

    def _check_deprecated(self, call_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """检查是否使用了弃用的 API"""
        name = call_info.get("name", "")

        for deprecated, replacement in self.DEPRECATED_APIS.items():
            if deprecated in name:
                return {
                    **call_info,
                    "deprecated_api": deprecated,
                    "replacement": replacement,
                    "severity": "warning",
                }

        return None


class SparkParser:
    """Spark 代码解析器"""

    def parse_file(self, file_path: str) -> SparkCodeStructure:
        """
        解析 Python 文件，返回 Spark 代码结构
        
        Args:
            file_path: 文件路径
            
        Returns:
            SparkCodeStructure 对象
        """
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")

        if not path.suffix == ".py":
            raise ValueError(f"不是 Python 文件: {file_path}")

        with open(path, "r", encoding="utf-8") as f:
            code = f.read()

        return self.parse_code(code, file_path)

    def parse_code(self, code: str, file_path: str = "<string>") -> SparkCodeStructure:
        """
        解析代码字符串
        
        Args:
            code: Python 代码字符串
            file_path: 文件路径（用于错误报告）
            
        Returns:
            SparkCodeStructure 对象
        """
        try:
            tree = ast.parse(code, filename=file_path)
        except SyntaxError as e:
            raise SyntaxError(f"代码解析错误 ({file_path}): {e}")

        visitor = SparkASTVisitor()
        visitor.visit(tree)

        return SparkCodeStructure(
            file_path=file_path,
            imports=visitor.imports,
            spark_session_usage=visitor.spark_session_usage,
            dataframe_operations=visitor.dataframe_operations,
            rdd_operations=visitor.rdd_operations,
            configurations=visitor.configurations,
            deprecated_apis=visitor.deprecated_apis,
            function_calls=visitor.function_calls,
            raw_code=code,
            ast_tree=tree,
        )

    def get_spark_imports(self, structure: SparkCodeStructure) -> List[Dict[str, Any]]:
        """获取所有 Spark 相关的导入"""
        return [imp for imp in structure.imports if imp.get("is_spark")]

    def get_deprecated_usage(self, structure: SparkCodeStructure) -> List[Dict[str, Any]]:
        """获取所有弃用 API 的使用"""
        return structure.deprecated_apis

    def has_spark_code(self, structure: SparkCodeStructure) -> bool:
        """检查代码是否包含 Spark 相关代码"""
        return (
            any(imp.get("is_spark") for imp in structure.imports)
            or len(structure.spark_session_usage) > 0
            or len(structure.dataframe_operations) > 0
            or len(structure.rdd_operations) > 0
        )
