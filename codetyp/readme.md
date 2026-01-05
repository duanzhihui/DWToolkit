# 代码分类程序 (codetyp)

识别大数据代码类型（Hive, Spark, Flink等）的自动化工具。

## 功能特性

- **代码类型识别**: 支持 Hive、Spark、Flink、Presto、Trino、Kafka 等大数据框架
- **积分制分类**: 为每条正则规则配置积分(score)，按匹配累加积分，仅选择积分最高的一个类型
- **正则表达式匹配**: 使用可配置的正则规则进行代码分类，支持按类型自定义 pattern 与 score
- **隐藏调用识别**: 支持识别通过函数封装、动态执行等方式的隐藏调用
- **文件过滤**: 支持包含/排除规则，使用Unix通配符格式
- **Excel输出**: 结果导出为Excel文件，包含文件目录、文件名、代码类型、依据（含积分明细）
- **YAML配置**: 所有参数可通过配置文件管理
- **命令行支持**: 支持命令行参数输入
- **模块化设计**: 可作为Python模块导入使用

## 安装依赖

```bash
pip install pyyaml pandas openpyxl
```

## 快速开始

### 命令行使用

```bash
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

# 不递归扫描子目录
python codetyp.py -d ./code --no-recursive

# 显示详细信息
python codetyp.py -d ./code -v
```

### 作为模块使用

```python
from codetyp import CodeClassifier

# 使用默认规则
classifier = CodeClassifier()

# 或使用配置文件
classifier = CodeClassifier("config.yml")

# 分类单个文件
result = classifier.classify_file("path/to/file.py")
print(f"类型: {result.code_type}, 依据: {result.evidence}")

# 分类目录
results = classifier.classify_directory("path/to/code", recursive=True)

# 分类文件列表
results = classifier.classify_files(["file1.py", "file2.sql", "dir/"])

# 导出到Excel
classifier.export_to_excel(results, "output.xlsx")

# 直接分类代码内容（查看各类型积分和依据）
content = """
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
"""
matches = classifier.classify_content(content)
for code_type, score, evidence in matches:
    print(f"{code_type} ({score}分): {evidence}")
```

## 命令行参数

| 参数 | 简写 | 说明 | 默认值 |
|------|------|------|--------|
| `--directory` | `-d` | 要扫描的目录路径 | - |
| `--files` | `-f` | 要分类的文件路径列表 | - |
| `--config` | `-c` | 配置文件路径 (YAML格式) | - |
| `--output` | `-o` | 输出Excel文件路径 | `classification_result.xlsx` |
| `--include` | - | 包含的文件模式 (Unix通配符) | - |
| `--exclude` | - | 排除的文件模式 (Unix通配符) | - |
| `--recursive` | `-r` | 递归扫描子目录 | `True` |
| `--no-recursive` | - | 不递归扫描子目录 | - |
| `--verbose` | `-v` | 显示详细信息 | `False` |

## 配置文件说明

配置文件使用YAML格式，主要包含以下部分：

### 扫描扩展名

```yaml
scan_extensions:
  - ".py"
  - ".sql"
  - ".scala"
  - ".java"
  - ".sh"
  - ".hql"
```

### 文件过滤规则

```yaml
file_filter:
  include:
    - "*.py"
    - "*.sql"
  exclude:
    - "*_test.py"
    - "__pycache__/*"
```

### 输出配置

```yaml
output:
  path: "classification_result.xlsx"
```

### 分类规则（积分制 + 互斥规则）

```yaml
classification_rules:
  hive:
    description: "Hive SQL及相关调用"
    priority: 10
    # 只保留Hive独有、能区分其他类型的互斥规则
    # 每条规则带有 score，用于积分累加
    patterns:
      - {pattern: "\\bPARTITIONED\\s+BY\\b", score: 10}
      - {pattern: "\\bSTORED\\s+AS\\s+(ORC|PARQUET|TEXTFILE|AVRO|SEQUENCEFILE)\\b", score: 10}
      - {pattern: "\\bMSCK\\s+REPAIR\\s+TABLE\\b", score: 15}
      - {pattern: "beeline\\s+-e", score: 15}
      - {pattern: "from\\s+pyhive\\s+import\\s+hive", score: 15}
```

## 支持的代码类型

### Hive
- 直接SQL: `CREATE TABLE`, `PARTITIONED BY`, `STORED AS`, `INSERT INTO TABLE`
- Hive函数: `get_json_object`, `lateral view explode`, `collect_set`
- 隐藏调用: `pyhive`, `beeline -e`, `hive -e`, `HiveContext`

### Spark
- SparkSession/SparkContext: `SparkSession.builder`, `SparkContext`
- DataFrame API: `.read.parquet()`, `.write.csv()`, `.filter()`, `.groupBy()`
- RDD API: `.parallelize()`, `.map()`, `.reduceByKey()`
- Spark SQL: `spark.sql()`, `.createOrReplaceTempView()`
- 隐藏调用: `spark-submit`, `findspark.init`

### Flink
- 执行环境: `StreamExecutionEnvironment`, `TableEnvironment`
- DataStream API: `.addSource()`, `.keyBy()`, `.window()`
- Table API: `.executeSql()`, `.sqlQuery()`
- Flink SQL: `TUMBLE()`, `HOP()`, `SESSION()`, `WATERMARK FOR`
- 隐藏调用: `flink run`, `FlinkKafkaConsumer`

### 其他
- **Presto**: `prestodb`, `presto-cli`
- **Trino**: `trino://`, `TrinoClient`
- **Kafka**: `KafkaProducer`, `KafkaConsumer`

## 输出格式

Excel输出包含以下列：

| 列名 | 说明 |
|------|------|
| 文件目录 | 文件所在目录的绝对路径 |
| 文件名 | 文件名称 |
| 代码类型 | 识别的代码类型，仅保留积分最高的一个类型 |
| 代码类型依据 | 各类型积分与证据，按积分降序，格式为 `类型(总分): 证据(+单条得分); ...` |

## 示例代码

`code/` 目录包含以下示例：

- `hive_example.sql` - Hive SQL示例
- `hive_python_example.py` - Python调用Hive示例
- `spark_example.py` - PySpark示例
- `spark_submit_example.sh` - Spark Submit脚本示例
- `flink_example.py` - PyFlink示例
- `flink_java_example.java` - Flink Java示例
- `mixed_example.py` - Spark+Hive混合示例
- `hidden_call_example.py` - 隐藏调用方式示例

## 扩展分类规则

可以通过修改 `config.yml` 添加自定义分类规则（同样支持积分配置）：

```yaml
classification_rules:
  custom_type:
    description: "自定义类型描述"
    priority: 15
    patterns:
      # 每条规则包含 pattern 与 score
      - {pattern: "pattern1", score: 10}
      - {pattern: "pattern2", score: 5}
```

优先级数值越大，在结果中排序越靠前。

## API参考

### CodeClassifier

```python
class CodeClassifier:
    def __init__(self, config_path: Optional[str] = None)
    def load_config(self, config_path: str)
    def classify_content(self, content: str) -> List[Tuple[str, int, str]]
    def classify_file(self, file_path: str) -> Optional[ClassificationResult]
    def classify_directory(self, directory: str, recursive: bool = True) -> List[ClassificationResult]
    def classify_files(self, file_paths: List[str]) -> List[ClassificationResult]
    def export_to_excel(self, results: List[ClassificationResult], output_path: Optional[str] = None)
```

### ClassificationResult

```python
@dataclass
class ClassificationResult:
    file_dir: str      # 文件目录
    file_name: str     # 文件名
    code_type: str     # 代码类型
    evidence: str      # 匹配依据
```

### FileFilter

```python
@dataclass
class FileFilter:
    include_patterns: List[str]  # 包含模式
    exclude_patterns: List[str]  # 排除模式
    
    def match(self, file_path: str) -> bool
```

## 许可证

MIT License
