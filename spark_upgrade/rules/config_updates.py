"""配置更新规则 - Spark 2.x 到 3.x 的配置变更"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class ConfigUpdate:
    """配置更新定义"""
    old_config: str
    new_config: str
    version: str
    description: str
    default_value: Optional[str] = None
    auto_fixable: bool = True
    category: str = "general"
    notes: Optional[str] = None


# SQL 相关配置
SQL_CONFIG_UPDATES: List[ConfigUpdate] = [
    ConfigUpdate(
        old_config="spark.sql.shuffle.partitions",
        new_config="spark.sql.adaptive.enabled",
        version="3.0",
        description="推荐启用自适应查询执行 (AQE) 替代固定分区数",
        default_value="true",
        auto_fixable=False,
        category="sql",
        notes="AQE 会自动调整 shuffle 分区数",
    ),
    ConfigUpdate(
        old_config="spark.sql.execution.arrow.enabled",
        new_config="spark.sql.execution.arrow.pyspark.enabled",
        version="3.0",
        description="Arrow 优化配置名称变更",
        default_value="true",
        category="sql",
    ),
    ConfigUpdate(
        old_config="spark.sql.execution.arrow.fallback.enabled",
        new_config="spark.sql.execution.arrow.pyspark.fallback.enabled",
        version="3.0",
        description="Arrow 回退配置名称变更",
        default_value="true",
        category="sql",
    ),
    ConfigUpdate(
        old_config="spark.sql.hive.convertMetastoreParquet",
        new_config="spark.sql.hive.convertMetastoreParquet",
        version="3.0",
        description="Hive Parquet 转换配置",
        auto_fixable=False,
        category="sql",
        notes="Spark 3.x 中默认行为可能不同",
    ),
]

# 性能相关配置
PERFORMANCE_CONFIG_UPDATES: List[ConfigUpdate] = [
    ConfigUpdate(
        old_config="spark.sql.adaptive.enabled",
        new_config="spark.sql.adaptive.enabled",
        version="3.0",
        description="自适应查询执行 (AQE)",
        default_value="true",
        category="performance",
        notes="Spark 3.0+ 中默认启用",
    ),
    ConfigUpdate(
        old_config="spark.sql.adaptive.coalescePartitions.enabled",
        new_config="spark.sql.adaptive.coalescePartitions.enabled",
        version="3.0",
        description="AQE 自动合并分区",
        default_value="true",
        category="performance",
    ),
    ConfigUpdate(
        old_config="spark.sql.adaptive.skewJoin.enabled",
        new_config="spark.sql.adaptive.skewJoin.enabled",
        version="3.0",
        description="AQE 倾斜 Join 优化",
        default_value="true",
        category="performance",
    ),
    ConfigUpdate(
        old_config="spark.sql.autoBroadcastJoinThreshold",
        new_config="spark.sql.autoBroadcastJoinThreshold",
        version="3.0",
        description="广播 Join 阈值",
        default_value="10485760",
        auto_fixable=False,
        category="performance",
        notes="Spark 3.x 中默认值可能不同",
    ),
]

# 内存相关配置
MEMORY_CONFIG_UPDATES: List[ConfigUpdate] = [
    ConfigUpdate(
        old_config="spark.memory.fraction",
        new_config="spark.memory.fraction",
        version="3.0",
        description="内存分配比例",
        default_value="0.6",
        auto_fixable=False,
        category="memory",
    ),
    ConfigUpdate(
        old_config="spark.memory.storageFraction",
        new_config="spark.memory.storageFraction",
        version="3.0",
        description="存储内存比例",
        default_value="0.5",
        auto_fixable=False,
        category="memory",
    ),
]

# 序列化相关配置
SERIALIZATION_CONFIG_UPDATES: List[ConfigUpdate] = [
    ConfigUpdate(
        old_config="spark.serializer",
        new_config="spark.serializer",
        version="3.0",
        description="序列化器配置",
        default_value="org.apache.spark.serializer.KryoSerializer",
        auto_fixable=False,
        category="serialization",
        notes="推荐使用 Kryo 序列化",
    ),
    ConfigUpdate(
        old_config="spark.kryo.unsafe",
        new_config="spark.kryo.unsafe",
        version="3.0",
        description="Kryo unsafe 模式",
        default_value="false",
        auto_fixable=False,
        category="serialization",
    ),
]

# 弃用的配置
DEPRECATED_CONFIG_UPDATES: List[ConfigUpdate] = [
    ConfigUpdate(
        old_config="spark.yarn.executor.memoryOverhead",
        new_config="spark.executor.memoryOverhead",
        version="3.0",
        description="YARN 内存开销配置名称变更",
        category="deprecated",
    ),
    ConfigUpdate(
        old_config="spark.yarn.driver.memoryOverhead",
        new_config="spark.driver.memoryOverhead",
        version="3.0",
        description="YARN 驱动内存开销配置名称变更",
        category="deprecated",
    ),
    ConfigUpdate(
        old_config="spark.yarn.am.memoryOverhead",
        new_config="spark.yarn.am.memoryOverhead",
        version="3.0",
        description="YARN AM 内存开销配置",
        auto_fixable=False,
        category="deprecated",
    ),
]

# 新增的推荐配置 (Spark 3.x)
NEW_RECOMMENDED_CONFIGS: List[ConfigUpdate] = [
    ConfigUpdate(
        old_config="",
        new_config="spark.sql.adaptive.enabled",
        version="3.0",
        description="启用自适应查询执行",
        default_value="true",
        auto_fixable=False,
        category="new",
    ),
    ConfigUpdate(
        old_config="",
        new_config="spark.sql.adaptive.coalescePartitions.enabled",
        version="3.0",
        description="启用自动分区合并",
        default_value="true",
        auto_fixable=False,
        category="new",
    ),
    ConfigUpdate(
        old_config="",
        new_config="spark.sql.adaptive.skewJoin.enabled",
        version="3.0",
        description="启用倾斜 Join 优化",
        default_value="true",
        auto_fixable=False,
        category="new",
    ),
    ConfigUpdate(
        old_config="",
        new_config="spark.sql.adaptive.localShuffleReader.enabled",
        version="3.0",
        description="启用本地 Shuffle 读取",
        default_value="true",
        auto_fixable=False,
        category="new",
    ),
]

# 汇总所有配置更新
CONFIG_UPDATES: List[ConfigUpdate] = (
    SQL_CONFIG_UPDATES
    + PERFORMANCE_CONFIG_UPDATES
    + MEMORY_CONFIG_UPDATES
    + SERIALIZATION_CONFIG_UPDATES
    + DEPRECATED_CONFIG_UPDATES
)


def get_updates_by_category(category: str) -> List[ConfigUpdate]:
    """按类别获取配置更新"""
    return [c for c in CONFIG_UPDATES if c.category == category]


def get_deprecated_configs() -> List[ConfigUpdate]:
    """获取弃用的配置"""
    return [c for c in CONFIG_UPDATES if c.category == "deprecated"]


def get_recommended_configs() -> List[ConfigUpdate]:
    """获取推荐的新配置"""
    return NEW_RECOMMENDED_CONFIGS
