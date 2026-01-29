"""配置文件加载和保存"""

from pathlib import Path
from typing import Optional, Union

import yaml

from .defaults import SparkUpgradeConfig


DEFAULT_CONFIG_NAMES = [
    ".spark_upgrade.yml",
    ".spark_upgrade.yaml",
    "spark_upgrade.yml",
    "spark_upgrade.yaml",
]


def find_config_file(start_path: Optional[Union[str, Path]] = None) -> Optional[Path]:
    """
    查找配置文件
    
    从指定路径向上查找配置文件，直到找到或到达根目录
    
    Args:
        start_path: 开始查找的路径，默认为当前目录
        
    Returns:
        配置文件路径，如果未找到则返回 None
    """
    if start_path is None:
        start_path = Path.cwd()
    else:
        start_path = Path(start_path)

    current = start_path.resolve()

    while current != current.parent:
        for config_name in DEFAULT_CONFIG_NAMES:
            config_path = current / config_name
            if config_path.exists():
                return config_path
        current = current.parent

    # 检查根目录
    for config_name in DEFAULT_CONFIG_NAMES:
        config_path = current / config_name
        if config_path.exists():
            return config_path

    return None


def load_config(
    config_path: Optional[Union[str, Path]] = None
) -> SparkUpgradeConfig:
    """
    加载配置文件
    
    Args:
        config_path: 配置文件路径，如果为 None 则自动查找
        
    Returns:
        SparkUpgradeConfig 配置对象
    """
    if config_path is None:
        config_path = find_config_file()

    if config_path is None:
        return SparkUpgradeConfig()

    config_path = Path(config_path)

    if not config_path.exists():
        return SparkUpgradeConfig()

    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if data is None:
        return SparkUpgradeConfig()

    return SparkUpgradeConfig.from_dict(data)


def save_config(
    config: SparkUpgradeConfig,
    config_path: Union[str, Path],
) -> None:
    """
    保存配置到文件
    
    Args:
        config: 配置对象
        config_path: 保存路径
    """
    config_path = Path(config_path)
    config_path.parent.mkdir(parents=True, exist_ok=True)

    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(
            config.to_dict(),
            f,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )
