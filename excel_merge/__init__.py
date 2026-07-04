"""excel_merge - Excel 文件合并工具（分层包）。

分层结构:
  - config: dataclass 配置模型，负责加载/校验 YAML
  - core:   纯函数库层，用 pandas 按表头名对齐合并
  - cli:    命令行入口，负责 argparse、logging 与退出码

对外导出配置模型、核心函数与自定义异常，便于作为库复用。
"""

from .config import MergeConfig, SheetConfig, load_config_file
from .core import (
    SOURCE_COLUMN,
    collect_input_files,
    merge_sheet,
    merge_sheets,
    read_sheet,
    write_output,
)
from .exceptions import (
    ConfigError,
    CorruptFileError,
    ExcelMergeError,
    SheetNotFoundError,
)

__all__ = [
    "MergeConfig",
    "SheetConfig",
    "load_config_file",
    "SOURCE_COLUMN",
    "collect_input_files",
    "read_sheet",
    "merge_sheet",
    "merge_sheets",
    "write_output",
    "ExcelMergeError",
    "ConfigError",
    "SheetNotFoundError",
    "CorruptFileError",
]

__version__ = "1.0.0"
