"""配置模型与 YAML 加载/校验。

用 dataclass 定义 ``SheetConfig`` 与顶层 ``MergeConfig``，负责:
  - 从 YAML 文件加载原始配置（``load_config_file``）
  - 将原始字典构建为强类型配置并校验（``MergeConfig.from_dict``）

校验失败一律抛出 :class:`ConfigError`，不调用 print / sys.exit。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .exceptions import ConfigError


@dataclass
class SheetConfig:
    """单个 sheet 的合并配置。

    Attributes:
        name: 源文件中的 sheet 名称（必填）。
        output_name: 输出文件中的 sheet 名称，默认同 ``name``。
        header_rows: 表头行数，支持多级表头；0 表示无表头。默认 1。
        data_start_row: 数据起始行（1 基）。为 ``None`` 时默认为
            ``header_rows + 1``。当表头与数据之间存在空行/说明行时可显式指定。
    """

    name: str
    output_name: str | None = None
    header_rows: int = 1
    data_start_row: int | None = None

    def __post_init__(self) -> None:
        if not self.name or not str(self.name).strip():
            raise ConfigError("sheet 配置缺少 name（源 sheet 名称）")
        self.name = str(self.name)

        if self.output_name is None or str(self.output_name).strip() == "":
            self.output_name = self.name
        else:
            self.output_name = str(self.output_name)

        try:
            self.header_rows = int(self.header_rows)
        except (TypeError, ValueError):
            raise ConfigError(
                f"sheet '{self.name}' 的 header_rows 必须是整数，"
                f"当前为: {self.header_rows!r}"
            )
        if self.header_rows < 0:
            raise ConfigError(
                f"sheet '{self.name}' 的 header_rows 不能为负数: {self.header_rows}"
            )

        if self.data_start_row is not None:
            try:
                self.data_start_row = int(self.data_start_row)
            except (TypeError, ValueError):
                raise ConfigError(
                    f"sheet '{self.name}' 的 data_start_row 必须是整数，"
                    f"当前为: {self.data_start_row!r}"
                )
            if self.data_start_row < 1:
                raise ConfigError(
                    f"sheet '{self.name}' 的 data_start_row 必须 >= 1，"
                    f"当前为: {self.data_start_row}"
                )
            # 校验数据起始行不得与表头行重叠
            if self.data_start_row <= self.header_rows:
                raise ConfigError(
                    f"sheet '{self.name}' 的 data_start_row({self.data_start_row}) "
                    f"与表头行重叠：必须大于 header_rows({self.header_rows})，"
                    f"即数据需从第 {self.header_rows + 1} 行或更靠后开始"
                )

    @property
    def effective_data_start_row(self) -> int:
        """实际数据起始行（1 基），未显式配置时为 ``header_rows + 1``。"""
        if self.data_start_row is not None:
            return self.data_start_row
        return self.header_rows + 1

    @classmethod
    def from_value(cls, value: Any) -> SheetConfig:
        """从字符串或字典构建 SheetConfig（向后兼容 ``-s`` 简写）。"""
        if isinstance(value, str):
            return cls(name=value)
        if isinstance(value, dict):
            allowed = {"name", "output_name", "header_rows", "data_start_row"}
            unknown = set(value) - allowed
            if unknown:
                raise ConfigError(
                    f"sheet 配置存在未知字段: {', '.join(sorted(unknown))}"
                )
            return cls(
                name=value.get("name"),
                output_name=value.get("output_name"),
                header_rows=value.get("header_rows", 1),
                data_start_row=value.get("data_start_row"),
            )
        raise ConfigError(f"无法识别的 sheet 配置项: {value!r}")


@dataclass
class MergeConfig:
    """顶层合并配置。"""

    sheets: list[SheetConfig]
    directory: str | None = None
    pattern: str | None = None
    recursive: bool = False
    files: list[str] = field(default_factory=list)
    output: str = "merged.xlsx"
    base_dir: Path = field(default_factory=Path.cwd)

    def __post_init__(self) -> None:
        self.base_dir = Path(self.base_dir)
        if not self.sheets:
            raise ConfigError("未配置需要合并的 sheet（config.sheets 或 -s 参数）")

    @classmethod
    def from_dict(
        cls, data: dict[str, Any], base_dir: Path | None = None
    ) -> MergeConfig:
        """从原始配置字典构建并校验 MergeConfig。"""
        if data is None:
            data = {}
        if not isinstance(data, dict):
            raise ConfigError("配置内容必须是键值映射（YAML 顶层为字典）")

        raw_sheets = data.get("sheets") or []
        if not isinstance(raw_sheets, list):
            raise ConfigError("config.sheets 必须是列表")
        sheets = [SheetConfig.from_value(item) for item in raw_sheets]

        files = data.get("files") or []
        if isinstance(files, str):
            files = [files]
        if not isinstance(files, list):
            raise ConfigError("config.files 必须是列表")

        return cls(
            sheets=sheets,
            directory=data.get("directory"),
            pattern=data.get("pattern"),
            recursive=bool(data.get("recursive", False)),
            files=[str(f) for f in files],
            output=str(data.get("output", "merged.xlsx")),
            base_dir=Path(base_dir) if base_dir is not None else Path.cwd(),
        )


def load_config_file(config_path: str) -> dict[str, Any]:
    """加载 YAML 配置文件，返回原始字典。

    Raises:
        ConfigError: 文件不存在或 YAML 解析失败。
    """
    path = Path(config_path)
    if not path.exists():
        raise ConfigError(f"配置文件不存在: {config_path}")
    try:
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        raise ConfigError(
            f"配置文件 YAML 解析失败: {config_path}\n  {e}\n"
            "提示: Windows 路径请勿放在双引号中（反斜杠会被当作转义符）。\n"
            "      可改用单引号 'D:\\dir'、正斜杠 \"D:/dir\" 或双反斜杠 \"D:\\\\dir\"。"
        )
