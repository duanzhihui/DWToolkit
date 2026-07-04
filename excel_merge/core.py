"""纯函数库层：用 pandas 按表头名对齐合并 Excel 数据。

本层不做任何 print / sys.exit，只在必要时 raise 自定义异常并通过标准
``logging`` 输出可跳过的告警。核心行为:

  - 用 ``pd.read_excel`` + ``pd.concat`` 按 **表头名** 对齐合并（而非列位置）。
  - 支持多级表头（``header=[0, 1, ...]``）与 ``data_start_row``（用 ``skiprows``）。
  - 为每行加 :data:`SOURCE_COLUMN` 溯源列，记录来源文件名。
  - 逐文件 try/except 做异常隔离：跳过损坏/加密/锁定的文件并收集失败清单。

关于 data_only：pandas 的 openpyxl 读取器以 ``data_only=True`` 打开工作簿，
因此读取到的是公式的缓存值而非公式文本，等价于原实现的 data_only 行为。
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import List, Tuple

import pandas as pd

from .config import MergeConfig, SheetConfig
from .exceptions import ConfigError, CorruptFileError, SheetNotFoundError

logger = logging.getLogger(__name__)

# 溯源列名：合并结果中每行标注其来源文件
SOURCE_COLUMN = "__source_file__"

# Excel 临时文件前缀（Office 打开文件时生成的锁文件）
_TEMP_PREFIX = "~$"

# Excel sheet 名称长度上限
_MAX_SHEET_NAME_LEN = 31

# 失败清单条目：(文件路径, 失败原因)
FailedFile = Tuple[Path, str]

# output_name -> DataFrame，保持配置顺序
OrderedSheets = List[Tuple[str, pd.DataFrame]]


def collect_input_files(
    files: list[str] | None,
    directory: str | None,
    pattern: str | None,
    recursive: bool,
    base_dir: Path,
) -> list[Path]:
    """收集参与合并的 Excel 文件。

    Args:
        files: 显式指定的文件列表（相对 base_dir 或绝对路径）。
        directory: 扫描目录。
        pattern: 文件名正则表达式（匹配文件名，不含路径）。
        recursive: 是否递归扫描子目录。
        base_dir: 相对路径的基准目录。

    Returns:
        去重后的文件路径列表（显式文件在前，目录扫描结果按名称排序在后）。

    Raises:
        ConfigError: ``pattern`` 不是合法正则表达式。
    """
    base_dir = Path(base_dir)
    result: list[Path] = []
    seen = set()

    def _add(p: Path) -> None:
        rp = p.resolve()
        if rp in seen:
            return
        if rp.name.startswith(_TEMP_PREFIX):
            return
        seen.add(rp)
        result.append(p)

    # 1. 显式文件列表
    for f in files or []:
        fp = Path(f)
        if not fp.is_absolute():
            fp = base_dir / fp
        if fp.exists():
            _add(fp)
        else:
            logger.warning("指定文件不存在，已跳过: %s", fp)

    # 2. 目录扫描 + 正则匹配
    if directory:
        dir_path = Path(directory)
        if not dir_path.is_absolute():
            dir_path = base_dir / dir_path
        if not dir_path.is_dir():
            logger.warning("扫描目录不存在，已跳过: %s", dir_path)
        else:
            regex = None
            if pattern:
                try:
                    regex = re.compile(pattern)
                except re.error as e:
                    raise ConfigError(
                        f"文件名正则表达式无效: {pattern!r}\n  {e}\n"
                        "提示: pattern 使用正则语法，非通配符。\n"
                        r'      匹配全部 xlsx: "\.xlsx$"；'
                        r'以"销售"开头: "^销售.*\.xlsx$"'
                    )
            globber = dir_path.rglob("*") if recursive else dir_path.glob("*")
            candidates = sorted(
                (
                    p
                    for p in globber
                    if p.is_file() and p.suffix.lower() in (".xlsx", ".xlsm")
                ),
                key=lambda p: str(p).lower(),
            )
            for p in candidates:
                if regex is None or regex.search(p.name):
                    _add(p)

    return result


def _header_param(header_rows: int):
    """将表头行数转换为 pandas ``header`` 参数。"""
    if header_rows <= 0:
        return None
    if header_rows == 1:
        return 0
    return list(range(header_rows))


def _skiprows(header_rows: int, data_start_row: int) -> list[int] | None:
    """计算表头与数据之间需跳过的空行/说明行（0 基物理行号）。

    表头位于物理行 ``0 .. header_rows-1``；数据从 ``data_start_row``（1 基）开始，
    即 0 基行号 ``data_start_row - 1``。二者之间的行需要跳过。
    """
    gap_start = header_rows
    gap_end = data_start_row - 1  # 数据起始行的 0 基行号（不含）
    if gap_end > gap_start:
        return list(range(gap_start, gap_end))
    return None


def _source_key(columns: pd.Index):
    """溯源列的列键：多级表头下需用与层数匹配的元组键。"""
    if isinstance(columns, pd.MultiIndex):
        return (SOURCE_COLUMN,) + ("",) * (columns.nlevels - 1)
    return SOURCE_COLUMN


def read_sheet(file_path: Path, sheet_cfg: SheetConfig) -> pd.DataFrame:
    """读取单个文件中指定 sheet 的数据，返回带溯源列的 DataFrame。

    表头按名称保留，数据行去除全空行，并追加 :data:`SOURCE_COLUMN` 列。

    Raises:
        SheetNotFoundError: 文件中不存在目标 sheet（可跳过的正常情况）。
        CorruptFileError: 文件损坏、加密或被占用，无法读取。
    """
    file_path = Path(file_path)
    header = _header_param(sheet_cfg.header_rows)
    skiprows = _skiprows(sheet_cfg.header_rows, sheet_cfg.effective_data_start_row)

    try:
        excel = pd.ExcelFile(file_path, engine="openpyxl")
    except Exception as e:  # 打开失败：损坏/加密/锁定
        raise CorruptFileError(f"无法打开文件（可能损坏/加密/被占用）: {file_path}\n  {e}")

    try:
        with excel:
            if sheet_cfg.name not in excel.sheet_names:
                raise SheetNotFoundError(
                    f"文件中未找到 sheet '{sheet_cfg.name}': {file_path}"
                )
            df = pd.read_excel(
                excel,
                sheet_name=sheet_cfg.name,
                header=header,
                skiprows=skiprows,
            )
    except SheetNotFoundError:
        raise
    except Exception as e:  # 读取阶段失败
        raise CorruptFileError(f"读取 sheet '{sheet_cfg.name}' 失败: {file_path}\n  {e}")

    # 过滤完全空白的数据行（在追加溯源列之前，避免溯源列干扰空行判断）
    df = df.dropna(axis=0, how="all")
    df = df.copy()
    df[_source_key(df.columns)] = file_path.name
    return df


def merge_sheet(
    input_files: list[Path],
    sheet_cfg: SheetConfig,
) -> tuple[pd.DataFrame, list[FailedFile]]:
    """合并所有文件中某个 sheet 的数据（按表头名对齐）。

    Returns:
        ``(merged_df, failed_files)``：合并后的 DataFrame（含溯源列），以及
        本次读取失败的文件清单 ``(路径, 原因)``。缺少该 sheet 的文件按正常
        跳过处理，不计入失败清单。
    """
    frames: list[pd.DataFrame] = []
    failed: list[FailedFile] = []

    for fp in input_files:
        try:
            df = read_sheet(fp, sheet_cfg)
        except SheetNotFoundError:
            logger.info("  - %s: 未找到 sheet '%s'，已跳过", fp.name, sheet_cfg.name)
            continue
        except CorruptFileError as e:
            logger.warning("  - %s: 读取失败，已跳过。%s", fp.name, e)
            failed.append((fp, str(e)))
            continue
        logger.info("  - %s: 合并 %d 行", fp.name, len(df))
        frames.append(df)

    if not frames:
        logger.warning("没有文件包含 sheet '%s'", sheet_cfg.name)
        merged = pd.DataFrame()
    else:
        # pd.concat 按列名（含多级表头元组）取并集对齐，缺列以 NaN 填充
        merged = pd.concat(frames, ignore_index=True)

    return merged, failed


def merge_sheets(
    input_files: list[Path],
    config: MergeConfig,
) -> tuple[OrderedSheets, list[FailedFile]]:
    """按配置合并所有 sheet。

    Returns:
        ``(sheets, failed_files)``：``sheets`` 为 ``output_name -> DataFrame``
        的有序列表（保持配置顺序），``failed_files`` 为去重后的失败清单。
    """
    sheets: OrderedSheets = []
    failed: list[FailedFile] = []
    seen_failed = set()

    for sheet_cfg in config.sheets:
        logger.info("合并 sheet: %s", sheet_cfg.name)
        df, sheet_failed = merge_sheet(input_files, sheet_cfg)
        sheets.append((sheet_cfg.output_name, df))
        for fp, reason in sheet_failed:
            key = str(fp.resolve())
            if key not in seen_failed:
                seen_failed.add(key)
                failed.append((fp, reason))

    return sheets, failed


def _dedupe_sheet_name(name: str, used: set) -> str:
    """将 sheet 名截断到 31 字符后检测重名并加后缀避免冲突。"""
    base = name[:_MAX_SHEET_NAME_LEN]
    candidate = base
    i = 1
    while candidate in used:
        suffix = f"_{i}"
        candidate = base[: _MAX_SHEET_NAME_LEN - len(suffix)] + suffix
        i += 1
    used.add(candidate)
    return candidate


def write_output(output_path: Path, sheets: OrderedSheets) -> None:
    """将合并结果写入输出 Excel 文件。

    sheet 名截断到 31 字符后自动去重加后缀；多级表头会写为多行表头。
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    used_names: set = set()
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        wrote_any = False
        for output_name, df in sheets:
            sheet_name = _dedupe_sheet_name(str(output_name), used_names)
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            wrote_any = True
        if not wrote_any:
            # pandas 不允许写入零个 sheet，补一个空 sheet 以生成合法文件
            pd.DataFrame().to_excel(writer, sheet_name="Sheet1", index=False)
