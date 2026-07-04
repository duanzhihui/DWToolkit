"""命令行入口层。

负责 argparse 参数解析、日志配置（-v/-q）、命令行覆盖配置文件的逻辑，
并捕获 core/config 抛出的自定义异常决定退出码。这是唯一允许调用
``sys.exit`` 的层。所有面向用户的信息通过标准 ``logging`` 输出。
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

from . import __version__
from .config import MergeConfig, load_config_file
from .core import collect_input_files, merge_sheets, write_output
from .exceptions import ExcelMergeError

logger = logging.getLogger("excel_merge")

# 退出码
EXIT_OK = 0
EXIT_ERROR = 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="excel-merge",
        description="excel_merge - Excel 文件合并工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  excel-merge config.yml
  excel-merge -c config.yml
  excel-merge -d ./data -p "^销售.*\\.xlsx$" -s Sheet1 -o merged.xlsx
        """,
    )
    parser.add_argument("config", nargs="?", help="YAML 配置文件路径")
    parser.add_argument(
        "-c", "--config-file", dest="config_file", help="YAML 配置文件路径（可选方式）"
    )
    parser.add_argument("-d", "--directory", help="扫描目录")
    parser.add_argument("-p", "--pattern", help="文件名正则表达式")
    parser.add_argument("-f", "--files", nargs="*", help="显式指定的文件列表")
    parser.add_argument("-s", "--sheets", nargs="*", help="需要合并的 sheet 名称列表")
    parser.add_argument("-o", "--output", help="输出文件路径")
    parser.add_argument(
        "-r", "--recursive", action="store_true", help="递归扫描子目录"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="输出更详细的日志（DEBUG）"
    )
    parser.add_argument(
        "-q", "--quiet", action="store_true", help="仅输出告警与错误（WARNING）"
    )
    parser.add_argument(
        "--version", action="version", version=f"excel-merge {__version__}"
    )
    return parser


def _setup_logging(verbose: bool, quiet: bool) -> None:
    if verbose:
        level = logging.DEBUG
    elif quiet:
        level = logging.WARNING
    else:
        level = logging.INFO
    logging.basicConfig(level=level, format="%(message)s")
    logging.getLogger().setLevel(level)


def build_config(args: argparse.Namespace) -> MergeConfig:
    """合并配置文件与命令行参数（命令行优先），构建并校验 MergeConfig。"""
    raw: dict[str, Any] = {}
    base_dir = Path.cwd()

    config_path = args.config or args.config_file
    if config_path:
        logger.info("加载配置文件: %s", config_path)
        raw = load_config_file(config_path)
        if not isinstance(raw, dict):
            raw = {}
        base_dir = Path(config_path).resolve().parent

    # 命令行参数覆盖
    if args.directory is not None:
        raw["directory"] = args.directory
    if args.pattern is not None:
        raw["pattern"] = args.pattern
    if args.files:
        raw["files"] = args.files
    if args.output is not None:
        raw["output"] = args.output
    if args.recursive:
        raw["recursive"] = True
    if args.sheets:
        raw["sheets"] = [{"name": s} for s in args.sheets]

    return MergeConfig.from_dict(raw, base_dir=base_dir)


def _log_failed_files(failed: list) -> None:
    if not failed:
        return
    logger.warning("以下 %d 个文件读取失败，已跳过:", len(failed))
    for fp, reason in failed:
        first_line = str(reason).splitlines()[0] if reason else ""
        logger.warning("  * %s：%s", fp, first_line)


def run(args: argparse.Namespace) -> int:
    """执行合并流程，返回退出码。异常由 :func:`main` 统一处理。"""
    if not (args.config or args.config_file or args.directory or args.files):
        build_parser().print_help()
        return EXIT_ERROR

    config = build_config(args)

    logger.info("收集输入文件...")
    input_files = collect_input_files(
        config.files,
        config.directory,
        config.pattern,
        config.recursive,
        config.base_dir,
    )
    if not input_files:
        logger.error("错误: 未找到任何输入文件")
        return EXIT_ERROR
    logger.info("共找到 %d 个输入文件:", len(input_files))
    for fp in input_files:
        logger.info("  * %s", fp)

    sheets, failed = merge_sheets(input_files, config)

    output_path = Path(config.output)
    if not output_path.is_absolute():
        output_path = config.base_dir / output_path
    write_output(output_path, sheets)

    total = sum(len(df) for _, df in sheets)
    logger.info("完成! 共合并 %d 行数据，保存至: %s", total, output_path)
    _log_failed_files(failed)
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _setup_logging(args.verbose, args.quiet)

    try:
        exit_code = run(args)
    except ExcelMergeError as e:
        logger.error("错误: %s", e)
        exit_code = EXIT_ERROR

    if exit_code != EXIT_OK:
        sys.exit(exit_code)
    return exit_code


if __name__ == "__main__":
    main()
