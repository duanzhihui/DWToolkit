"""collect_input_files 单元测试：正则匹配、去重、锁文件过滤、递归。"""

import pytest

from excel_merge.core import collect_input_files
from excel_merge.exceptions import ConfigError


def _touch(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")
    return path


def test_pattern_matching(tmp_path):
    _touch(tmp_path / "销售_1.xlsx")
    _touch(tmp_path / "销售_2.xlsx")
    _touch(tmp_path / "库存.xlsx")
    result = collect_input_files(None, ".", r"^销售.*\.xlsx$", False, tmp_path)
    names = sorted(p.name for p in result)
    assert names == ["销售_1.xlsx", "销售_2.xlsx"]


def test_only_xlsx_xlsm(tmp_path):
    _touch(tmp_path / "a.xlsx")
    _touch(tmp_path / "b.xlsm")
    _touch(tmp_path / "c.csv")
    _touch(tmp_path / "d.txt")
    result = collect_input_files(None, ".", None, False, tmp_path)
    assert sorted(p.name for p in result) == ["a.xlsx", "b.xlsm"]


def test_lock_file_filtered(tmp_path):
    _touch(tmp_path / "data.xlsx")
    _touch(tmp_path / "~$data.xlsx")
    result = collect_input_files(None, ".", None, False, tmp_path)
    assert [p.name for p in result] == ["data.xlsx"]


def test_dedupe_between_files_and_directory(tmp_path):
    f = _touch(tmp_path / "data.xlsx")
    result = collect_input_files([str(f)], ".", None, False, tmp_path)
    assert len(result) == 1


def test_recursive_scan(tmp_path):
    _touch(tmp_path / "top.xlsx")
    _touch(tmp_path / "sub" / "nested.xlsx")

    non_recursive = collect_input_files(None, ".", None, False, tmp_path)
    assert sorted(p.name for p in non_recursive) == ["top.xlsx"]

    recursive = collect_input_files(None, ".", None, True, tmp_path)
    assert sorted(p.name for p in recursive) == ["nested.xlsx", "top.xlsx"]


def test_missing_explicit_file_skipped(tmp_path):
    result = collect_input_files(["nope.xlsx"], None, None, False, tmp_path)
    assert result == []


def test_invalid_regex_raises(tmp_path):
    _touch(tmp_path / "a.xlsx")
    with pytest.raises(ConfigError):
        collect_input_files(None, ".", "[unclosed", False, tmp_path)
