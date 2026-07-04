"""config 层单元测试。"""

import pytest

from excel_merge.config import MergeConfig, SheetConfig, load_config_file
from excel_merge.exceptions import ConfigError


def test_sheet_config_defaults():
    sc = SheetConfig(name="Sheet1")
    assert sc.output_name == "Sheet1"
    assert sc.header_rows == 1
    assert sc.data_start_row is None
    assert sc.effective_data_start_row == 2


def test_sheet_config_effective_data_start_row_with_multiheader():
    sc = SheetConfig(name="明细", header_rows=2)
    assert sc.effective_data_start_row == 3


def test_sheet_config_requires_name():
    with pytest.raises(ConfigError):
        SheetConfig(name="")


def test_data_start_row_overlap_with_header_raises():
    # header_rows=2 占用第 1、2 行，data_start_row=2 与表头重叠
    with pytest.raises(ConfigError, match="重叠"):
        SheetConfig(name="S", header_rows=2, data_start_row=2)


def test_data_start_row_equal_header_rows_raises():
    with pytest.raises(ConfigError):
        SheetConfig(name="S", header_rows=1, data_start_row=1)


def test_data_start_row_just_after_header_ok():
    sc = SheetConfig(name="S", header_rows=2, data_start_row=3)
    assert sc.effective_data_start_row == 3


def test_data_start_row_must_be_positive():
    with pytest.raises(ConfigError):
        SheetConfig(name="S", header_rows=1, data_start_row=0)


def test_from_value_string():
    sc = SheetConfig.from_value("Sheet1")
    assert sc.name == "Sheet1"
    assert sc.output_name == "Sheet1"


def test_from_value_unknown_field():
    with pytest.raises(ConfigError, match="未知字段"):
        SheetConfig.from_value({"name": "S", "bogus": 1})


def test_merge_config_requires_sheets():
    with pytest.raises(ConfigError):
        MergeConfig.from_dict({"directory": "./data"})


def test_merge_config_from_dict():
    cfg = MergeConfig.from_dict(
        {
            "directory": "./data",
            "pattern": r"\.xlsx$",
            "recursive": True,
            "output": "out.xlsx",
            "sheets": ["Sheet1", {"name": "明细", "header_rows": 2, "data_start_row": 3}],
        },
        base_dir="/tmp",
    )
    assert cfg.recursive is True
    assert len(cfg.sheets) == 2
    assert cfg.sheets[1].header_rows == 2


def test_load_config_file_missing():
    with pytest.raises(ConfigError):
        load_config_file("/nonexistent/config.yml")


def test_load_config_file_ok(tmp_path):
    p = tmp_path / "config.yml"
    p.write_text("directory: ./data\nsheets:\n  - name: Sheet1\n", encoding="utf-8")
    raw = load_config_file(str(p))
    assert raw["directory"] == "./data"
