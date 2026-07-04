"""合并逻辑单元测试：表头名对齐、空行过滤、多级表头、data_start_row、溯源列。"""

import pandas as pd

from excel_merge.config import MergeConfig, SheetConfig
from excel_merge.core import (
    SOURCE_COLUMN,
    merge_sheet,
    merge_sheets,
    read_sheet,
    write_output,
)


def test_read_sheet_adds_source_column(make_xlsx):
    p = make_xlsx("a.xlsx", {"S": [["A", "B"], [1, 2]]})
    df = read_sheet(p, SheetConfig(name="S"))
    assert SOURCE_COLUMN in df.columns
    assert df[SOURCE_COLUMN].tolist() == ["a.xlsx"]


def test_header_name_alignment(make_xlsx):
    # 两个文件列顺序不同，应按表头名对齐而非按位置拼接
    f1 = make_xlsx("f1.xlsx", {"S": [["A", "B"], [1, 2]]})
    f2 = make_xlsx("f2.xlsx", {"S": [["B", "A"], [20, 10]]})
    merged, failed = merge_sheet([f1, f2], SheetConfig(name="S"))
    assert failed == []
    # A 列应为 [1, 10]，B 列应为 [2, 20]（按名对齐）
    assert merged["A"].tolist() == [1, 10]
    assert merged["B"].tolist() == [2, 20]


def test_union_of_columns(make_xlsx):
    # 不同文件列集合不同，取并集，缺列填 NaN
    f1 = make_xlsx("f1.xlsx", {"S": [["A", "B"], [1, 2]]})
    f2 = make_xlsx("f2.xlsx", {"S": [["A", "C"], [3, 4]]})
    merged, _ = merge_sheet([f1, f2], SheetConfig(name="S"))
    assert set(merged.columns) >= {"A", "B", "C", SOURCE_COLUMN}
    assert pd.isna(merged.loc[1, "B"])
    assert pd.isna(merged.loc[0, "C"])


def test_empty_rows_filtered(make_xlsx):
    p = make_xlsx(
        "a.xlsx",
        {"S": [["A", "B"], [1, 2], [None, None], ["", ""], [3, 4]]},
    )
    df = read_sheet(p, SheetConfig(name="S"))
    # 全空行被过滤，仅保留两行数据
    assert df["A"].dropna().tolist() == [1, 3]
    assert len(df) == 2


def test_multi_level_header(make_xlsx):
    f1 = make_xlsx(
        "f1.xlsx",
        {"S": [["g1", "g1", "g2"], ["a", "b", "c"], [1, 2, 3]]},
    )
    f2 = make_xlsx(
        "f2.xlsx",
        {"S": [["g1", "g1", "g2"], ["a", "b", "c"], [4, 5, 6]]},
    )
    merged, failed = merge_sheet([f1, f2], SheetConfig(name="S", header_rows=2))
    assert failed == []
    assert isinstance(merged.columns, pd.MultiIndex)
    assert merged[("g1", "a")].tolist() == [1, 4]
    assert merged[("g2", "c")].tolist() == [3, 6]


def test_data_start_row_skips_note_rows(make_xlsx):
    # 第 1 行表头，第 2 行说明，数据从第 3 行开始
    p = make_xlsx(
        "a.xlsx",
        {"S": [["A", "B"], ["说明", "忽略"], [1, 2], [3, 4]]},
    )
    df = read_sheet(p, SheetConfig(name="S", header_rows=1, data_start_row=3))
    assert df["A"].tolist() == [1, 3]
    assert "说明" not in df["A"].tolist()


def test_sheet_not_found_is_skipped_not_failed(make_xlsx):
    f1 = make_xlsx("f1.xlsx", {"S": [["A"], [1]]})
    f2 = make_xlsx("f2.xlsx", {"Other": [["A"], [2]]})
    merged, failed = merge_sheet([f1, f2], SheetConfig(name="S"))
    assert failed == []
    assert merged["A"].tolist() == [1]


def test_corrupt_file_collected_in_failed(tmp_path, make_xlsx):
    good = make_xlsx("good.xlsx", {"S": [["A"], [1]]})
    bad = tmp_path / "bad.xlsx"
    bad.write_bytes(b"not a real xlsx")
    merged, failed = merge_sheet([good, bad], SheetConfig(name="S"))
    assert merged["A"].tolist() == [1]
    assert len(failed) == 1
    assert failed[0][0] == bad


def test_merge_sheets_and_write_output(tmp_path, make_xlsx):
    f1 = make_xlsx("f1.xlsx", {"S": [["A", "B"], [1, 2]]})
    f2 = make_xlsx("f2.xlsx", {"S": [["A", "B"], [3, 4]]})
    cfg = MergeConfig.from_dict({"sheets": ["S"]}, base_dir=tmp_path)
    sheets, failed = merge_sheets([f1, f2], cfg)
    assert failed == []
    out = tmp_path / "out.xlsx"
    write_output(out, sheets)
    assert out.exists()
    back = pd.read_excel(out, sheet_name="S")
    assert back["A"].tolist() == [1, 3]


def test_write_output_dedupes_truncated_sheet_names(tmp_path):
    long_a = "长" * 40 + "甲"
    long_b = "长" * 40 + "乙"
    df = pd.DataFrame({"A": [1]})
    sheets = [(long_a, df), (long_b, df)]
    out = tmp_path / "out.xlsx"
    write_output(out, sheets)
    names = pd.ExcelFile(out).sheet_names
    assert len(names) == 2
    assert len(set(names)) == 2
    assert all(len(n) <= 31 for n in names)
