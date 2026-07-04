# excel_merge - Excel 文件合并工具

将多个 Excel 文件中指定 sheet 的数据合并到一个输出文件。基于 **pandas 按表头名对齐**，
支持目录扫描、文件名正则匹配、多级表头、自定义数据起始行，并为每行标注来源文件。

## 包结构

重构为分层包，便于复用与测试：

```
excel_merge/
├── __init__.py      # 对外导出配置模型、核心函数与异常
├── config.py        # dataclass 配置模型（SheetConfig / MergeConfig），加载/校验 YAML
├── core.py          # 纯函数库层：pandas 按表头名对齐合并，异常隔离，只 raise 不 print/exit
├── cli.py           # 命令行入口：argparse + logging + 退出码（唯一允许 sys.exit 的层）
├── exceptions.py    # 自定义异常
├── __main__.py      # 支持 python -m excel_merge
└── tests/           # pytest 单元测试
```

- **config**：用 dataclass 定义配置，负责加载/校验 YAML；校验失败抛 `ConfigError`（不 `sys.exit`）。
  其中会校验 `data_start_row` 不得与 `header_rows` 重叠。
- **core**：纯函数库层，用 `pd.read_excel` + `pd.concat` 按表头名对齐合并；逐文件做异常隔离，
  跳过损坏/加密/锁定的文件并收集失败清单；只 `raise` 自定义异常（`SheetNotFoundError`、
  `CorruptFileError` 等），不 `print`、不 `exit`。
- **cli**：保留原有命令行接口，用标准 `logging` 替换所有 `print`，捕获异常并决定退出码，
  最后汇总失败文件清单。

## 安装

在仓库根目录（含 `pyproject.toml`）执行：

```bash
pip install -e .
```

安装后提供 `excel-merge` 命令。也可直接安装依赖后用 `python -m excel_merge` 运行：

```bash
pip install -r excel_merge/requirements.txt
```

## 功能特性

1. 输入输出可通过配置文件（YAML）或命令行参数指定。
2. 可配置需要合并的 sheet（支持多个）。
3. 可配置每个 sheet 的数据起始行，支持多级表头。
4. **按表头名对齐**合并数据，而非按列位置拼接；不同文件列顺序不同也能正确对齐。
5. 可配置参与合并的 Excel 文件：目录扫描 + 文件名正则匹配 + 显式文件列表。
6. 合并结果为每行追加 `__source_file__` 溯源列，记录数据来源文件名。

## 使用方式

### 方式一：配置文件

```bash
excel-merge config.yml
# 或
excel-merge -c config.yml
# 或
python -m excel_merge config.yml
```

### 方式二：命令行参数

```bash
excel-merge -d ./data -p "^销售.*\.xlsx$" -s Sheet1 -o merged.xlsx
```

命令行参数会覆盖配置文件中的同名配置。

## 命令行参数

| 参数 | 说明 |
| --- | --- |
| `config` / `-c, --config-file` | YAML 配置文件路径 |
| `-d, --directory` | 扫描目录 |
| `-p, --pattern` | 文件名正则表达式（匹配文件名，不含路径）|
| `-f, --files` | 显式指定的文件列表 |
| `-s, --sheets` | 需要合并的 sheet 名称列表 |
| `-o, --output` | 输出文件路径 |
| `-r, --recursive` | 递归扫描子目录 |
| `-v, --verbose` | 输出更详细的日志（DEBUG）|
| `-q, --quiet` | 仅输出告警与错误（WARNING）|

## 配置文件说明

```yaml
# 输入：目录扫描
directory: "./data"          # 扫描目录（相对路径基于配置文件所在目录）
pattern: "^销售.*\\.xlsx$"   # 文件名正则，留空匹配全部 .xlsx/.xlsm
recursive: false             # 是否递归子目录
files:                       # 显式文件列表（可选，与扫描结果去重合并）
  - "./extra/补充数据.xlsx"

# 输出
output: "merged.xlsx"

# Sheet 合并配置
sheets:
  - name: "Sheet1"           # 源 sheet 名称（必填）
    output_name: "Sheet1"    # 输出 sheet 名称（可选，默认同 name）
    header_rows: 1           # 表头行数，支持多级表头（默认 1）
    data_start_row: 2        # 数据起始行（1 基，可选，默认 header_rows+1）
```

### 多级表头示例

当 sheet 有两行表头、数据从第 3 行开始：

```yaml
sheets:
  - name: "明细"
    header_rows: 2
    data_start_row: 3
```

> `data_start_row` 必须大于 `header_rows`（不得与表头行重叠），否则加载配置时抛出异常。

## 工作说明

- **按表头名对齐**：合并使用 `pd.concat`，以列的表头名（多级表头为表头元组）取并集对齐。
  这意味着不同文件的列顺序可以不同，只要表头名一致就会归到同一列；某文件缺少的列会以空值填充。
  这与旧版本「按列位置拼接」的行为不同，是本次重构的关键改进。
- **溯源列**：合并结果每行追加 `__source_file__` 列，值为来源文件名，便于追溯数据出处。
- **异常隔离**：逐文件读取，损坏/加密/被占用的文件会被跳过并计入失败清单，运行结束时汇总；
  不含目标 sheet 的文件按正常情况跳过，不计入失败清单。
- 自动跳过完全空白的数据行，以及 Office 锁文件（`~$` 开头）。
- 仅处理 `.xlsx` / `.xlsm` 文件。
- Excel sheet 名称上限 31 字符，超出会被截断；截断后若重名会自动加后缀避免冲突。

## 关于公式与 data_only 缓存

pandas 的 openpyxl 读取器以 `data_only=True` 打开工作簿，读取到的是**公式的缓存值**而非
公式文本，等价于旧实现的 `data_only` 行为。

> 限制：缓存值由 Excel 在保存时写入。若某个 `.xlsx` 文件是由程序生成、从未经 Excel 打开并保存，
> 其公式单元格可能没有缓存值，读取时会得到空值（`NaN`）。如需正确读取公式结果，请先用 Excel
> 打开并保存这些文件。

## 开发

```bash
pip install -e ".[dev]"
pytest excel_merge/tests
ruff check excel_merge
```
