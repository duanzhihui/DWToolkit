# excel_merge - Excel 文件合并工具

将多个 Excel 文件中指定 sheet 的数据合并到一个输出文件。支持目录扫描、文件名正则匹配、多级表头、自定义数据起始行。

## 功能特性

1. 输入输出可通过配置文件（YAML）或命令行参数指定
2. 可配置需要合并的 sheet（支持多个）
3. 可配置每个 sheet 的数据起始行，支持多级表头
4. 只合并数据，表头默认取第一个包含该 sheet 的文件
5. 可配置参与合并的 Excel 文件：支持目录扫描 + 文件名正则匹配 + 显式文件列表

## 安装依赖

```bash
pip install -r requirements.txt
```

## 使用方式

### 方式一：配置文件

```bash
python excel_merge.py config.yml
# 或
python excel_merge.py -c config.yml
```

### 方式二：命令行参数

```bash
python excel_merge.py -d ./data -p "^销售.*\.xlsx$" -s Sheet1 -o merged.xlsx
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

## 工作说明

- 表头取**第一个包含该 sheet 的文件**，其余文件仅合并数据行。
- 自动跳过完全空白的数据行，以及 Office 锁文件（`~$` 开头）。
- 仅处理 `.xlsx` / `.xlsm` 文件。
- Excel sheet 名称上限 31 字符，超出会被截断。
