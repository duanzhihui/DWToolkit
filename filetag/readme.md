# 文件标签

这是一个用Python开发的文件标签管理，可以帮助您管理和追踪指定目录下的所有文件。

## 功能特点

- 递归扫描指定目录下的所有文件
- 自动生成并更新文件列表Excel文件
- 追踪文件状态（新增、删除、已有）
- 支持为文件添加标签和备注
- 自动记录文件更新日期
- **支持文件过滤（包含/排除规则，Unix通配符）**
- **支持YAML配置文件**

## 安装依赖

```bash
pip install -r requirements.txt
```

## 配置文件

程序支持通过 `config.yml` 配置文件进行设置。配置文件位于程序所在目录。

```yaml
# 扫描目录路径（留空则使用程序所在目录）
directory: ""

# 输出文件路径（默认为扫描目录下的"文件列表.xlsx"）
# 可以指定完整路径或仅文件名
output: "文件列表.xlsx"

# 包含过滤器（仅扫描匹配的文件和目录）
# 支持Unix通配符，多个规则用分号分隔
# 文件示例: *.txt; *.xlsx; data_*.csv
# 目录示例: MyFolder\; backup[0-9]\
# 留空表示包含所有文件
include: ""

# 排除过滤器（排除匹配的文件和目录）
# 支持Unix通配符，多个规则用分号分隔
exclude: "文件列表.xlsx; .git\\; __pycache__\\"
```

### 过滤规则说明

- **包含规则**：仅扫描匹配指定模式的文件与目录内文件
- **排除规则**：排除匹配指定模式的文件与目录
- **多个规则**：使用分号 `;` 分隔
- **目录模式**：在模式后加 `\` 表示目录（例：`MyFolder\`）
- **通配符支持**：
  - `*` 匹配任意字符
  - `?` 匹配单个字符
  - `[seq]` 匹配seq中的任意字符
  - `[!seq]` 匹配不在seq中的任意字符

## 使用方法

### 1. 使用配置文件（推荐）

编辑 `config.yml` 后直接运行：
```bash
python filetag.py
```

### 2. 命令行参数

```bash
# 扫描指定目录
python filetag.py --dir "D:/你的目录路径"
python filetag.py -d "D:/你的目录路径"

# 指定包含规则
python filetag.py --include "*.txt; *.xlsx"
python filetag.py -i "*.txt; *.xlsx"

# 指定排除规则
python filetag.py --exclude "*.tmp; temp\"
python filetag.py -e "*.tmp; temp\"

# 指定输出文件路径
python filetag.py --output "D:/output/文件列表.xlsx"
python filetag.py -o "mylist.xlsx"

# 指定配置文件
python filetag.py --config "/path/to/config.yml"
python filetag.py -c "/path/to/config.yml"

# 组合使用
python filetag.py -d "D:/数据" -i "*.csv; *.xlsx" -e "backup\"
```

### 参数优先级

命令行参数 > 配置文件 > 默认值

## 输出说明

程序会在指定目录下生成"文件列表.xlsx"文件，包含以下信息：
- 文件目录
- 文件名
- 文件扩展名
- 文件大小(MB)
- 创建日期
- 修改日期
- 文件md5
- 文件重复数
- 文件标签
- 文件备注
- 文件状态（新增、删除、已有）
- 更新日期