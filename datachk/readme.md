# datachk 数据验证工具

数据开发完成后，从数据库中提取数据并写入 Excel 进行数据验证。支持多数据库、SSH 隧道、批量脚本执行，结果按脚本分 sheet 输出。

- 版本：v0.2.0
- 作者：段智慧
- 分类：数据开发

## 功能特性

- **多数据库支持**：Oracle、MySQL、SQLServer、Hive/Impala，采用工厂模式按 `type` 字段自动选择驱动
- **SSH 隧道**：通过 `ssh_id` 关联 `sshs` 配置，自动建立本地端口转发，连接内网数据库
- **批量脚本执行**：从 Excel「脚本清单」sheet 读取待执行 SQL，逐条执行并回写执行状态
- **结果分 sheet 输出**：每个脚本对应一个 sheet，自动从「模板」sheet 复制创建
- **连接复用**：同一 `conn_id` 的连接在运行期间复用，避免重复建连
- **执行状态回写**：`1=待执行`、`2=已完成`、`4=异常`，回写到脚本清单 E 列

## 目录结构

```
datachk/
├── datachk.py              # 主程序
├── config.yml              # 配置文件（连接、输出、SSH）
├── datachk_template.xlsx   # Excel 模板（含「脚本清单」与「模板」sheet）
└── readme.md               # 本文档
```

## 配置说明（config.yml）

```yaml
# 默认连接ID与默认输出ID
default_conn_id: conn_oracle_1
default_output_id: output_1

# 数据库连接配置
connections:
  conn_oracle_1:
    type: oracle              # 支持：oracle / mysql / sqlserver / hive / impala
    conn_str: "oracle+oracledb://user:password@..."
    description: "Oracle开发环境"
  conn_mysql_ssh:
    type: mysql
    conn_str: "mysql+pymysql://user:password@192.168.1.1:3306/dbname?charset=utf8mb4"
    ssh_id: ssh_1             # 关联 sshs 中的隧道配置，可选

# Excel输出配置
outputs:
  output_1:
    path: D:\...\datachk_test1.xlsx
    description: "默认输出文件"

# SSH隧道配置
sshs:
  ssh_1:
    host: 192.168.1.1
    port: 1080
    username: username
    password: password
```

### 连接字符串示例

| 类型       | conn_str 示例                                                                                       |
| ---------- | --------------------------------------------------------------------------------------------------- |
| Oracle     | `oracle+oracledb://user:pwd@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=...)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=SID)))` |
| MySQL      | `mysql+pymysql://user:pwd@host:3306/dbname?charset=utf8mb4`                                          |
| SQLServer  | `mssql+pymssql://user:pwd@host:1433/dbname?charset=cp936`                                            |
| Hive/Impala| `impala://user:pwd@host:21050/dbname`                                                                |

## Excel 模板结构

输出 Excel 需包含两个 sheet：

- **脚本清单**：从第 3 行开始读取（第 1-2 行为表头）

  | 列  | 字段       | 说明                                            |
  | --- | ---------- | ----------------------------------------------- |
  | A   | 连接id     | 对应 config.yml 中的 connections，为空用默认连接 |
  | B   | 脚本名称   | 用于创建/定位对应的 sheet                       |
  | C   | 脚本分类   | 备注信息                                        |
  | D   | 脚本描述   | 备注信息                                        |
  | E   | 执行标示   | `1=待执行`、`2=已完成`、`4=异常`                |
  | G   | SCRIPT     | SQL 脚本内容                                    |

- **模板**：当脚本对应的 sheet 不存在时，自动复制此 sheet 并重命名

## 使用方法

### 命令行

```bash
# 使用默认配置
python datachk.py

# 指定配置文件、连接ID、输出文件
python datachk.py --config config.yml --conn_id conn_oracle_1 --output output_1

# output 参数既可以是 outputs 中的 ID，也可以是直接的文件路径
python datachk.py --output D:\path\to\result.xlsx
```

### 参数说明

| 参数        | 说明                                              |
| ----------- | ------------------------------------------------- |
| `--config`  | 配置文件路径，默认 `config.yml`                   |
| `--conn_id` | 数据库连接ID，默认取 `default_conn_id`            |
| `--output`  | 输出 Excel 路径或 outputs 中的ID，默认取 `default_output_id` |

### 运行流程

1. 加载 `config.yml`，解析连接、输出、SSH 配置
2. 打开输出 Excel，读取「脚本清单」
3. 遍历脚本，仅处理 E 列为 `1` 的记录
4. 按 A 列 `连接id`（或默认连接）获取/创建验证器，必要时建立 SSH 隧道
5. 执行 G 列 SQL，结果写入以 B 列脚本名命名的 sheet
6. 回写执行状态到 E 列并保存

## 依赖

- Python 3.x
- pandas、numpy、xlwings、pyyaml、sqlalchemy
- 数据库驱动：`oracledb`、`pymysql`、`pymssql`、`impyla`（按需安装）
- SSH 隧道：`sshtunnel`、`paramiko`

> 注：xlwings 依赖本机已安装的 Excel，运行时会启动 Excel 进程。

## 注意事项

- `config.yml` 中含数据库与 SSH 凭据，请勿提交到公共仓库
- 脚本清单 E 列执行标示为 `1` 才会被执行，其他值会被跳过
- Hive/Impala 连接使用 `impala.dbapi`，并自动设置 `PARQUET_FALLBACK_SCHEMA_RESOLUTION=name` 与 `mem_limit=2048M`
- SSH 隧道创建失败会重试 3 次，仍失败则抛出异常
