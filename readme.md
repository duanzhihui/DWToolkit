# DWToolkit

数据仓库 / 数据工程工具集，涵盖 SQL 解析、Airflow / Spark 代码升级、ETL 作业生成、数据探查、文件管理等常用工具。

## 目录总览

| 分类 | 目录 | 说明 |
| --- | --- | --- |
| Airflow 升级 | `airflow_upgrade/` | 将 Airflow 2.x DAG 自动升级为 3.x 兼容版本的工具 |
| Spark 升级 | `spark_upgrade/` | 将 PySpark 代码从 Spark 2.x 升级到 3.x 的工具 |
| DAG 解析 | `dag_parser/` | 解析 Airflow DAG 文件，提取任务信息并导出 Excel |
| SQL 提取 | `sql_parser/` | 从 py/sh/hql/sql 等代码文件中提取 SQL 语句 |
| SQL 表名解析 | `sql_table_parser/` | 用正则从 SQL/脚本中提取所有依赖表名 |
| CTE 解析 | `cte_parser/` | 解析 SQL WITH 语句（CTE）中的临时表名 |
| 代码分类 | `codetyp/` | 识别大数据代码类型（Hive/Spark/Flink 等）的积分制分类工具 |
| 作业生成 | `jobgen/` | 将 `.script` 文件按模板转换为 `.job` 文件 |
| 代码转脚本 | `code2script/` | 将代码文件按模板批量转换为脚本文件 |
| 测试用例生成 | `casegen/` | 从任务清单 Excel 生成测试用例 Excel |
| 数据探查 | `dataexp/` | 多数据库数据质量探查工具（Oracle/SQL Server/MySQL/Impala）|
| 文件标签 | `filetag/` | 递归扫描目录并用 Excel 管理/追踪文件标签与状态 |
| Excel 合并 | `excel_merge/` | 合并多个 Excel 文件中指定 sheet 的数据 |
| 批量重跑 | `rerun/` | Shell 批处理作业的顺序执行 / 重跑框架 |
| 根目录 | `./` | 项目许可证、忽略规则与本导航文档 |

## 文件导航

### airflow_upgrade/ — Airflow 2.x → 3.x DAG 升级工具

| 分类 | 文件名 | 文件说明 |
| --- | --- | --- |
| airflow_upgrade | README.md | 工具说明与使用文档 |
| airflow_upgrade | pyproject.toml | 项目打包与依赖配置 |
| airflow_upgrade | requirements.txt | 依赖清单 |
| airflow_upgrade | .airflow_upgrade.yml | 升级规则默认配置 |
| airflow_upgrade | cli/main.py | 命令行入口 |
| airflow_upgrade | core/config_loader.py | 配置加载 |
| airflow_upgrade | core/parser.py | DAG 代码解析 |
| airflow_upgrade | core/transformer.py | 代码转换（2.x→3.x） |
| airflow_upgrade | core/migrator.py | 迁移流程编排 |
| airflow_upgrade | core/validator.py | 升级结果校验 |
| airflow_upgrade | rules/imports.py | import 语句升级规则 |
| airflow_upgrade | rules/operators.py | Operator 升级规则 |
| airflow_upgrade | rules/config.py | 配置项升级规则 |
| airflow_upgrade | tools/backup_manager.py | 原文件备份与回滚 |
| airflow_upgrade | tools/ruff_integration.py | Ruff 代码质量集成 |
| airflow_upgrade | tools/flake8_integration.py | Flake8 代码质量集成 |
| airflow_upgrade | tests/ | 单元测试与示例 DAG（v2/v3） |

### spark_upgrade/ — PySpark 2.x → 3.x 升级工具

| 分类 | 文件名 | 文件说明 |
| --- | --- | --- |
| spark_upgrade | README.md | 工具说明与使用文档 |
| spark_upgrade | pyproject.toml | 项目打包与依赖配置 |
| spark_upgrade | requirements.txt | 依赖清单 |
| spark_upgrade | .spark_upgrade.yml | 升级规则默认配置 |
| spark_upgrade | cli/main.py | 命令行入口 |
| spark_upgrade | config/settings.py | 运行配置 |
| spark_upgrade | config/defaults.py | 默认配置 |
| spark_upgrade | core/parser.py | PySpark 代码 AST 解析 |
| spark_upgrade | core/transformer.py | 代码转换（2.x→3.x） |
| spark_upgrade | core/migrator.py | 迁移流程编排 |
| spark_upgrade | core/validator.py | 升级结果校验 |
| spark_upgrade | rules/api_changes.py | API 变更规则 |
| spark_upgrade | rules/config_updates.py | 配置更新规则 |
| spark_upgrade | rules/deprecations.py | 弃用 API 规则 |
| spark_upgrade | rules/syntax_changes.py | 语法变更规则 |
| spark_upgrade | tools/backup_manager.py | 原文件备份与回滚 |
| spark_upgrade | tools/quality_checker.py | 代码质量检查 |
| spark_upgrade | tools/report_generator.py | 升级报告生成 |
| spark_upgrade | tests/ | 单元/集成测试与示例代码 |

### dag_parser/ — Airflow DAG 解析工具

| 分类 | 文件名 | 文件说明 |
| --- | --- | --- |
| dag_parser | README.md | 说明与使用文档 |
| dag_parser | dag_parser.py | 主程序，解析 DAG 任务信息并导出 Excel |
| dag_parser | config.yml | 输入/输出配置 |
| dag_parser | requirements.txt | 依赖清单 |
| dag_parser | dags/example_dag.py | 示例 DAG 文件 |

### sql_parser/ — SQL 代码提取工具

| 分类 | 文件名 | 文件说明 |
| --- | --- | --- |
| sql_parser | readme.md | 说明与使用文档 |
| sql_parser | sql_parser.py | 主程序，从代码文件中提取 SQL 语句 |
| sql_parser | config.yaml | 配置文件 |
| sql_parser | input/sample_queries.sql | 示例 SQL 文件 |
| sql_parser | input/sample_script.sh | 示例 Shell 脚本 |
| sql_parser | input/folder/sample_code.py | 示例 Python 代码 |
| sql_parser | input/folder/sample_etl.hql | 示例 HQL 文件 |

### sql_table_parser/ — SQL 表名解析工具

| 分类 | 文件名 | 文件说明 |
| --- | --- | --- |
| sql_table_parser | readme.md | 说明与使用文档 |
| sql_table_parser | sql_table_parser.py | 主程序，提取 SQL/脚本中的所有表名 |
| sql_table_parser | sample_queries.sql | 示例 SQL 文件 |
| sql_table_parser | sample_script.py | 示例 Python 脚本 |
| sql_table_parser | sample_script.sh | 示例 Shell 脚本 |
| sql_table_parser | parse_result.txt | 解析结果输出示例 |

### cte_parser/ — SQL CTE 解析工具

| 分类 | 文件名 | 文件说明 |
| --- | --- | --- |
| cte_parser | readme.md | 说明与使用文档 |
| cte_parser | cte_parser.py | 主程序，解析 WITH 语句临时表名 |
| cte_parser | sample.sql | 示例 SQL 文件 |

### codetyp/ — 代码类型分类工具

| 分类 | 文件名 | 文件说明 |
| --- | --- | --- |
| codetyp | readme.md | 说明与使用文档 |
| codetyp | codetyp.py | 主程序，积分制识别代码类型 |
| codetyp | config.yml | 分类规则配置 |
| codetyp | requirements.txt | 依赖清单 |
| codetyp | classification_result.xlsx | 分类结果输出示例 |
| codetyp | code/ | 各类框架示例代码（Hive/Spark/Flink 等） |

### jobgen/ — Script→Job 作业生成工具

| 分类 | 文件名 | 文件说明 |
| --- | --- | --- |
| jobgen | readme.md | 说明与使用文档 |
| jobgen | jobgen.py | 主程序，按模板生成 job 文件 |
| jobgen | config.yml | 配置文件 |
| jobgen | template.job | Job 模板文件 |
| jobgen | job/ | 生成的 job 文件示例 |

### code2script/ — 代码转脚本工具

| 分类 | 文件名 | 文件说明 |
| --- | --- | --- |
| code2script | README.md | 说明与使用文档 |
| code2script | code2script.py | 主程序，按模板将代码转换为脚本 |
| code2script | config.yml | 配置文件 |
| code2script | code/ | 源代码示例（sql/hql） |
| code2script | script/ | 转换后脚本示例 |

### casegen/ — 测试用例生成工具

| 分类 | 文件名 | 文件说明 |
| --- | --- | --- |
| casegen | README.md | 说明与使用文档 |
| casegen | casegen.py | 主程序，从任务清单生成测试用例 |
| casegen | config.yml | 转换规则配置 |
| casegen | requirements.txt | 依赖清单 |
| casegen | task.xlsx | 任务清单输入示例 |
| casegen | case.xlsx | 测试用例输出示例 |
| casegen | create_sample_task.py | 生成示例任务清单的脚本 |

### dataexp/ — 数据探查工具

| 分类 | 文件名 | 文件说明 |
| --- | --- | --- |
| dataexp | README.md | 说明与使用文档 |
| dataexp | dataexp.py | 主程序，多数据库数据质量探查 |
| dataexp | config.yml | 数据库连接与探查配置 |
| dataexp | requirements.txt | 依赖清单 |
| dataexp | dataexp_template.xlsx | 探查结果 Excel 模板 |

### filetag/ — 文件标签管理工具

| 分类 | 文件名 | 文件说明 |
| --- | --- | --- |
| filetag | readme.md | 说明与使用文档 |
| filetag | filetag.py | 主程序，扫描目录并管理文件标签 |
| filetag | config.yml | 扫描与过滤配置 |
| filetag | requirements.txt | 依赖清单 |

### excel_merge/ — Excel 合并工具

| 分类 | 文件名 | 文件说明 |
| --- | --- | --- |
| excel_merge | readme.md | 说明与使用文档 |
| excel_merge | excel_merge.py | 主程序，合并多个 Excel 的指定 sheet |
| excel_merge | config.yml | 合并配置 |
| excel_merge | requirements.txt | 依赖清单 |

### rerun/ — 批量重跑框架

| 分类 | 文件名 | 文件说明 |
| --- | --- | --- |
| rerun | rerun.sh | 批处理作业顺序执行 / 重跑主脚本 |
| rerun | common_init.sh | 公共初始化脚本 |
| rerun | iniGetValue.sh | 读取 ini 配置值的工具脚本 |
| rerun | prog.ini | 作业配置（ini 格式） |

### 根目录

| 分类 | 文件名 | 文件说明 |
| --- | --- | --- |
| 根目录 | readme.md | 本项目导航说明文档 |
| 根目录 | LICENSE | 开源许可证 |
| 根目录 | .gitignore | Git 忽略规则 |
