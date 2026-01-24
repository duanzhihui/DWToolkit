# 模块重命名指南

## 重命名说明

模块已从 `airflowupdt` 重命名为 `airflow_upgrade`。

## 需要手动完成的步骤

由于无法直接重命名目录，请按以下步骤完成重命名：

### 1. 重命名主目录

```bash
# 在 d:\github\duanzhihui\DWToolkit\ 目录下执行
mv airflowupdt airflow_upgrade
```

或在 Windows PowerShell 中：

```powershell
# 在 d:\github\duanzhihui\DWToolkit\ 目录下执行
Rename-Item -Path "airflowupdt" -NewName "airflow_upgrade"
```

### 2. 验证重命名

重命名后，目录结构应该是：

```
d:\github\duanzhihui\DWToolkit\airflow_upgrade\
├── __init__.py
├── core/
├── rules/
├── tools/
├── cli/
├── tests/
├── pyproject.toml
├── requirements.txt
└── README.md
```

### 3. 重新安装

```bash
cd d:\github\duanzhihui\DWToolkit\airflow_upgrade
pip install -e .
```

### 4. 验证安装

```bash
airflow-upgrade --version
```

## 已完成的代码更新

所有 Python 文件中的导入语句已更新：
- ✅ `airflowupdt` → `airflow_upgrade`
- ✅ 所有 `from airflowupdt.xxx import` → `from airflow_upgrade.xxx import`
- ✅ CLI 命令名称：`airflowupdt` → `airflow-upgrade`
- ✅ pyproject.toml 配置更新
- ✅ README.md 文档更新
- ✅ 测试文件导入更新

## 命令变更

| 旧命令 | 新命令 |
|--------|--------|
| `airflowupdt upgrade` | `airflow-upgrade upgrade` |
| `airflowupdt upgrade-dir` | `airflow-upgrade upgrade-dir` |
| `airflowupdt analyze` | `airflow-upgrade analyze` |
| `airflowupdt validate` | `airflow-upgrade validate` |
| `airflowupdt lint` | `airflow-upgrade lint` |
| `airflowupdt rollback` | `airflow-upgrade rollback` |
| `airflowupdt init-config` | `airflow-upgrade init-config` |

## 配置文件变更

- 配置文件名建议：`.airflow_upgrade.yml`（之前为 `.airflowupdt.yml`）
- 备份目录默认：`.airflow_upgrade_backup`（之前为 `.airflowupdt_backup`）

## 注意事项

1. 如果之前已经安装过 `airflowupdt`，请先卸载：
   ```bash
   pip uninstall airflowupdt
   ```

2. 重命名目录后，需要重新安装才能使用新的命令名称

3. 旧的配置文件仍然可以使用，但建议更新为新的命名规范
