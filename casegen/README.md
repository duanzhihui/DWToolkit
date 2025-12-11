# casegen - 任务清单转测试用例工具

从 `task.xlsx` 读取任务清单，根据 YAML 配置文件生成 `case.xlsx` 测试用例。

## 功能特性

- 从 Excel 任务清单读取任务数据
- 支持 YAML 配置文件定义转换规则
- 一个任务可生成多个测试用例
- 支持字段占位符替换
- 支持自定义用例字段

## 安装依赖

```bash
pip install -r requirements.txt
```

## 使用方法

```bash
python casegen.py config.yml
# 或
python casegen.py -c config.yml
```

## 配置文件说明

配置文件使用 YAML 格式，主要配置项：

### 基本配置

```yaml
input_file: "task.xlsx"      # 输入文件路径
output_file: "case.xlsx"     # 输出文件路径
input_sheet: null            # 输入sheet名称（null表示第一个sheet）
output_sheet: "测试用例"      # 输出sheet名称
```

### 用例模板配置

```yaml
cases:
  - name: "正向测试"          # 用例模板名称
    fields:                   # 字段列表
      - name: "用例编号"       # 输出列名
        value: "TC_<任务编号>_{index}"  # 值的生成规则
```

### 占位符说明

| 占位符格式 | 说明 | 示例 |
|-----------|------|------|
| `<字段名>` | 从任务清单中提取该字段的值 | `<任务编号>`, `<任务名称>` |
| `{index}` | 全局用例序号（从1开始递增） | 1, 2, 3... |
| `{task_index}` | 任务序号（从1开始） | 1, 2, 3... |

### 完整配置示例

```yaml
input_file: "task.xlsx"
output_file: "case.xlsx"
output_sheet: "测试用例"

cases:
  - name: "正向测试"
    fields:
      - name: "用例编号"
        value: "TC_<任务编号>_{index}"
      - name: "用例名称"
        value: "<任务名称>_正向测试"
      - name: "前置条件"
        value: "<前置条件>"
      - name: "测试步骤"
        value: "<测试步骤>"
      - name: "预期结果"
        value: "<预期结果>"
      - name: "优先级"
        value: "高"

  - name: "异常测试"
    fields:
      - name: "用例编号"
        value: "TC_<任务编号>_{index}"
      - name: "用例名称"
        value: "<任务名称>_异常测试"
      - name: "前置条件"
        value: "<前置条件>"
      - name: "测试步骤"
        value: "输入异常数据进行测试"
      - name: "预期结果"
        value: "系统给出正确的错误提示"
      - name: "优先级"
        value: "中"
```

## 快速开始

1. 创建示例任务文件：
   ```bash
   python create_sample_task.py
   ```

2. 运行转换：
   ```bash
   python casegen.py config.yml
   ```

3. 查看生成的 `case.xlsx`

## 任务清单格式

任务清单 Excel 文件第一行为表头，后续行为任务数据。表头名称可在配置文件中通过 `<字段名>` 引用。

示例：

| 任务编号 | 任务名称 | 所属模块 | 前置条件 | 测试步骤 | 预期结果 |
|---------|---------|---------|---------|---------|---------|
| T001 | 用户登录 | 用户模块 | 用户已注册 | 1.输入用户名... | 登录成功 |
