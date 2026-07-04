"""excel_merge 自定义异常。

分层结构中，core 与 config 层只 raise 这些异常，不调用 print / sys.exit；
由 cli 层统一捕获并决定日志输出与退出码。
"""


class ExcelMergeError(Exception):
    """excel_merge 所有异常的基类。"""


class ConfigError(ExcelMergeError):
    """配置文件缺失、YAML 解析失败或校验不通过。"""


class SheetNotFoundError(ExcelMergeError):
    """目标 sheet 在某个文件中不存在。

    属于可跳过的正常情况（该文件不含此 sheet），不计入失败清单。
    """


class CorruptFileError(ExcelMergeError):
    """文件损坏、加密或被占用（锁定），无法读取。

    计入失败清单，其余文件继续处理。
    """
