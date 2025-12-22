#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Code to Script Converter

将代码文件转换为指定格式的脚本文件，支持自定义模板和YAML配置。
"""

import os
import re
import json
import argparse
import fnmatch
from pathlib import Path
from typing import Dict, List, Optional, Any, Union

import yaml


class Code2Script:
    """代码文件转脚本文件转换器"""

    def __init__(self, config_path: Optional[str] = None, **kwargs):
        """
        初始化转换器

        Args:
            config_path: YAML配置文件路径
            **kwargs: 覆盖配置的参数
        """
        self.config = self._load_config(config_path)
        # 命令行参数覆盖配置文件
        self._override_config(kwargs)

    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """加载YAML配置文件"""
        default_config = {
            "input": {
                "code_dir": "./code",
                "patterns": ["*.sql"],
            },
            "output": {
                "script_dir": "./script",
                "extension": ".script",
            },
            "template": {
                "file": None,
                "content": {
                    "configuration": {},
                    "connectionName": "mrs-spark",
                    "content": "<sql_content>",
                    "currentDatabase": "",
                    "description": "",
                    "directory": "<file_dir>",
                    "name": "<file_name>",
                    "owner": "datalake",
                    "templateVersion": "1.0",
                    "type": "SparkSQL",
                },
            },
            "variables": {
                "sql_content": {
                    "type": "file_content",
                    "escape_newline": True,
                },
                "file_dir": {
                    "type": "file_path",
                    "strip_prefix": "",
                    "add_prefix": "/script",
                    "use_posix": True,
                },
                "file_name": {
                    "type": "file_name",
                    "include_extension": False,
                },
            },
        }

        if config_path and os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f:
                user_config = yaml.safe_load(f)
                if user_config:
                    default_config = self._merge_config(default_config, user_config)

        return default_config

    def _merge_config(
        self, base: Dict[str, Any], override: Dict[str, Any]
    ) -> Dict[str, Any]:
        """递归合并配置"""
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_config(result[key], value)
            else:
                result[key] = value
        return result

    def _override_config(self, kwargs: Dict[str, Any]) -> None:
        """使用命令行参数覆盖配置"""
        if kwargs.get("code_dir"):
            self.config["input"]["code_dir"] = kwargs["code_dir"]
        if kwargs.get("script_dir"):
            self.config["output"]["script_dir"] = kwargs["script_dir"]
        if kwargs.get("patterns"):
            self.config["input"]["patterns"] = kwargs["patterns"]
        if kwargs.get("template_file"):
            self.config["template"]["file"] = kwargs["template_file"]

    def _get_template(self) -> Union[Dict[str, Any], str]:
        """获取模板内容"""
        template_file = self.config["template"].get("file")
        if template_file and os.path.exists(template_file):
            with open(template_file, "r", encoding="utf-8") as f:
                content = f.read()
                try:
                    return json.loads(content)
                except json.JSONDecodeError:
                    return content
        return self.config["template"]["content"]

    def _match_patterns(self, filename: str) -> bool:
        """检查文件是否匹配过滤规则"""
        patterns = self.config["input"]["patterns"]
        return any(fnmatch.fnmatch(filename, pattern) for pattern in patterns)

    def _get_variable_value(
        self, var_name: str, file_path: Path, code_dir: Path
    ) -> str:
        """根据变量配置获取变量值"""
        var_config = self.config["variables"].get(var_name, {})
        var_type = var_config.get("type", "literal")

        if var_type == "file_content":
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            if var_config.get("escape_newline", True):
                content = content.replace("\\", "\\\\")
                content = content.replace('"', '\\"')
                content = content.replace("\n", "\\n")
                content = content.replace("\r", "")
                content = content.replace("\t", "\\t")
            return content

        elif var_type == "file_path":
            rel_path = file_path.parent.relative_to(code_dir)
            strip_prefix = var_config.get("strip_prefix", "")
            add_prefix = var_config.get("add_prefix", "/")
            use_posix = var_config.get("use_posix", True)

            if use_posix:
                path_str = rel_path.as_posix()
            else:
                path_str = str(rel_path)

            if strip_prefix and path_str.startswith(strip_prefix):
                path_str = path_str[len(strip_prefix):]

            if path_str == ".":
                path_str = ""

            result = add_prefix + path_str if path_str else add_prefix
            return result.rstrip("/") if result != "/" else result

        elif var_type == "file_name":
            include_ext = var_config.get("include_extension", False)
            if include_ext:
                return file_path.name
            return file_path.stem

        elif var_type == "literal":
            return var_config.get("value", "")

        return ""

    def _replace_variables(
        self, template: Any, file_path: Path, code_dir: Path
    ) -> Any:
        """替换模板中的变量"""
        if isinstance(template, str):
            pattern = r"<(\w+)>"
            matches = re.findall(pattern, template)
            result = template
            for var_name in matches:
                value = self._get_variable_value(var_name, file_path, code_dir)
                result = result.replace(f"<{var_name}>", value)
            return result

        elif isinstance(template, dict):
            return {k: self._replace_variables(v, file_path, code_dir) for k, v in template.items()}

        elif isinstance(template, list):
            return [self._replace_variables(item, file_path, code_dir) for item in template]

        return template

    def _find_files(self, code_dir: Path) -> List[Path]:
        """查找所有匹配的文件"""
        files = []
        for root, _, filenames in os.walk(code_dir):
            for filename in filenames:
                if self._match_patterns(filename):
                    files.append(Path(root) / filename)
        return files

    def convert_file(self, file_path: Union[str, Path], code_dir: Optional[Union[str, Path]] = None) -> str:
        """
        转换单个文件

        Args:
            file_path: 源文件路径
            code_dir: 代码根目录（用于计算相对路径）

        Returns:
            转换后的脚本内容
        """
        file_path = Path(file_path)
        if code_dir is None:
            code_dir = file_path.parent
        else:
            code_dir = Path(code_dir)

        template = self._get_template()
        result = self._replace_variables(template, file_path, code_dir)

        if isinstance(result, dict):
            return json.dumps(result, ensure_ascii=False, indent="\t")
        return str(result)

    def convert(self) -> List[str]:
        """
        执行批量转换

        Returns:
            生成的脚本文件路径列表
        """
        code_dir = Path(self.config["input"]["code_dir"]).resolve()
        script_dir = Path(self.config["output"]["script_dir"]).resolve()
        extension = self.config["output"]["extension"]

        if not code_dir.exists():
            raise FileNotFoundError(f"代码目录不存在: {code_dir}")

        files = self._find_files(code_dir)
        output_files = []

        for file_path in files:
            rel_path = file_path.relative_to(code_dir)
            output_path = script_dir / rel_path.with_suffix(extension)

            output_path.parent.mkdir(parents=True, exist_ok=True)

            script_content = self.convert_file(file_path, code_dir)

            with open(output_path, "w", encoding="utf-8") as f:
                f.write(script_content)

            output_files.append(str(output_path))
            print(f"转换完成: {file_path} -> {output_path}")

        return output_files


def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(
        description="Code to Script Converter - 将代码文件转换为脚本文件",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 使用配置文件
  python code2script.py -c config.yml

  # 指定输入输出目录
  python code2script.py -i ./code -o ./script

  # 指定文件过滤规则
  python code2script.py -i ./code -o ./script -p "*.sql" "*.hql"

  # 使用自定义模板文件
  python code2script.py -c config.yml -t template.json
        """,
    )

    parser.add_argument(
        "-c", "--config",
        type=str,
        default="config.yml",
        help="YAML配置文件路径 (默认: config.yml)",
    )
    parser.add_argument(
        "-i", "--input",
        type=str,
        dest="code_dir",
        help="输入代码目录路径",
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        dest="script_dir",
        help="输出脚本目录路径",
    )
    parser.add_argument(
        "-p", "--patterns",
        type=str,
        nargs="+",
        help="文件过滤规则 (如: *.sql *.hql)",
    )
    parser.add_argument(
        "-t", "--template",
        type=str,
        dest="template_file",
        help="自定义模板文件路径",
    )
    parser.add_argument(
        "-f", "--file",
        type=str,
        help="转换单个文件",
    )

    args = parser.parse_args()

    config_path = args.config if os.path.exists(args.config) else None

    converter = Code2Script(
        config_path=config_path,
        code_dir=args.code_dir,
        script_dir=args.script_dir,
        patterns=args.patterns,
        template_file=args.template_file,
    )

    if args.file:
        result = converter.convert_file(args.file)
        print(result)
    else:
        output_files = converter.convert()
        print(f"\n共转换 {len(output_files)} 个文件")


if __name__ == "__main__":
    main()
