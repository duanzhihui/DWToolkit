#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
JobGen - Script to Job File Converter

将 .script 文件转换为 .job 文件的工具程序。
支持自定义模板、配置文件和命令行参数。
"""

import argparse
import json
import os
import re
import sys
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml


class JobGenerator:
    """Script 文件到 Job 文件的转换器"""

    def __init__(self, config_path: Optional[str] = None):
        """
        初始化 JobGenerator

        Args:
            config_path: 配置文件路径，默认为当前目录下的 config.yml
        """
        self.config_path = config_path or self._get_default_config_path()
        self.config: Dict[str, Any] = {}
        self.template: str = ""
        self.current_id: int = 1
        
        self._load_config()
        self._load_template()

    def _get_default_config_path(self) -> str:
        """获取默认配置文件路径"""
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yml")

    def _load_config(self) -> None:
        """加载配置文件"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"配置文件不存在: {self.config_path}")
        
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)
        
        # 设置 id 初始值
        self.current_id = self.config.get("variables", {}).get("id_start", 1)

    def _load_template(self) -> None:
        """加载 Job 模板文件"""
        template_path = self.config.get("template", {}).get("file", "template.job")
        
        # 如果是相对路径，则相对于配置文件所在目录
        if not os.path.isabs(template_path):
            config_dir = os.path.dirname(os.path.abspath(self.config_path))
            template_path = os.path.join(config_dir, template_path)
        
        if not os.path.exists(template_path):
            raise FileNotFoundError(f"模板文件不存在: {template_path}")
        
        with open(template_path, "r", encoding="utf-8") as f:
            self.template = f.read()

    def _read_script_file(self, script_path: str) -> Dict[str, Any]:
        """
        读取 script 文件

        Args:
            script_path: script 文件路径

        Returns:
            script 文件内容的字典
        """
        with open(script_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _get_variables(self, script_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        获取所有可用变量

        Args:
            script_data: script 文件数据

        Returns:
            变量字典
        """
        variables: Dict[str, Any] = {}
        
        # 1. 添加 script 文件中的所有字段
        for key, value in script_data.items():
            variables[key] = value
        
        # 2. 添加自定义变量
        custom_vars = self.config.get("variables", {}).get("custom", {})
        if custom_vars:
            variables.update(custom_vars)
        
        # 3. 添加自增 id
        variables["id"] = str(self.current_id)
        
        # 4. 应用映射规则
        mappings = self.config.get("mappings", {})
        if mappings:
            for target_var, source_var in mappings.items():
                if source_var in variables:
                    variables[target_var] = variables[source_var]
                else:
                    variables[target_var] = source_var  # 作为固定值
        
        return variables

    def _replace_variables(self, template: str, variables: Dict[str, Any]) -> str:
        """
        替换模板中的变量

        Args:
            template: 模板字符串
            variables: 变量字典

        Returns:
            替换后的字符串
        """
        result = template
        
        # 查找所有 <变量名> 格式的占位符
        pattern = r"<(\w+)>"
        
        def replace_func(match: re.Match) -> str:
            var_name = match.group(1)
            if var_name in variables:
                value = variables[var_name]
                # 如果值是字符串，需要处理特殊字符
                if isinstance(value, str):
                    # 转义 JSON 特殊字符
                    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
                else:
                    return str(value)
            return match.group(0)  # 保留未匹配的变量
        
        result = re.sub(pattern, replace_func, result)
        return result

    def convert_file(self, script_path: str, output_path: str) -> None:
        """
        转换单个 script 文件为 job 文件

        Args:
            script_path: script 文件路径
            output_path: 输出 job 文件路径
        """
        # 读取 script 文件
        script_data = self._read_script_file(script_path)
        
        # 获取变量
        variables = self._get_variables(script_data)
        
        # 替换模板变量
        job_content = self._replace_variables(self.template, variables)
        
        # 验证生成的 JSON 是否有效
        try:
            json.loads(job_content)
        except json.JSONDecodeError as e:
            print(f"警告: 生成的 job 文件 JSON 格式无效: {output_path}")
            print(f"  错误: {e}")
        
        # 确保输出目录存在
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # 写入 job 文件
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(job_content)
        
        # 自增 id
        self.current_id += 1

    def convert_directory(
        self,
        script_dir: Optional[str] = None,
        output_dir: Optional[str] = None,
        pattern: Optional[str] = None,
        verbose: bool = False
    ) -> List[str]:
        """
        转换目录下的所有 script 文件

        Args:
            script_dir: script 文件目录，默认使用配置文件中的值
            output_dir: 输出目录，默认使用配置文件中的值
            pattern: 文件过滤规则，默认使用配置文件中的值
            verbose: 是否输出详细信息

        Returns:
            生成的 job 文件路径列表
        """
        # 使用参数或配置文件中的值
        script_dir = script_dir or self.config.get("input", {}).get("script_dir", "./script")
        output_dir = output_dir or self.config.get("output", {}).get("job_dir", "./job")
        pattern = pattern or self.config.get("input", {}).get("pattern", "*.script")
        
        # 如果是相对路径，则相对于配置文件所在目录
        config_dir = os.path.dirname(os.path.abspath(self.config_path))
        if not os.path.isabs(script_dir):
            script_dir = os.path.join(config_dir, script_dir)
        if not os.path.isabs(output_dir):
            output_dir = os.path.join(config_dir, output_dir)
        
        if not os.path.exists(script_dir):
            raise FileNotFoundError(f"Script 目录不存在: {script_dir}")
        
        generated_files: List[str] = []
        
        # 遍历目录
        for root, dirs, files in os.walk(script_dir):
            for filename in files:
                # 应用过滤规则
                if not fnmatch(filename, pattern):
                    continue
                
                script_path = os.path.join(root, filename)
                
                # 计算相对路径，保持目录结构
                rel_path = os.path.relpath(script_path, script_dir)
                
                # 生成输出路径，将 .script 替换为 .job
                output_filename = os.path.splitext(rel_path)[0] + ".job"
                output_path = os.path.join(output_dir, output_filename)
                
                try:
                    self.convert_file(script_path, output_path)
                    generated_files.append(output_path)
                    if verbose:
                        print(f"转换成功: {script_path} -> {output_path}")
                except Exception as e:
                    print(f"转换失败: {script_path}")
                    print(f"  错误: {e}")
        
        return generated_files

    def convert_single_file(
        self,
        script_path: str,
        output_dir: Optional[str] = None,
        verbose: bool = False
    ) -> Optional[str]:
        """
        转换单个 script 文件

        Args:
            script_path: script 文件路径
            output_dir: 输出目录，默认使用配置文件中的值
            verbose: 是否输出详细信息

        Returns:
            生成的 job 文件路径，失败返回 None
        """
        output_dir = output_dir or self.config.get("output", {}).get("job_dir", "./job")
        
        # 如果是相对路径，则相对于配置文件所在目录
        config_dir = os.path.dirname(os.path.abspath(self.config_path))
        if not os.path.isabs(output_dir):
            output_dir = os.path.join(config_dir, output_dir)
        
        if not os.path.exists(script_path):
            print(f"Script 文件不存在: {script_path}")
            return None
        
        # 生成输出路径
        filename = os.path.basename(script_path)
        output_filename = os.path.splitext(filename)[0] + ".job"
        output_path = os.path.join(output_dir, output_filename)
        
        try:
            self.convert_file(script_path, output_path)
            if verbose:
                print(f"转换成功: {script_path} -> {output_path}")
            return output_path
        except Exception as e:
            print(f"转换失败: {script_path}")
            print(f"  错误: {e}")
            return None


def main():
    """命令行入口函数"""
    parser = argparse.ArgumentParser(
        description="JobGen - Script 文件到 Job 文件转换工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 使用默认配置转换目录
  python jobgen.py

  # 指定配置文件
  python jobgen.py -c /path/to/config.yml

  # 转换指定目录
  python jobgen.py -i ./scripts -o ./jobs

  # 转换单个文件
  python jobgen.py -f ./script/query.script

  # 使用自定义过滤规则
  python jobgen.py -p "*.script"
        """
    )
    
    parser.add_argument(
        "-c", "--config",
        help="配置文件路径 (默认: config.yml)",
        default=None
    )
    parser.add_argument(
        "-i", "--input",
        help="Script 文件目录",
        default=None
    )
    parser.add_argument(
        "-o", "--output",
        help="Job 文件输出目录",
        default=None
    )
    parser.add_argument(
        "-f", "--file",
        help="转换单个 script 文件",
        default=None
    )
    parser.add_argument(
        "-p", "--pattern",
        help="文件过滤规则 (默认: *.script)",
        default=None
    )
    parser.add_argument(
        "-t", "--template",
        help="Job 模板文件路径",
        default=None
    )
    parser.add_argument(
        "--id-start",
        type=int,
        help="ID 自增长初始值",
        default=None
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="输出详细信息"
    )
    
    args = parser.parse_args()
    
    try:
        # 创建 JobGenerator 实例
        generator = JobGenerator(config_path=args.config)
        
        # 覆盖配置
        if args.template:
            generator.config["template"]["file"] = args.template
            generator._load_template()
        
        if args.id_start is not None:
            generator.current_id = args.id_start
        
        # 执行转换
        if args.file:
            # 转换单个文件
            result = generator.convert_single_file(
                script_path=args.file,
                output_dir=args.output,
                verbose=args.verbose
            )
            if result:
                print(f"完成! 生成文件: {result}")
            else:
                sys.exit(1)
        else:
            # 转换目录
            results = generator.convert_directory(
                script_dir=args.input,
                output_dir=args.output,
                pattern=args.pattern,
                verbose=args.verbose
            )
            print(f"完成! 共转换 {len(results)} 个文件")
            if args.verbose:
                for f in results:
                    print(f"  - {f}")
    
    except FileNotFoundError as e:
        print(f"错误: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"发生错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
