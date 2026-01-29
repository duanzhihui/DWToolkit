#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import ast
import yaml
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DAGParser:
    """Airflow DAG文件解析器"""
    
    def __init__(self, config_path: str = "config.yml"):
        """初始化解析器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = self._load_config(config_path)
        self.results = []
        self.variables = {}
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """加载配置文件"""
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def parse(self):
        """执行解析"""
        input_path = self.config['input']['dag_path']
        
        if os.path.isfile(input_path):
            self._parse_file(input_path)
        elif os.path.isdir(input_path):
            self._parse_directory(input_path)
        else:
            logger.error(f"路径不存在: {input_path}")
            return
        
        self._export_to_excel()
        logger.info(f"解析完成,共解析 {len(self.results)} 个任务")
    
    def _parse_directory(self, directory: str):
        """解析目录中的所有DAG文件"""
        recursive = self.config['input'].get('recursive', True)
        extensions = self.config['input'].get('file_extensions', ['.py'])
        
        if recursive:
            for root, dirs, files in os.walk(directory):
                for file in files:
                    if any(file.endswith(ext) for ext in extensions):
                        file_path = os.path.join(root, file)
                        self._parse_file(file_path)
        else:
            for file in os.listdir(directory):
                if any(file.endswith(ext) for ext in extensions):
                    file_path = os.path.join(directory, file)
                    if os.path.isfile(file_path):
                        self._parse_file(file_path)
    
    def _parse_file(self, file_path: str):
        """解析单个DAG文件"""
        logger.info(f"解析文件: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content)
            
            dag_dir = os.path.dirname(os.path.abspath(file_path))
            dag_file = os.path.basename(file_path)
            
            self.variables = {}
            self._extract_variables(tree)
            
            dag_ids = self._extract_dag_ids(tree)
            
            tasks = self._extract_tasks(tree, dag_dir, dag_file, dag_ids)
            self.results.extend(tasks)
            
        except Exception as e:
            logger.error(f"解析文件失败 {file_path}: {str(e)}")
    
    def _extract_variables(self, tree: ast.AST):
        """提取文件中的变量定义"""
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        try:
                            value = ast.literal_eval(node.value)
                            self.variables[target.id] = value
                        except:
                            if isinstance(node.value, ast.Name):
                                var_name = node.value.id
                                if var_name in self.variables:
                                    self.variables[target.id] = self.variables[var_name]
    
    def _extract_dag_ids(self, tree: ast.AST) -> List[str]:
        """提取DAG ID"""
        dag_ids = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id == 'DAG':
                    for keyword in node.keywords:
                        if keyword.arg == 'dag_id':
                            try:
                                dag_id = ast.literal_eval(keyword.value)
                                dag_ids.append(dag_id)
                            except:
                                if isinstance(keyword.value, ast.Name):
                                    var_name = keyword.value.id
                                    if var_name in self.variables:
                                        dag_ids.append(self.variables[var_name])
            
            elif isinstance(node, ast.With):
                for item in node.items:
                    if isinstance(item.context_expr, ast.Call):
                        if isinstance(item.context_expr.func, ast.Name) and item.context_expr.func.id == 'DAG':
                            for keyword in item.context_expr.keywords:
                                if keyword.arg == 'dag_id':
                                    try:
                                        dag_id = ast.literal_eval(keyword.value)
                                        dag_ids.append(dag_id)
                                    except:
                                        if isinstance(keyword.value, ast.Name):
                                            var_name = keyword.value.id
                                            if var_name in self.variables:
                                                dag_ids.append(self.variables[var_name])
        
        return dag_ids if dag_ids else ['unknown']
    
    def _extract_tasks(self, tree: ast.AST, dag_dir: str, dag_file: str, dag_ids: List[str]) -> List[Dict[str, Any]]:
        """提取任务信息"""
        tasks = []
        task_types = self.config['parse'].get('task_types', [])
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    operator_name = node.func.id
                    
                    if operator_name in task_types or operator_name.endswith('Operator'):
                        task_info = self._parse_task(node, dag_dir, dag_file, dag_ids[0])
                        if task_info:
                            tasks.append(task_info)
                
                elif isinstance(node.func, ast.Attribute):
                    operator_name = node.func.attr
                    
                    if operator_name in task_types or operator_name.endswith('Operator'):
                        task_info = self._parse_task(node, dag_dir, dag_file, dag_ids[0])
                        if task_info:
                            tasks.append(task_info)
        
        return tasks
    
    def _parse_task(self, node: ast.Call, dag_dir: str, dag_file: str, dag_id: str) -> Optional[Dict[str, Any]]:
        """解析单个任务"""
        task_info = {
            'dag_path': dag_dir,
            'dag_file': dag_file,
            'dag_id': dag_id,
            'task_id': '',
            'command': '',
            'command_dir': '',
            'command_file': ''
        }
        
        for keyword in node.keywords:
            if keyword.arg == 'task_id':
                try:
                    task_info['task_id'] = ast.literal_eval(keyword.value)
                except:
                    if isinstance(keyword.value, ast.Name):
                        var_name = keyword.value.id
                        if var_name in self.variables:
                            task_info['task_id'] = self.variables[var_name]
                        else:
                            task_info['task_id'] = var_name
            
            elif keyword.arg in ['bash_command', 'python_callable', 'application']:
                command = self._extract_command(keyword.value)
                if command:
                    task_info['command'] = command
                    task_info['command_dir'] = self._extract_command_dir(command)
                    task_info['command_file'] = self._extract_command_file(command)
        
        if task_info['task_id']:
            return task_info
        return None
    
    def _extract_command(self, node: ast.AST) -> str:
        """提取命令内容"""
        try:
            return ast.literal_eval(node)
        except:
            if isinstance(node, ast.Name):
                var_name = node.id
                if var_name in self.variables:
                    return str(self.variables[var_name])
                return var_name
            elif isinstance(node, ast.Attribute):
                return f"{self._get_full_name(node)}"
            elif isinstance(node, ast.JoinedStr):
                parts = []
                for value in node.values:
                    if isinstance(value, ast.Constant):
                        parts.append(str(value.value))
                    elif isinstance(value, ast.FormattedValue):
                        if isinstance(value.value, ast.Name):
                            var_name = value.value.id
                            if var_name in self.variables:
                                parts.append(str(self.variables[var_name]))
                            else:
                                parts.append(f"{{{var_name}}}")
                return ''.join(parts)
            return ''
    
    def _get_full_name(self, node: ast.AST) -> str:
        """获取完整的属性名"""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            return f"{self._get_full_name(node.value)}.{node.attr}"
        return ''
    
    def _extract_command_dir(self, command: str) -> str:
        """提取命令中的cd目录"""
        if not command:
            return ''
        
        cd_pattern = self.config['parse']['command_patterns'].get('cd_pattern', r'cd\s+([^\s;&|]+)')
        matches = re.findall(cd_pattern, command)
        
        if matches:
            dir_path = matches[-1]
            
            if self.config['parse'].get('resolve_variables', True):
                dir_path = self._resolve_variables(dir_path)
            
            return dir_path
        
        return ''
    
    def _extract_command_file(self, command: str) -> str:
        """提取命令中执行的文件"""
        if not command:
            return ''
        
        script_pattern = self.config['parse']['command_patterns'].get(
            'script_pattern', 
            r'([^\s;&|]+\.(py|sh))(?:\s|$|;|&|\|)'
        )
        matches = re.findall(script_pattern, command)
        
        if matches:
            return matches[-1][0] if isinstance(matches[-1], tuple) else matches[-1]
        
        return ''
    
    def _resolve_variables(self, text: str) -> str:
        """解析文本中的变量"""
        var_pattern = r'\$\{?(\w+)\}?'
        
        def replace_var(match):
            var_name = match.group(1)
            if var_name in self.variables:
                return str(self.variables[var_name])
            return match.group(0)
        
        return re.sub(var_pattern, replace_var, text)
    
    def _export_to_excel(self):
        """导出结果到Excel"""
        if not self.results:
            logger.warning("没有解析到任何任务")
            return
        
        df = pd.DataFrame(self.results)
        
        columns = self.config['output'].get('columns', [])
        if columns:
            df = df[columns]
        
        output_file = self.config['output']['excel_file']
        sheet_name = self.config['output'].get('sheet_name', 'DAG任务信息')
        
        df.to_excel(output_file, sheet_name=sheet_name, index=False)
        logger.info(f"结果已导出到: {output_file}")


def main():
    """主函数"""
    config_file = "config.yml"
    
    if not os.path.exists(config_file):
        logger.error(f"配置文件不存在: {config_file}")
        return
    
    parser = DAGParser(config_file)
    parser.parse()


if __name__ == "__main__":
    main()
