# -*- coding: utf-8 -*-
"""
DAG 验证器测试
"""

import pytest
from airflowupdt.core.validator import DAGValidator, ValidationResult


class TestDAGValidator:
    """DAG 验证器测试类"""
    
    @pytest.fixture
    def validator(self):
        return DAGValidator(target_version="3.0")
    
    def test_validate_valid_code(self, validator):
        """测试验证有效代码"""
        code = '''
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
    dag_id='valid_dag',
    schedule='@daily',
    start_date=datetime(2024, 1, 1)
) as dag:
    task = PythonOperator(
        task_id='my_task',
        python_callable=lambda: None
    )
'''
        result = validator.validate_code(code)
        
        assert result.is_valid
        assert len(result.errors) == 0
    
    def test_validate_syntax_error(self, validator):
        """测试语法错误检测"""
        code = '''
from airflow import DAG
def broken(
'''
        result = validator.validate_code(code)
        
        assert not result.is_valid
        assert len(result.errors) > 0
    
    def test_validate_removed_imports(self, validator):
        """测试已移除导入检测"""
        code = '''
from airflow.operators.subdag_operator import SubDagOperator
'''
        result = validator.validate_code(code)
        
        assert len(result.errors) > 0
    
    def test_validate_contrib_imports(self, validator):
        """测试 contrib 导入检测"""
        code = '''
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
'''
        result = validator.validate_code(code)
        
        assert len(result.errors) > 0
        assert any('contrib' in e.lower() for e in result.errors)
    
    def test_validate_deprecated_params(self, validator):
        """测试已弃用参数检测"""
        code = '''
from airflow import DAG

dag = DAG(
    dag_id='test_dag',
    schedule_interval='@daily'
)
'''
        result = validator.validate_code(code)
        
        # schedule_interval 应该产生警告
        assert len(result.warnings) > 0
    
    def test_validate_subdag_operator(self, validator):
        """测试 SubDagOperator 检测"""
        code = '''
from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator

with DAG(dag_id='test') as dag:
    subdag = SubDagOperator(task_id='subdag')
'''
        result = validator.validate_code(code)
        
        assert len(result.errors) > 0
        assert any('SubDag' in e for e in result.errors)
    
    def test_validate_execution_date(self, validator):
        """测试 execution_date 检测"""
        code = '''
from airflow import DAG

def my_func(**context):
    date = context['execution_date']
    return date
'''
        result = validator.validate_code(code)
        
        assert len(result.warnings) > 0
    
    def test_validate_jinja_templates(self, validator):
        """测试 Jinja 模板检测"""
        code = '''
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id='test') as dag:
    task = BashOperator(
        task_id='task',
        bash_command='echo {{ execution_date }}'
    )
'''
        result = validator.validate_code(code)
        
        assert len(result.warnings) > 0
    
    def test_compatibility_score(self, validator):
        """测试兼容性评分"""
        code = '''
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(dag_id='test', schedule='@daily') as dag:
    task = PythonOperator(task_id='task', python_callable=lambda: None)
'''
        result = validator.validate_code(code)
        score = validator.get_compatibility_score(result)
        
        assert 'score' in score
        assert 'grade' in score
        assert score['score'] >= 0
        assert score['score'] <= 100
    
    def test_compatibility_grade(self, validator):
        """测试兼容性等级"""
        # 有效代码应该得到好成绩
        code = '''
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(dag_id='test', schedule='@daily') as dag:
    task = EmptyOperator(task_id='task')
'''
        result = validator.validate_code(code)
        score = validator.get_compatibility_score(result)
        
        assert score['grade'] in ['A', 'B', 'C', 'D', 'F']


class TestValidationResult:
    """验证结果测试"""
    
    def test_validation_result_valid(self):
        """测试有效的验证结果"""
        result = ValidationResult(is_valid=True, airflow3_compatible=True)
        
        assert result.is_valid
        assert result.airflow3_compatible
        assert len(result.errors) == 0
    
    def test_validation_result_invalid(self):
        """测试无效的验证结果"""
        result = ValidationResult(is_valid=False)
        result.errors.append("测试错误")
        
        assert not result.is_valid
        assert len(result.errors) == 1
