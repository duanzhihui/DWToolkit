# -*- coding: utf-8 -*-
"""
DAG 转换器测试
"""

import pytest
from airflowupdt.core.parser import DAGParser
from airflowupdt.core.transformer import DAGTransformer, TransformResult


class TestDAGTransformer:
    """DAG 转换器测试类"""
    
    @pytest.fixture
    def parser(self):
        return DAGParser()
    
    @pytest.fixture
    def transformer(self):
        return DAGTransformer(target_version="3.0")
    
    def test_transform_dummy_operator(self, parser, transformer):
        """测试 DummyOperator 转换"""
        code = '''
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

with DAG(dag_id='test_dag') as dag:
    task = DummyOperator(task_id='dummy_task')
'''
        dag_structure = parser.parse_source(code)
        result = transformer.transform(dag_structure)
        
        assert result.success
        assert 'EmptyOperator' in result.transformed_code
        assert 'DummyOperator' not in result.transformed_code or 'REMOVED' in result.transformed_code
    
    def test_transform_import_paths(self, parser, transformer):
        """测试导入路径转换"""
        code = '''
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
'''
        dag_structure = parser.parse_source(code)
        result = transformer.transform(dag_structure)
        
        assert result.success
        assert 'airflow.operators.python' in result.transformed_code
        assert 'airflow.operators.bash' in result.transformed_code
    
    def test_transform_schedule_interval(self, parser, transformer):
        """测试 schedule_interval 转换"""
        code = '''
from airflow import DAG

dag = DAG(
    dag_id='test_dag',
    schedule_interval='@daily'
)
'''
        dag_structure = parser.parse_source(code)
        result = transformer.transform(dag_structure)
        
        assert result.success
        assert 'schedule=' in result.transformed_code or 'schedule =' in result.transformed_code
    
    def test_transform_provide_context(self, parser, transformer):
        """测试移除 provide_context"""
        code = '''
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(dag_id='test_dag') as dag:
    task = PythonOperator(
        task_id='my_task',
        python_callable=my_func,
        provide_context=True
    )
'''
        dag_structure = parser.parse_source(code)
        result = transformer.transform(dag_structure)
        
        assert result.success
        assert 'provide_context' not in result.transformed_code
    
    def test_transform_contrib_imports(self, parser, transformer):
        """测试 contrib 导入转换"""
        code = '''
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.hooks.gcs_hook import GCSHook
'''
        dag_structure = parser.parse_source(code)
        result = transformer.transform(dag_structure)
        
        assert result.success
        assert 'airflow.providers.google' in result.transformed_code
    
    def test_transform_concurrency(self, parser, transformer):
        """测试 concurrency 参数转换"""
        code = '''
from airflow import DAG

dag = DAG(
    dag_id='test_dag',
    concurrency=10
)
'''
        dag_structure = parser.parse_source(code)
        result = transformer.transform(dag_structure)
        
        assert result.success
        assert 'max_active_tasks' in result.transformed_code
    
    def test_transform_generates_warnings(self, parser, transformer):
        """测试生成警告"""
        code = '''
from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator

with DAG(dag_id='test_dag') as dag:
    subdag = SubDagOperator(task_id='subdag')
'''
        dag_structure = parser.parse_source(code)
        result = transformer.transform(dag_structure)
        
        # SubDagOperator 应该生成警告
        assert len(result.warnings) > 0
    
    def test_transform_preserves_valid_code(self, parser, transformer):
        """测试保留有效代码"""
        code = '''
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def my_function():
    return "Hello"

with DAG(
    dag_id='valid_dag',
    schedule='@daily',
    start_date=datetime(2024, 1, 1)
) as dag:
    task = PythonOperator(
        task_id='my_task',
        python_callable=my_function
    )
'''
        dag_structure = parser.parse_source(code)
        result = transformer.transform(dag_structure)
        
        assert result.success
        # 已经是有效的代码,应该基本保持不变
        assert 'my_function' in result.transformed_code
        assert 'valid_dag' in result.transformed_code
    
    def test_transformation_summary(self, parser, transformer):
        """测试转换摘要"""
        code = '''
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
'''
        dag_structure = parser.parse_source(code)
        transformer.transform(dag_structure)
        
        summary = transformer.get_transformation_summary()
        assert isinstance(summary, dict)


class TestTransformResult:
    """转换结果测试"""
    
    def test_transform_result_success(self):
        """测试成功的转换结果"""
        result = TransformResult(
            success=True,
            transformed_code="# transformed code"
        )
        
        assert result.success
        assert result.transformed_code == "# transformed code"
        assert len(result.errors) == 0
    
    def test_transform_result_failure(self):
        """测试失败的转换结果"""
        result = TransformResult(
            success=False,
            transformed_code="",
            errors=["转换错误"]
        )
        
        assert not result.success
        assert len(result.errors) == 1
