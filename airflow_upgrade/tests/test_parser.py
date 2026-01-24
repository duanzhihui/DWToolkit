# -*- coding: utf-8 -*-
"""
DAG 解析器测试
"""

import pytest
from airflow_upgrade.core.parser import DAGParser, DAGStructure


class TestDAGParser:
    """DAG 解析器测试类"""
    
    @pytest.fixture
    def parser(self):
        return DAGParser()
    
    def test_parse_simple_dag(self, parser):
        """测试解析简单 DAG"""
        code = '''
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
    dag_id='simple_dag',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    task1 = PythonOperator(
        task_id='task1',
        python_callable=lambda: print("Hello")
    )
'''
        result = parser.parse_source(code)
        
        assert isinstance(result, DAGStructure)
        assert result.dag_config is not None
        assert result.dag_config.dag_id == 'simple_dag'
        assert result.dag_config.schedule_interval == "'@daily'"
        assert len(result.operators) == 1
        assert result.operators[0].task_id == 'task1'
    
    def test_parse_imports(self, parser):
        """测试解析导入语句"""
        code = '''
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import datetime
'''
        result = parser.parse_source(code)
        
        assert len(result.imports) >= 4
        modules = [imp.module for imp in result.imports]
        assert 'airflow' in modules
        assert 'airflow.operators.python_operator' in modules
    
    def test_parse_taskflow_dag(self, parser):
        """测试解析 TaskFlow API DAG"""
        code = '''
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id='taskflow_dag',
    schedule='@daily',
    start_date=datetime(2024, 1, 1)
)
def my_dag():
    
    @task
    def extract():
        return {"data": 1}
    
    @task
    def transform(data):
        return data
    
    data = extract()
    transform(data)

my_dag()
'''
        result = parser.parse_source(code)
        
        assert 'dag' in result.decorators
        assert 'task' in result.decorators
        assert result.dag_config is not None
    
    def test_parse_dependencies(self, parser):
        """测试解析任务依赖"""
        code = '''
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(dag_id='dep_dag') as dag:
    t1 = EmptyOperator(task_id='t1')
    t2 = EmptyOperator(task_id='t2')
    t3 = EmptyOperator(task_id='t3')
    
    t1 >> t2 >> t3
    t1 >> t3
'''
        result = parser.parse_source(code)
        
        assert len(result.dependencies) >= 2
    
    def test_parse_variables(self, parser):
        """测试解析 Airflow 变量"""
        code = '''
from airflow import DAG
from airflow.models import Variable

my_var = Variable.get("my_variable")
another_var = Variable.get("another_variable", default_var="default")
'''
        result = parser.parse_source(code)
        
        assert len(result.variables) >= 1
        assert 'my_variable' in result.variables
    
    def test_parse_connections(self, parser):
        """测试解析连接"""
        code = '''
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(dag_id='conn_dag') as dag:
    task = PostgresOperator(
        task_id='query',
        postgres_conn_id='my_postgres_conn',
        sql='SELECT 1'
    )
'''
        result = parser.parse_source(code)
        
        assert 'my_postgres_conn' in result.connections
    
    def test_detect_airflow_version_deprecated(self, parser):
        """测试检测使用已弃用导入的版本"""
        code = '''
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
'''
        result = parser.parse_source(code)
        
        assert result.airflow_version is not None
        assert '2.x' in result.airflow_version or 'deprecated' in result.airflow_version
    
    def test_detect_airflow_version_taskflow(self, parser):
        """测试检测使用 TaskFlow 的版本"""
        code = '''
from airflow.decorators import dag, task
'''
        result = parser.parse_source(code)
        
        assert result.airflow_version is not None
        assert '2.0' in result.airflow_version or '2.4' in result.airflow_version
    
    def test_parse_syntax_error(self, parser):
        """测试解析语法错误的代码"""
        code = '''
from airflow import DAG
def broken(
'''
        result = parser.parse_source(code)
        
        assert len(result.issues) > 0
        assert '语法错误' in result.issues[0]
    
    def test_parse_operators_with_params(self, parser):
        """测试解析带参数的操作符"""
        code = '''
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(dag_id='param_dag') as dag:
    task = PythonOperator(
        task_id='my_task',
        python_callable=my_func,
        op_args=['arg1', 'arg2'],
        op_kwargs={'key': 'value'},
        provide_context=True
    )
'''
        result = parser.parse_source(code)
        
        assert len(result.operators) == 1
        assert 'provide_context' in result.operators[0].parameters


class TestDAGParserEdgeCases:
    """边界情况测试"""
    
    @pytest.fixture
    def parser(self):
        return DAGParser()
    
    def test_empty_file(self, parser):
        """测试空文件"""
        result = parser.parse_source('')
        
        assert isinstance(result, DAGStructure)
        assert result.dag_config is None
        assert len(result.operators) == 0
    
    def test_non_dag_python_file(self, parser):
        """测试非 DAG Python 文件"""
        code = '''
def hello():
    print("Hello World")

if __name__ == "__main__":
    hello()
'''
        result = parser.parse_source(code)
        
        assert result.dag_config is None
        assert len(result.operators) == 0
    
    def test_multiple_dags(self, parser):
        """测试多个 DAG 定义"""
        code = '''
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(dag_id='dag1') as dag1:
    t1 = EmptyOperator(task_id='t1')

with DAG(dag_id='dag2') as dag2:
    t2 = EmptyOperator(task_id='t2')
'''
        result = parser.parse_source(code)
        
        # 应该能解析到操作符
        assert len(result.operators) >= 2
