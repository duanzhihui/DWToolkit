# -*- coding: utf-8 -*-
"""
示例 Airflow 2.x DAG - 用于测试升级工具
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

# 获取变量
my_variable = Variable.get("my_variable", default_var="default_value")

# 默认参数
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def extract_data(**context):
    """提取数据"""
    execution_date = context['execution_date']
    print(f"Extracting data for {execution_date}")
    return {"data": [1, 2, 3]}


def transform_data(**context):
    """转换数据"""
    ti = context['ti']
    data = ti.xcom_pull(task_ids='extract')
    print(f"Transforming data: {data}")
    return {"transformed": data}


def load_data(**context):
    """加载数据"""
    ti = context['ti']
    data = ti.xcom_pull(task_ids='transform')
    print(f"Loading data: {data}")


with DAG(
    dag_id='sample_etl_dag_v2',
    default_args=default_args,
    description='示例 ETL DAG (Airflow 2.x 风格)',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    concurrency=10,
    tags=['example', 'etl'],
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
        provide_context=True,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        provide_context=True,
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_data,
        provide_context=True,
    )

    notify = BashOperator(
        task_id='notify',
        bash_command='echo "ETL completed for {{ execution_date }}"',
    )

    end = DummyOperator(
        task_id='end'
    )

    # 定义依赖关系
    start >> extract >> transform >> load >> notify >> end
