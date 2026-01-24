# -*- coding: utf-8 -*-
"""
示例 Airflow 3.x DAG - 升级后的目标格式
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

# 获取变量 (建议在任务内部获取)
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
    logical_date = context['logical_date']
    print(f"Extracting data for {logical_date}")
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
    dag_id='sample_etl_dag_v3',
    default_args=default_args,
    description='示例 ETL DAG (Airflow 3.x 风格)',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=10,
    tags=['example', 'etl'],
) as dag:

    start = EmptyOperator(
        task_id='start'
    )

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_data,
    )

    notify = BashOperator(
        task_id='notify',
        bash_command='echo "ETL completed for {{ logical_date }}"',
    )

    end = EmptyOperator(
        task_id='end'
    )

    # 定义依赖关系
    start >> extract >> transform >> load >> notify >> end
