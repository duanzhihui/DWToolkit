#!/usr/bin/env python
# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

BASE_DIR = "/data/etl"
SCRIPT_DIR = "/opt/scripts"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='example_etl_dag',
    default_args=default_args,
    description='示例ETL DAG',
    schedule_interval='0 2 * * *',
    catchup=False,
) as dag:
    
    task1 = BashOperator(
        task_id='extract_data',
        bash_command=f'cd {BASE_DIR}/extract && python extract.py --date {{{{ ds }}}}'
    )
    
    task2 = BashOperator(
        task_id='transform_data',
        bash_command=f'cd {BASE_DIR}/transform && sh transform.sh && python validate.py'
    )
    
    task3 = BashOperator(
        task_id='load_data',
        bash_command='cd /data/warehouse && python load.py --mode incremental'
    )
    
    task4 = BashOperator(
        task_id='backup_data',
        bash_command=f'cd {SCRIPT_DIR} && sh backup.sh /data/warehouse /backup/daily'
    )
    
    def send_notification():
        print("ETL process completed")
    
    task5 = PythonOperator(
        task_id='send_notification',
        python_callable=send_notification
    )
    
    task1 >> task2 >> task3 >> task4 >> task5
