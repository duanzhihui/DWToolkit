#!/usr/bin/env python
# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

ENV = "prod"
BASE_DIR = f"/data/{ENV}/etl"
SCRIPT_DIR = f"{BASE_DIR}/scripts"
BACKUP_DIR = "/data/backup"

dag_prefix = "etl"
dag_name = dag_prefix + "_" + ENV

task_prefix = "run"

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
    dag_id=dag_name,
    default_args=default_args,
    description='示例ETL DAG - 演示变量解析功能',
    schedule_interval='0 2 * * *',
    catchup=False,
) as dag:
    
    extract_task_id = f'{task_prefix}_extract'
    extract_command = f'cd {SCRIPT_DIR} && python extract.py --date {{{{ ds }}}}'
    
    task1 = BashOperator(
        task_id=extract_task_id,
        bash_command=extract_command
    )
    
    transform_dir = f"{BASE_DIR}/transform"
    task2 = BashOperator(
        task_id=f'{task_prefix}_transform',
        bash_command=f'cd {transform_dir} && sh transform.sh && python validate.py'
    )
    
    warehouse_dir = "/data/warehouse"
    task3 = BashOperator(
        task_id=f'{task_prefix}_load',
        bash_command=f'cd {warehouse_dir} && python load.py --mode incremental'
    )
    
    backup_script = "backup.sh"
    backup_command = f'cd {BACKUP_DIR} && sh {backup_script} {warehouse_dir} /backup/daily'
    
    task4 = BashOperator(
        task_id=f'{task_prefix}_backup',
        bash_command=backup_command
    )
    
    def send_notification():
        print(f"ETL process completed for {ENV}")
    
    notify_task_id = task_prefix + "_notify"
    task5 = PythonOperator(
        task_id=notify_task_id,
        python_callable=send_notification
    )
    
    task1 >> task2 >> task3 >> task4 >> task5
