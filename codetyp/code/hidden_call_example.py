# -*- coding: utf-8 -*-
"""
隐藏调用示例
演示通过函数封装、动态执行等方式调用大数据框架
"""

import subprocess
import os


class DataPlatformClient:
    """数据平台客户端 - 封装各种大数据调用"""
    
    def __init__(self, platform='hive'):
        self.platform = platform
    
    def execute(self, query):
        """执行查询 - 隐藏了具体的调用方式"""
        if self.platform == 'hive':
            return self._execute_hive(query)
        elif self.platform == 'spark':
            return self._execute_spark(query)
        elif self.platform == 'flink':
            return self._execute_flink(query)
    
    def _execute_hive(self, query):
        """通过beeline执行Hive查询"""
        cmd = f'beeline -e "{query}"'
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.stdout
    
    def _execute_spark(self, query):
        """通过spark-submit执行"""
        # 动态生成Spark作业
        spark_code = f'''
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
result = spark.sql("{query}")
result.show()
'''
        with open('/tmp/spark_job.py', 'w') as f:
            f.write(spark_code)
        
        cmd = 'spark-submit --master yarn /tmp/spark_job.py'
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.stdout
    
    def _execute_flink(self, query):
        """通过flink run执行"""
        cmd = f'flink run -c com.example.SqlJob /path/to/flink-job.jar "{query}"'
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.stdout


def run_hive_sql(sql, host='hive-server', port=10000):
    """通用Hive SQL执行函数"""
    from pyhive import hive
    conn = hive.Connection(host=host, port=port)
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()


def execute_hive_query(query, use_beeline=True):
    """执行Hive查询的另一种封装"""
    if use_beeline:
        cmd = f'beeline -e "{query}"'
    else:
        cmd = f'hive -e "{query}"'
    return subprocess.check_output(cmd, shell=True)


def submit_spark_job(job_file, **kwargs):
    """提交Spark作业"""
    master = kwargs.get('master', 'yarn')
    memory = kwargs.get('executor_memory', '4g')
    
    cmd = f'spark-submit --master {master} --executor-memory {memory} {job_file}'
    return subprocess.run(cmd, shell=True, capture_output=True)


def run_flink_job(jar_path, main_class, args=None):
    """运行Flink作业"""
    cmd = f'flink run -c {main_class} {jar_path}'
    if args:
        cmd += f' {args}'
    return subprocess.run(cmd, shell=True, capture_output=True)


# 使用环境变量配置的隐藏调用
def get_data_from_warehouse(table_name):
    """从数据仓库获取数据 - 根据环境变量决定使用哪种引擎"""
    engine = os.environ.get('DW_ENGINE', 'hive')
    
    if engine == 'hive':
        return run_hive_sql(f"SELECT * FROM {table_name}")
    elif engine == 'spark':
        # 使用findspark初始化
        import findspark
        findspark.init()
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        return spark.sql(f"SELECT * FROM {table_name}").collect()
    elif engine == 'presto':
        import prestodb
        conn = prestodb.dbapi.connect(host='presto-server', port=8080)
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        return cursor.fetchall()


if __name__ == '__main__':
    # 使用封装的客户端
    client = DataPlatformClient('hive')
    result = client.execute("SELECT * FROM users LIMIT 10")
    print(result)
