# -*- coding: utf-8 -*-
"""
Hive Python调用示例
演示通过Python调用Hive的各种方式
"""

from pyhive import hive
import subprocess


def connect_hive_direct():
    """直接连接HiveServer2"""
    conn = hive.Connection(
        host='hive-server',
        port=10000,
        username='hadoop',
        database='data_warehouse'
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM ods_user_info WHERE dt='2024-01-01' LIMIT 10")
    results = cursor.fetchall()
    return results


def execute_hive_query(sql):
    """执行Hive查询的封装函数"""
    conn = hive.Connection(host='hive-server', port=10000)
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()


def run_hive_sql(sql_file):
    """通过beeline执行SQL文件"""
    cmd = f'beeline -e "source {sql_file}"'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.stdout


def hive_cli_execute(query):
    """通过hive命令行执行查询"""
    cmd = f'hive -e "{query}"'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.stdout


class HiveClient:
    """Hive客户端封装类"""
    
    def __init__(self, host, port=10000, database='default'):
        self.host = host
        self.port = port
        self.database = database
        self.conn = None
    
    def connect(self):
        """建立连接"""
        self.conn = hive.Connection(
            host=self.host,
            port=self.port,
            database=self.database
        )
    
    def query(self, sql):
        """执行查询"""
        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    
    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()


def main():
    # 使用HiveClient
    hive_client = HiveClient('hive-server', 10000, 'data_warehouse')
    hive_client.connect()
    
    # 执行查询
    results = hive_client.query("""
        SELECT user_id, user_name, age
        FROM ods_user_info
        WHERE dt = '2024-01-01'
        LIMIT 100
    """)
    
    for row in results:
        print(row)
    
    hive_client.close()


if __name__ == '__main__':
    main()
