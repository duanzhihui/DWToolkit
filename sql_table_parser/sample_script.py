#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
示例Python脚本，包含SQL语句
用于测试sql_table_parser解析.py文件时能正确过滤Python import语句
同时演示变量表名的使用场景（Python f-string 格式）
"""

import os
import sys
from pathlib import Path
from typing import List, Dict
from collections import OrderedDict
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text


# 变量表名配置 - 用于动态指定schema和表名
# 这些变量会被 sql_table_parser 自动提取并用于替换 SQL 中的变量表名
schema = "dw"
ods_schema = "ods"
table_users = "dim_users"
table_orders = "fact_orders"
table_name = "dim_products"


def get_user_orders(engine, user_id: int):
    """查询用户订单"""
    sql = """
    SELECT u.id, u.name, o.order_id, o.amount
    FROM users u
    INNER JOIN orders o ON u.id = o.user_id
    WHERE u.id = :user_id
    """
    return pd.read_sql(sql, engine, params={'user_id': user_id})


def get_product_sales(engine):
    """查询产品销售统计"""
    sql = """
    WITH sales_summary AS (
        SELECT product_id, SUM(quantity) as total_qty
        FROM order_items
        GROUP BY product_id
    )
    SELECT p.name, p.price, s.total_qty
    FROM products p
    LEFT JOIN sales_summary s ON p.id = s.product_id
    """
    return pd.read_sql(sql, engine)


def get_user_orders_with_variable_table(engine, user_id: int):
    """使用变量表名查询用户订单 - Python f-string 格式"""
    sql = f"""
    SELECT u.id, u.name, o.order_id, o.amount
    FROM {schema}.{table_users} u
    INNER JOIN {schema}.{table_orders} o ON u.id = o.user_id
    WHERE u.id = :user_id
    """
    return pd.read_sql(sql, engine, params={'user_id': user_id})


def get_cross_schema_data(engine):
    """跨schema查询 - 混合变量和固定表名"""
    sql = f"""
    SELECT s.*, t.*
    FROM {ods_schema}.raw_users s
    LEFT JOIN {schema}.{table_name} t ON s.id = t.source_id
    """
    return pd.read_sql(sql, engine)


def create_partition_table(engine):
    """动态创建分区表"""
    sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
        id INT PRIMARY KEY,
        user_id INT,
        amount DECIMAL(10,2)
    )
    """
    with engine.connect() as conn:
        conn.execute(text(sql))


def update_inventory(engine, product_id: int, quantity: int):
    """更新库存"""
    sql = """
    UPDATE inventory
    SET quantity = quantity - :qty
    WHERE product_id = :product_id
    """
    with engine.connect() as conn:
        conn.execute(text(sql), {'qty': quantity, 'product_id': product_id})


def update_variable_table(engine):
    """使用变量表名更新"""
    sql = f"""
    UPDATE {schema}.{table_name}
    SET updated_at = NOW()
    WHERE status = 'active'
    """
    with engine.connect() as conn:
        conn.execute(text(sql))


def log_action(engine, action: str):
    """记录操作日志"""
    sql = """
    INSERT INTO action_logs (action, created_at)
    VALUES (:action, NOW())
    """
    with engine.connect() as conn:
        conn.execute(text(sql), {'action': action})


def insert_to_variable_table(engine):
    """使用变量表名插入数据"""
    sql = f"""
    INSERT INTO {schema}.audit_logs (action, table_name, executed_at)
    SELECT 'sync', '{table_name}', NOW()
    FROM dual
    """
    with engine.connect() as conn:
        conn.execute(text(sql))


def cleanup_old_data(engine):
    """清理旧数据"""
    sql = """
    DELETE FROM temp_cache WHERE created_at < DATE_SUB(NOW(), INTERVAL 7 DAY)
    """
    with engine.connect() as conn:
        conn.execute(text(sql))


def delete_from_variable_table(engine):
    """使用变量表名删除数据"""
    sql = f"""
    DELETE FROM {schema}.{table_name}
    WHERE order_date < DATE_SUB(NOW(), INTERVAL 1 YEAR)
    """
    with engine.connect() as conn:
        conn.execute(text(sql))


def truncate_variable_table(engine):
    """使用变量表名清空表"""
    sql = f"""
    TRUNCATE TABLE {schema}.{table_name}
    """
    with engine.connect() as conn:
        conn.execute(text(sql))


if __name__ == '__main__':
    # 创建数据库连接
    engine = create_engine('mysql://user:pass@localhost/mydb')
    
    # 执行普通查询
    orders = get_user_orders(engine, 1)
    print(orders)
    
    # 执行变量表名查询
    orders_var = get_user_orders_with_variable_table(engine, 1)
    print(orders_var)
    
    # 跨schema查询
    cross_data = get_cross_schema_data(engine)
    print(cross_data)
    
    # 创建分区表
    create_partition_table(engine)
    
    # 更新变量表
    update_variable_table(engine)
    
    # 插入到变量表
    insert_to_variable_table(engine)
    
    # 删除变量表数据
    delete_from_variable_table(engine)
