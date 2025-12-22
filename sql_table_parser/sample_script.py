#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
示例Python脚本，包含SQL语句
用于测试sql_table_parser解析.py文件时能正确过滤Python import语句
"""

import os
import sys
from pathlib import Path
from typing import List, Dict
from collections import OrderedDict

import pandas as pd
from sqlalchemy import create_engine, text


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


def update_inventory(engine, product_id: int, quantity: int):
    """更新库存"""
    sql = """
    UPDATE inventory
    SET quantity = quantity - :qty
    WHERE product_id = :product_id
    """
    with engine.connect() as conn:
        conn.execute(text(sql), {'qty': quantity, 'product_id': product_id})


def log_action(engine, action: str):
    """记录操作日志"""
    sql = """
    INSERT INTO action_logs (action, created_at)
    VALUES (:action, NOW())
    """
    with engine.connect() as conn:
        conn.execute(text(sql), {'action': action})


def cleanup_old_data(engine):
    """清理旧数据"""
    sql = """
    DELETE FROM temp_cache WHERE created_at < DATE_SUB(NOW(), INTERVAL 7 DAY)
    """
    with engine.connect() as conn:
        conn.execute(text(sql))


if __name__ == '__main__':
    # 创建数据库连接
    engine = create_engine('mysql://user:pass@localhost/mydb')
    
    # 执行查询
    orders = get_user_orders(engine, 1)
    print(orders)
