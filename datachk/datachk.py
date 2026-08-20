###############################################################################
## File       : datachk.py
## Purpose    : 在数据开发后获取数据做数据验证
## Title      : 数据验证
## Category   : 数据开发
## Version    : v0.2.0
## Company    : duanzhihui.com
## Author     : 段智慧
## Description: 数据验证
##                  数据验证 根据"脚本清单"获取数据做数据验证。
##                  支持多数据库：Oracle、MySQL、SQLServer、Hive/Impala
##                  采用工厂模式，通过配置文件管理数据库连接
## History    : 2025-05-21  v0.2.0  段智慧 采用工厂模式整合多数据库支持
###############################################################################

import datetime
import numpy as np
import xlwings as xw
import pandas as pd
import yaml
import os
import re
from abc import ABC, abstractmethod
from sqlalchemy import create_engine
import sshtunnel
from urllib.parse import urlparse


class DataChecker(ABC):
    """数据验证基类，定义接口规范"""
    
    def __init__(self, config):
        """初始化连接和配置"""
        self.config = config
        self.tunnel = None
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 初始化连接: {self.config['database']['conn_str']}")
        
        # 检查是否需要SSH隧道
        if 'ssh_id' in self.config['database'] and self.config['database']['ssh_id']:
            self._setup_ssh_tunnel()
        
        # 创建数据库连接
        self._create_connection()
    
    def _setup_ssh_tunnel(self):
        """设置SSH隧道"""
        ssh_id = self.config['database']['ssh_id']
        if ssh_id not in self.config.get('sshs', {}):
            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 警告: SSH隧道ID '{ssh_id}' 在配置中不存在")
            return
            
        ssh_config = self.config['sshs'][ssh_id]
        conn_str = self.config['database']['conn_str']
        db_url = urlparse(conn_str)
        db_host = None
        db_port = None
        
        # 从连接字符串中提取主机和端口
        if db_url.netloc:
            netloc_parts = db_url.netloc.split('@')
            if len(netloc_parts) > 1:
                host_port = netloc_parts[1].split(':')
                db_host = host_port[0]
                if len(host_port) > 1:
                    # 处理端口后可能带有路径的情况
                    port_str = host_port[1].split('/')[0]
                    db_port = int(port_str)
        elif 'HOST' in conn_str.upper():
            host_match = re.search(r'HOST=([^)]+)', conn_str, re.IGNORECASE)
            port_match = re.search(r'PORT=(\d+)', conn_str, re.IGNORECASE)
            if host_match:
                db_host = host_match.group(1)
            if port_match:
                db_port = int(port_match.group(1))
        
        if not db_host or not db_port:
            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 无法从连接字符串中提取主机和端口信息")
            raise ValueError("无法从连接字符串中提取主机和端口信息")
        
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 正在创建SSH隧道连接到 {ssh_config['host']}:{ssh_config['port']}")
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 远程数据库地址: {db_host}:{db_port}")
        
        # 测试SSH连接
        try:
            import paramiko
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                hostname=ssh_config['host'],
                port=int(ssh_config['port']),
                username=ssh_config['username'],
                password=ssh_config['password'],
                timeout=10
            )
            ssh.close()
            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} SSH 连接测试成功")
        except Exception as e:
            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} SSH 连接测试失败: {str(e)}")
            raise ValueError(f"无法连接到 SSH 服务器: {str(e)}")
        
        # 创建SSH隧道
        ssh_kwargs = {
            'ssh_username': ssh_config['username'],
            'ssh_password': ssh_config['password'],
            'remote_bind_address': (db_host, db_port),
            'local_bind_address': ('127.0.0.1', 0),
        }
        
        max_retries = 3
        for retry_count in range(max_retries):
            try:
                self.tunnel = sshtunnel.SSHTunnelForwarder(
                    (ssh_config['host'], int(ssh_config['port'])),
                    **ssh_kwargs
                )
                self.tunnel.start()
                
                local_port = self.tunnel.local_bind_port
                
                # 修改连接字符串
                if db_url.netloc:
                    old_host_port = f"{db_host}:{db_port}"
                    new_host_port = f"127.0.0.1:{local_port}"
                    conn_str = conn_str.replace(old_host_port, new_host_port)
                elif 'HOST' in conn_str.upper():
                    conn_str = re.sub(r'HOST=[^)]+', f'HOST=127.0.0.1', conn_str, flags=re.IGNORECASE)
                    conn_str = re.sub(r'PORT=\d+', f'PORT={local_port}', conn_str, flags=re.IGNORECASE)
                
                print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} SSH隧道已创建，本地端口: {local_port}")
                self.config['database']['conn_str'] = conn_str
                break
                
            except Exception as e:
                print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} SSH隧道创建失败 (尝试 {retry_count+1}/{max_retries}): {str(e)}")
                if self.tunnel:
                    try:
                        self.tunnel.stop()
                    except:
                        pass
                    self.tunnel = None
                
                if retry_count >= max_retries - 1:
                    raise ValueError(f"无法创建SSH隧道: {str(e)}")
                
                import time
                time.sleep(2)
    
    def _create_connection(self):
        """创建数据库连接"""
        try:
            self.engine = create_engine(self.config['database']['conn_str'])
            self.conn = self.engine.connect()
        except Exception as e:
            if self.tunnel:
                self.tunnel.stop()
                print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} SSH隧道已关闭（因数据库连接失败）")
            raise ValueError(f"无法连接到数据库: {str(e)}")
    
    def close(self):
        """关闭连接"""
        if hasattr(self, 'conn'):
            self.conn.close()
        if hasattr(self, 'engine'):
            self.engine.dispose()
        if hasattr(self, 'tunnel') and self.tunnel:
            self.tunnel.stop()
            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} SSH隧道已关闭")
    
    def execute_script(self, script):
        """执行SQL并返回DataFrame"""
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 执行脚本: {script}")
        try:
            data = pd.read_sql(script, self.conn)
            return pd.DataFrame(data, dtype=object), 2
        except Exception as e:
            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 执行异常: {repr(e)}")
            return pd.DataFrame(), 4


class OracleChecker(DataChecker):
    """Oracle数据验证实现"""
    pass


class MySQLChecker(DataChecker):
    """MySQL数据验证实现"""
    pass


class SQLServerChecker(DataChecker):
    """SQLServer数据验证实现"""
    pass


class HiveChecker(DataChecker):
    """Hive/Impala数据验证实现"""
    
    def _create_connection(self):
        """创建Hive/Impala连接"""
        try:
            # 使用impala.dbapi连接
            from impala.dbapi import connect
            
            conn_str = self.config['database']['conn_str']
            # 解析连接字符串 impala://user:pass@host:port/database
            db_url = urlparse(conn_str)
            
            host = db_url.hostname or 'localhost'
            port = db_url.port or 21050
            database = db_url.path.lstrip('/') or 'default'
            user = db_url.username or ''
            password = db_url.password or ''
            
            self.conn = connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                auth_mechanism="PLAIN" if user else "NOSASL"
            )
            self.engine = None  # Hive不使用SQLAlchemy engine
        except Exception as e:
            if self.tunnel:
                self.tunnel.stop()
                print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} SSH隧道已关闭（因数据库连接失败）")
            raise ValueError(f"无法连接到数据库: {str(e)}")
    
    def execute_script(self, script):
        """执行SQL并返回DataFrame"""
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 执行脚本: {script}")
        try:
            from impala.util import as_pandas
            cursor = self.conn.cursor()
            cursor.execute('set PARQUET_FALLBACK_SCHEMA_RESOLUTION=name')
            cursor.execute('set mem_limit=2048M')
            cursor.execute(script)
            data = as_pandas(cursor)
            data = pd.DataFrame(data, dtype=object)
            cursor.close()
            return data, 2
        except Exception as e:
            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 执行异常: {repr(e)}")
            return pd.DataFrame(), 4
    
    def close(self):
        """关闭连接"""
        if hasattr(self, 'conn'):
            self.conn.close()
        if hasattr(self, 'tunnel') and self.tunnel:
            self.tunnel.stop()
            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} SSH隧道已关闭")


class CheckerFactory:
    """数据验证器工厂类"""
    
    _checkers = {
        'oracle': OracleChecker,
        'mysql': MySQLChecker,
        'sqlserver': SQLServerChecker,
        'hive': HiveChecker,
        'impala': HiveChecker,
    }
    
    @classmethod
    def create_checker(cls, db_type, config):
        """创建数据验证器
        Args:
            db_type (str): 数据库类型
            config (dict): 配置信息
        Returns:
            DataChecker: 数据验证器实例
        """
        db_type = db_type.lower()
        if db_type not in cls._checkers:
            raise ValueError(f"不支持的数据库类型: {db_type}")
        return cls._checkers[db_type](config)


class DataChkApp:
    """数据验证应用类"""
    
    def __init__(self, config_file, conn_id=None, output_file=None):
        """初始化应用
        Args:
            config_file (str): 配置文件路径
            conn_id (str, optional): 连接ID. 默认None，使用配置文件中的默认连接
            output_file (str, optional): 输出Excel文件路径. 默认None，使用配置文件中的默认路径
        """
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, config_file)
        self._load_config(config_path, conn_id, output_file)
        self._init_excel()
        # 存储已连接的数据验证器
        self.checkers = {}
    
    def _load_config(self, config_file, conn_id=None, output_file=None):
        """加载配置
        Args:
            config_file (str): 配置文件路径
            conn_id (str, optional): 连接ID
            output_file (str, optional): 输出文件路径或输出ID
        """
        try:
            with open(config_file, encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
            
            # 处理输出文件路径
            if output_file:
                if output_file in self.config.get('outputs', {}):
                    self.output_path = self.config['outputs'][output_file]['path']
                else:
                    self.output_path = output_file
            else:
                default_output_id = self.config.get('default_output_id')
                if not default_output_id:
                    raise ValueError("未指定输出文件且配置文件中未找到默认输出ID")
                
                if default_output_id not in self.config.get('outputs', {}):
                    raise ValueError(f"在配置文件中未找到输出ID '{default_output_id}' 的配置")
                
                self.output_path = self.config['outputs'][default_output_id]['path']
            
            # 处理连接ID
            self.conn_id = conn_id or self.config.get('default_conn_id')
            if not self.conn_id:
                raise ValueError("未指定连接ID且配置文件中未找到默认连接ID")
            
            if 'connections' not in self.config or self.conn_id not in self.config['connections']:
                raise ValueError(f"在配置文件中未找到连接ID '{self.conn_id}' 的配置")
            
            # 确保sshs配置存在
            if 'sshs' not in self.config:
                self.config['sshs'] = {}
                
        except FileNotFoundError:
            raise ValueError(f"配置文件未找到: {config_file}")
        except yaml.YAMLError as e:
            raise ValueError(f"配置文件格式错误: {str(e)}")
        except Exception as e:
            raise ValueError(f"加载配置文件时出错: {str(e)}")
    
    def _init_excel(self):
        """初始化Excel"""
        self.wb = self._open_workbook()
        self.script_sht = self.wb.sheets["脚本清单"]
    
    def _open_workbook(self):
        """打开Excel工作簿"""
        Apps = xw.apps
        if Apps.count:
            app = Apps.active
        else:
            app = xw.App(visible=True, add_book=False)
        return app.books.open(self.output_path)
    
    def _get_checker(self, conn_id):
        """获取数据验证器，如果已存在则复用，否则创建新的
        Args:
            conn_id (str): 连接ID
        Returns:
            DataChecker: 数据验证器实例
        """
        if conn_id in self.checkers:
            return self.checkers[conn_id]
        
        if 'connections' not in self.config or conn_id not in self.config['connections']:
            raise ValueError(f"在配置文件中未找到连接ID '{conn_id}' 的配置")
        
        conn_config = self.config['connections'][conn_id]
        db_type = conn_config['type'].lower()
        
        # 创建数据库配置
        db_config = {'database': conn_config.copy()}
        
        # 添加SSH隧道配置
        if 'ssh_id' in conn_config and conn_config['ssh_id'] and 'sshs' in self.config:
            ssh_id = conn_config['ssh_id']
            if ssh_id in self.config['sshs']:
                db_config['sshs'] = self.config['sshs']
                print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 使用SSH隧道连接: {ssh_id}")
        
        # 使用工厂创建验证器
        checker = CheckerFactory.create_checker(db_type, db_config)
        self.checkers[conn_id] = checker
        return checker
    
    def _pre_sht(self, sht_name):
        """准备sheet
        Args:
            sht_name (str): sheet名称
        Returns:
            sheet: Excel sheet对象
        """
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 准备sheet: {sht_name}")
        try:
            sht = self.wb.sheets[sht_name]
        except:
            mb_sht = self.wb.sheets["模板"]
            mb_sht.api.Copy(Before=mb_sht.api)
            sht = self.wb.sheets["模板 (2)"]
            sht.name = sht_name
            self.wb.save()
        return sht
    
    def run(self):
        """运行数据验证
        
        脚本清单Excel结构（从第3行开始处理，第1-2行为表头）：
            A列(索引0): 连接id - 数据库连接ID，对应config.yml中的connections
            B列(索引1): 脚本名称 - 用于创建对应的sheet
            C列(索引2): 脚本分类
            D列(索引3): 脚本描述
            E列(索引4): 执行标示 - 1:待执行, 2:已完成, 4:异常
            G列(索引6): SCRIPT - SQL脚本
        """
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 开始数据验证")
        
        # 列索引定义
        COL_CONN_ID = 0      # A列：连接id
        COL_SCRIPT_NAME = 1  # B列：脚本名称
        COL_CATEGORY = 2     # C列：脚本分类
        COL_DESC = 3         # D列：脚本描述
        COL_STATUS = 4       # E列：执行标示
        COL_SCRIPT = 6       # G列：SCRIPT
        
        try:
            # 获取脚本清单 - 使用used_range获取实际使用的范围
            last_row = self.script_sht.used_range.last_cell.row
            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 脚本清单共 {last_row - 2} 行数据")
            
            if last_row < 3:
                print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 脚本清单为空，无数据需要处理")
                return
            
            # 从第3行开始读取数据（第1-2行是表头）
            script_list = self.script_sht.range((3, 1), (last_row, 7)).value
            
            # 确保script_list是二维列表
            if script_list is None:
                print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 脚本清单为空")
                return
            if not isinstance(script_list[0], (list, tuple)):
                script_list = [script_list]
            
            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 读取到 {len(script_list)} 条脚本记录")
            
            # 遍历脚本清单
            for i in range(len(script_list)):
                row = script_list[i]
                script_name = row[COL_SCRIPT_NAME]
                
                # 检查执行标示（E列，索引4）
                if row[COL_STATUS] == 1:
                    # 获取连接ID（A列），如果为空则使用默认连接
                    conn_id = row[COL_CONN_ID] if row[COL_CONN_ID] else self.conn_id
                    
                    print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 处理脚本 {i+1}: {script_name}, 连接: {conn_id}")
                    
                    try:
                        # 获取数据验证器
                        checker = self._get_checker(conn_id)
                        
                        # 执行脚本
                        script = row[COL_SCRIPT]
                        data, status = checker.execute_script(script)
                        script_list[i][COL_STATUS] = status
                        
                        if status == 2 and not data.empty:
                            # 准备sheet并写入数据
                            sht = self._pre_sht(script_name)
                            data_headers = list(data.columns)
                            data_rows = np.array(data)
                            sht.range('D2').value = data_headers
                            sht_row = sht.range('D2').end('down').row
                            sht_row = sht_row if sht.range('D3').value == '-' else sht_row + 1
                            sht.range(sht_row, 4).value = data_rows
                            self.wb.save()
                            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 已写入数据到sheet: {script_name}")
                        else:
                            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 脚本执行状态: {status}, 数据为空: {data.empty if status == 2 else 'N/A'}")
                    except Exception as e:
                        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 处理脚本异常: {str(e)}")
                        script_list[i][COL_STATUS] = 4
                else:
                    print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 跳过脚本 {i+1}: {script_name}, 执行标示={row[COL_STATUS]}")
            
            # 保存结果状态到E列（第5列），从第3行开始
            status_list = [[row[COL_STATUS]] for row in script_list]
            self.script_sht.range('E3').value = status_list
            self.wb.save()
            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 已保存执行状态")
            
        finally:
            # 关闭所有已连接的验证器
            for checker in self.checkers.values():
                checker.close()
        
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 结束数据验证")


# 运行入口
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='数据验证工具')
    parser.add_argument('--config', default='config.yml', help='配置文件路径')
    parser.add_argument('--conn_id', help='数据库连接ID')
    parser.add_argument('--output', help='输出Excel文件路径')
    
    args = parser.parse_args()
    
    app = DataChkApp(
        config_file=args.config,
        conn_id=args.conn_id,
        output_file=args.output
    )
    
    app.run()
