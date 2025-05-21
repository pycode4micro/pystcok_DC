import requests
import json
import logging
from typing import Dict, Any, List
import time
import mysql.connector
from mysql.connector import Error, pooling
from datetime import datetime
import os
from dotenv import load_dotenv
from functools import lru_cache

# 加载环境变量
load_dotenv()

#该文件用来获取股票单页面列表数据，并上传到MySQL数据库
# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 数据库连接池配置
dbconfig = {
    "host": os.getenv('MYSQL_HOST'),
    "user": os.getenv('MYSQL_USER'),
    "password": os.getenv('MYSQL_PASSWORD'),
    "database": os.getenv('MYSQL_DATABASE'),
    "connect_timeout": 10,
    "connection_timeout": 10,
    "use_pure": True,
    "auth_plugin": 'mysql_native_password',
    "pool_size": 10,  # 增加连接池大小
    "pool_name": "mypool"
}

# 创建连接池
connection_pool = mysql.connector.pooling.MySQLConnectionPool(**dbconfig)

def get_connection():
    """
    从连接池获取数据库连接
    """
    try:
        connection = connection_pool.get_connection()
        if connection.is_connected():
            return connection
    except Error as e:
        logger.error(f"从连接池获取连接时发生错误: {str(e)}")
        raise

@lru_cache(maxsize=100)
def get_stock_list_one_page_request(page):
    """
    获取股票数据，使用内存缓存
    """
    url = 'https://push2.eastmoney.com/api/qt/clist/get'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    }
    params = {
        'np': 1,
        'fltt': 1,
        'invt': 2,
        'fs': 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048',
        'fields': 'f12,f13,f14,f1,f2,f4,f3,f152,f5,f6,f7,f15,f18,f16,f17,f10,f8,f9,f23',
        'fid': 'f3',
        'pn': page,
        'pz': 20,
        'po': 1,
        'dect': 1,
    }
    
    try:
        resp = requests.get(url=url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        logger.info(f"从API获取第{page}页数据")
        return data
    except Exception as e:
        logger.error(f"获取股票数据时发生错误: {str(e)}")
        return None

def create_stock_table(connection: mysql.connector.connection.MySQLConnection):
    """
    创建股票数据表（如果不存在）
    """
    try:
        cursor = connection.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS stock_data (
            stock_code VARCHAR(10),
            stock_name VARCHAR(100),
            current_price DECIMAL(10,2),
            change_percent DECIMAL(10,2),
            change_amount DECIMAL(10,2),
            volume BIGINT,
            amount DECIMAL(20,2),
            amplitude DECIMAL(10,2),      -- 振幅
            turnover_rate DECIMAL(10,2),  -- 换手率
            volume_ratio DECIMAL(10,2),   -- 量比
            high_price DECIMAL(10,2),     -- 最高
            low_price DECIMAL(10,2),      -- 最低
            open_price DECIMAL(10,2),     -- 今开
            pre_close DECIMAL(10,2),      -- 昨收
            market_cap DECIMAL(20,2),     -- 总市值
            pe_ratio DECIMAL(20,2),       -- 市盈率
            pb_ratio DECIMAL(20,2),       -- 市净率
            update_time DATETIME,
            PRIMARY KEY (stock_code, update_time)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
        cursor.execute(create_table_query)
        connection.commit()
        logger.info("股票数据表创建成功或已存在")
    except Error as e:
        logger.error(f"创建数据表时发生错误: {str(e)}")
        raise
    finally:
        if cursor:
            cursor.close()

def upload_stock_data_to_mysql(data: Dict[str, Any], connection: mysql.connector.connection.MySQLConnection) -> bool:
    """
    将股票数据上传到MySQL数据库
    """
    try:
        if 'data' not in data or 'diff' not in data['data']:
            logger.error("数据格式不正确")
            return False

        cursor = connection.cursor()
        insert_query = """
        INSERT INTO stock_data 
        (stock_code, stock_name, current_price, change_percent, change_amount, 
         volume, amount, amplitude, turnover_rate, volume_ratio,
         high_price, low_price, open_price, pre_close,
         pe_ratio, pb_ratio, update_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        stock_name = VALUES(stock_name),
        current_price = VALUES(current_price),
        change_percent = VALUES(change_percent),
        change_amount = VALUES(change_amount),
        volume = VALUES(volume),
        amount = VALUES(amount),
        amplitude = VALUES(amplitude),
        turnover_rate = VALUES(turnover_rate),
        volume_ratio = VALUES(volume_ratio),
        high_price = VALUES(high_price),
        low_price = VALUES(low_price),
        open_price = VALUES(open_price),
        pre_close = VALUES(pre_close),
        pe_ratio = VALUES(pe_ratio),
        pb_ratio = VALUES(pb_ratio)
        """
        
        current_time = datetime.now()
        batch_size = 50  # 增加批量插入的大小
        stock_values_list = []
        
        def safe_float(value, divisor=100):
            """安全地转换数值，处理'_'和None的情况"""
            if value == '_' or value is None:
                return None
            try:
                return float(value) / divisor
            except (ValueError, TypeError):
                return None

        def safe_int(value):
            """安全地转换整数，处理'_'和None的情况"""
            if value == '_' or value is None:
                return None
            try:
                return int(value)
            except (ValueError, TypeError):
                return None
        
        for stock in data['data']['diff']:
            stock_values = (
                stock.get('f12', ''),  # 股票代码
                stock.get('f14', ''),  # 股票名称
                safe_float(stock.get('f2')),  # 当前价格
                safe_float(stock.get('f3')),  # 涨跌幅
                safe_float(stock.get('f4')),  # 涨跌额
                safe_int(stock.get('f5')),  # 成交量
                safe_float(stock.get('f6')),  # 成交额
                safe_float(stock.get('f7')),  # 振幅
                safe_float(stock.get('f8')),  # 换手率
                safe_float(stock.get('f10')), # 量比
                safe_float(stock.get('f15')), # 最高
                safe_float(stock.get('f16')), # 最低
                safe_float(stock.get('f17')), # 今开
                safe_float(stock.get('f18')), # 昨收
                safe_float(stock.get('f9')),  # 市盈率
                safe_float(stock.get('f23')), # 市净率
                current_time
            )
            stock_values_list.append(stock_values)
            
            # 当达到批量大小时执行插入
            if len(stock_values_list) >= batch_size:
                cursor.executemany(insert_query, stock_values_list)
                connection.commit()
                stock_values_list = []
        
        # 插入剩余的数据
        if stock_values_list:
            cursor.executemany(insert_query, stock_values_list)
            connection.commit()
        
        logger.info(f"成功上传 {cursor.rowcount} 条股票数据")
        return True
        
    except Error as e:
        logger.error(f"上传数据到MySQL时发生错误: {str(e)}")
        return False
    finally:
        if cursor:
            cursor.close()

def process_and_upload_stock_page(page: int) -> bool:
    """
    获取单页股票数据并上传到MySQL数据库
    """
    try:
        # 获取股票数据（使用内存缓存）
        data = get_stock_list_one_page_request(page)
        if not data:
            logger.error("获取股票数据失败")
            return False
        
        # 检查数据是否为空
        if data.get('data') is None:
            logger.warning(f"第 {page} 页数据为空，停止处理")
            return False
        
        # 从连接池获取连接
        connection = get_connection()
        
        try:
            # 确保数据表存在
            create_stock_table(connection)
            
            # 上传到MySQL
            return upload_stock_data_to_mysql(data, connection)
            
        finally:
            if connection.is_connected():
                connection.close()
                logger.info("MySQL连接已关闭")
        
    except Exception as e:
        logger.error(f"处理第{page}页数据时发生错误: {str(e)}")
        return False

# 使用示例
if __name__ == "__main__":
    # 处理并上传第一页数据
    success = process_and_upload_stock_page(page=1)
    if success:
        print("数据上传成功")
    else:
        print("数据上传失败")   
    


