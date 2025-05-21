import mysql.connector
from mysql.connector import Error, pooling
import logging
import time
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

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
    "connect_timeout": 10,
    "connection_timeout": 10,
    "use_pure": True,
    "auth_plugin": 'mysql_native_password'
}

# 创建连接池
connection_pool = mysql.connector.pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=5,  # 连接池大小
    **dbconfig
)

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

def create_database_and_table():
    """
    创建数据库和表
    """
    connection = None
    try:
        # 从连接池获取连接
        logger.info("正在尝试连接到MySQL服务器...")
        connection = get_connection()
        
        if connection.is_connected():
            logger.info("成功连接到MySQL服务器")
            cursor = connection.cursor()
            
            # 创建数据库
            logger.info("正在创建数据库...")
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {os.getenv('MYSQL_DATABASE')}")
            logger.info("数据库创建成功")
            
            # 切换到新创建的数据库
            cursor.execute(f"USE {os.getenv('MYSQL_DATABASE')}")
            
            # 删除旧表（如果存在）
            logger.info("正在删除旧表...")
            cursor.execute("DROP TABLE IF EXISTS stock_data")
            logger.info("旧表删除成功")
            
            # 创建股票数据表
            logger.info("正在创建数据表...")
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
            logger.info("股票数据表创建成功")
            
    except Error as e:
        logger.error(f"创建数据库和表时发生错误: {str(e)}")
        if "Access denied" in str(e):
            logger.error("访问被拒绝，请确保：")
            logger.error("1. 用户名和密码正确")
            logger.error("2. 用户有远程访问权限")
            logger.error("3. MySQL服务器允许远程连接")
            logger.error("4. 防火墙允许MySQL连接（端口3306）")
        elif "Can't connect" in str(e):
            logger.error("无法连接到数据库服务器，请检查：")
            logger.error("1. 服务器地址是否正确")
            logger.error("2. 服务器是否在线")
            logger.error("3. 网络连接是否正常")
            logger.error("4. 防火墙是否允许连接")
        elif "Connection timed out" in str(e):
            logger.error("连接超时，请检查：")
            logger.error("1. 网络连接是否稳定")
            logger.error("2. 服务器是否响应")
            logger.error("3. 防火墙设置是否正确")
        raise
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            logger.info("MySQL连接已关闭")

if __name__ == "__main__":
    max_retries = 3
    retry_delay = 5  # 秒
    
    for attempt in range(max_retries):
        try:
            # 创建数据库和表
            create_database_and_table()
            
            print("\n数据库初始化完成！")
            print("\n请确保.env文件包含以下配置：")
            print("""
            MYSQL_HOST=115.190.28.164
            MYSQL_USER=stockuser
            MYSQL_PASSWORD=Xbbbuai1.
            MYSQL_DATABASE=stock_db
            """)
            break
            
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"\n初始化失败，{retry_delay}秒后进行第{attempt + 2}次尝试...")
                time.sleep(retry_delay)
            else:
                print(f"\n初始化失败，已达到最大重试次数: {str(e)}")
                print("\n请检查：")
                print("1. MySQL服务器是否运行")
                print("2. 网络连接是否正常")
                print("3. 用户名和密码是否正确")
                print("4. 用户是否有远程访问权限") 