import asyncio
import aiohttp
import logging
from datetime import datetime
from .stock_simple import process_and_upload_stock_page
import time
import random
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

# MySQL配置
mysql_config = {
    "host": os.getenv('MYSQL_HOST'),
    "user": os.getenv('MYSQL_USER'),
    "password": os.getenv('MYSQL_PASSWORD'),
    "database": os.getenv('MYSQL_DATABASE')
}

async def process_page(page: int, semaphore: asyncio.Semaphore) -> tuple[bool, bool]:
    """
    处理单页数据
    
    Args:
        page: 页码
        semaphore: 信号量，用于控制并发
    
    Returns:
        tuple[bool, bool]: (处理是否成功, 是否数据为空)
    """
    async with semaphore:
        try:
            # 添加随机延迟，遵守robots协议
            delay = random.uniform(1.0, 3.0)
            await asyncio.sleep(delay)
            
            logger.info(f"开始处理第 {page} 页数据")
            success = process_and_upload_stock_page(page)
            
            if success:
                logger.info(f"第 {page} 页数据处理成功")
                return True, False
            else:
                logger.error(f"第 {page} 页数据处理失败")
                return False, False  # 修改为False，因为失败不一定是数据为空
            
        except Exception as e:
            logger.error(f"处理第 {page} 页数据时发生错误: {str(e)}")
            return False, False

async def process_all_pages(start_page: int = 1, max_concurrent: int = 3, max_retries: int = 3):
    """
    异步处理所有页面，直到遇到空数据或达到最大重试次数
    
    Args:
        start_page: 起始页码
        max_concurrent: 最大并发数
        max_retries: 最大重试次数
    """
    # 创建信号量，控制并发数
    semaphore = asyncio.Semaphore(max_concurrent)
    
    current_page = start_page
    empty_data_found = False
    success_count = 0
    fail_count = 0
    retry_count = 0
    
    while not empty_data_found and retry_count < max_retries:
        # 创建当前批次的任务
        tasks = []
        for _ in range(max_concurrent):
            if empty_data_found:
                break
            task = asyncio.create_task(process_page(current_page, semaphore))
            tasks.append(task)
            current_page += 1
        
        if not tasks:
            break
            
        # 等待当前批次的任务完成
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 处理结果
        all_failed = True
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"任务执行出错: {str(result)}")
                continue
                
            success, is_empty = result
            if success:
                all_failed = False
                success_count += 1
            else:
                fail_count += 1
        
        # 如果所有任务都失败，增加重试计数
        if all_failed:
            retry_count += 1
            logger.warning(f"当前批次所有任务失败，重试次数: {retry_count}/{max_retries}")
        else:
            retry_count = 0  # 重置重试计数
    
    if retry_count >= max_retries:
        logger.error(f"达到最大重试次数 {max_retries}，停止处理")
    
    logger.info(f"数据处理完成！成功: {success_count}, 失败: {fail_count}, 最后处理的页码: {current_page - 1}")

async def main():
    """
    主函数
    """
    try:
        # 验证环境变量
        required_env_vars = ['MYSQL_HOST', 'MYSQL_USER', 'MYSQL_PASSWORD', 'MYSQL_DATABASE']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"缺少必要的环境变量: {', '.join(missing_vars)}")
            
        start_time = time.time()
        logger.info("开始批量处理股票数据...")
        
        # 设置处理范围（从第1页开始，直到遇到空数据）
        await process_all_pages(start_page=1, max_concurrent=3, max_retries=3)
        
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"所有数据处理完成，总耗时: {duration:.2f} 秒")
        
    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")

if __name__ == "__main__":
    # 运行异步主函数
    asyncio.run(main()) 