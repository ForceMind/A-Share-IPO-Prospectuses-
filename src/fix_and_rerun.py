import os
import sys
import logging
from audit_and_clean import audit_and_clean
from downloader import Downloader
from config import DATA_DIR

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fix_and_rerun():
    # 1. 执行清理
    logger.info("=== 第一步：清理无效和错误文件 ===")
    audit_and_clean()
    
    # 2. 重新下载
    logger.info("=== 第二步：针对缺失文件重新启动下载 ===")
    stock_list_path = os.path.join(DATA_DIR, 'stock_list.csv')
    
    if not os.path.exists(stock_list_path):
        logger.error("找不到股票列表文件，无法继续")
        return

    downloader = Downloader()
    # 强制重新扫描所有缺失文件（Downloader 内部会跳过已存在的）
    downloader.run(stock_list_path)
    
    logger.info("=== 修复流程完成 ===")

if __name__ == '__main__':
    fix_and_rerun()
