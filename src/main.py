import os
import sys

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import logging
import argparse
import traceback as tb
from src.config import DATA_DIR, LOG_FORMAT
from src.pipeline_utils import process_file_serial, save_results, load_state, generate_report
# from src.task_manager import task_manager # Delayed import

# Global Exception Handler to prevent flash-quit
def exception_hook(exctype, value, traceback):
    print(f"\n[FATAL ERROR] 脚本发生严重错误 (Critical Error):")
    print(f"{exctype.__name__}: {value}")
    tb.print_tb(traceback)
    print("\n程序已停止。请截图并联系开发者。")
    # Only wait for input if not in a CI/automated environment to avoid hanging
    if sys.stdin.isatty():
        input("按回车键退出... (Press Enter to exit)")
    sys.exit(1)

sys.excepthook = exception_hook

# Delayed imports to allow exception hook to catch import errors
try:
    from src.downloader import Downloader
    from src.extractor import ProspectusExtractor
except ImportError as e:
    print(f"\n[ERROR] 依赖库加载失败: {e}")
    print("请尝试重新运行脚本，或者手动执行: pip install -r requirements.txt")
    if sys.stdin.isatty():
        input("按回车键退出...")
    sys.exit(1)

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(os.path.join(DATA_DIR, '..', 'logs', 'pipeline.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_pipeline(action='all', limit=None, csv_file='stock_list.csv', parallel=False):
    """
    Main entry point for pipeline execution.
    Delegates to TaskManager for parallel execution, or runs locally for serial execution.
    """
    stock_list_path = os.path.join(DATA_DIR, csv_file)
    
    if not os.path.exists(stock_list_path):
        logger.error("股票列表不存在，请先运行 src/get_stock_list.py")
        return

    # If parallel mode is requested, use TaskManager
    if parallel:
        # Note: parallel mode argument from CLI starts the backend task manager logic in this process.
        # But if we use the Web Server, the server starts its own TaskManager instance.
        # Here we assume CLI usage.
        
        logger.info(f"=== 启动并行模式 [PID:{os.getpid()}] (Action: {action}, Limit: {limit}) ===")
        
        # Delayed import and instantiation to avoid multiprocessing fork issues
        from src.task_manager import get_task_manager
        task_manager = get_task_manager()
        
        # Start the tasks
        task_manager.start_tasks(action=action, limit=limit)
        
        # Monitor the tasks until completion
        try:
            while task_manager.status["is_running"]:
                status = task_manager.get_status()
                # Print status updates occasionally or just wait
                # Since TaskManager logs to root logger via _log_listener, we should see output.
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("用户中断，正在停止任务...")
            task_manager.stop_tasks()
            # Wait a bit for cleanup
            time.sleep(2)
            
        return

    # Serial Mode (Legacy / Fallback)
    logger.info(f"=== 启动串行模式 [PID:{os.getpid()}] (Action: {action}, Limit: {limit}) ===")
    
    # Audit phase for serial mode if requested (or included in all)
    if action in ['audit']:
        # Standalone audit
        from src.audit_and_clean import check_and_fix_pdf_type
        check_and_fix_pdf_type()
        return

    processed_files, all_dividends = load_state()

    # 1. Download Phase
    if action in ['all', 'download']:
        logger.info("=== 开始下载阶段 ===")
        downloader = Downloader()
        try:
            downloader.run(stock_list_path)
        except Exception as e:
            logger.error(f"下载阶段失败: {e}")
            if action == 'download': return

    # 2. Extraction Phase
    if action in ['all', 'extract']:
        logger.info("=== 开始解析阶段 ===")
        from src.config import PDF_DIR
        
        if not os.path.exists(PDF_DIR):
             os.makedirs(PDF_DIR)

        pdf_files = [f for f in os.listdir(PDF_DIR) if f.endswith('.pdf')]
        pdf_files = [f for f in pdf_files if f not in processed_files]
        
        if limit:
            pdf_files = pdf_files[:limit]
            
        logger.info(f"发现 {len(pdf_files)} 个未处理文件")
        
        extractor = ProspectusExtractor()
        
        try:
            count = 0
            for pdf_file in pdf_files:
                process_file_serial(pdf_file, extractor, all_dividends)
                processed_files.add(pdf_file)
                count += 1
                
                if count % 10 == 0:
                    save_results(all_dividends, processed_files)
                    
        except KeyboardInterrupt:
            logger.info("用户中断，保存结果...")
        finally:
            save_results(all_dividends, processed_files)
            generate_report(stock_list_path)
            
    logger.info("任务完成")

if __name__ == '__main__':
    # Add freeze_support() for Windows multiprocessing support
    from multiprocessing import freeze_support
    freeze_support()
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--action', choices=['download', 'extract', 'all'], default='all')
    parser.add_argument('--limit', type=int, help='限制处理数量(用于测试)', default=None)
    parser.add_argument('--csv', help='指定股票列表CSV文件', default='stock_list.csv')
    parser.add_argument('--parallel', action='store_true', help='启用并行模式：一边下载一边解析')
    args = parser.parse_args()
    
    run_pipeline(args.action, args.limit, args.csv, args.parallel)
