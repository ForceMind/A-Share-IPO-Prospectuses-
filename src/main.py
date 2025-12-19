import os
import sys
import time
import logging
import argparse
import pandas as pd
import threading
import json
from tqdm import tqdm

# Global Exception Handler to prevent flash-quit
def exception_hook(exctype, value, traceback):
    print(f"\n[FATAL ERROR] 脚本发生严重错误 (Critical Error):")
    print(f"{exctype.__name__}: {value}")
    import traceback as tb
    tb.print_tb(traceback)
    print("\n程序已停止。请截图并联系开发者。")
    input("按回车键退出... (Press Enter to exit)")
    sys.exit(1)

sys.excepthook = exception_hook

# Delayed imports to allow exception hook to catch import errors
try:
    from downloader import Downloader
    from extractor import ProspectusExtractor
    from config import DATA_DIR, PDF_DIR, OUTPUT_DIR
except ImportError as e:
    print(f"\n[ERROR] 依赖库加载失败: {e}")
    print("请尝试重新运行脚本，或者手动执行: pip install -r requirements.txt")
    input("按回车键退出...")
    sys.exit(1)

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(DATA_DIR, '..', 'logs', 'pipeline.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def save_results(all_dividends, processed_files=None):
    if all_dividends:
        df = pd.DataFrame(all_dividends)
        cols = ['code', 'name', 'year', 'amount', 'page', 'source_file', 'note']
        for c in cols:
            if c not in df.columns:
                df[c] = ''
        df = df[cols]
        
        output_file = os.path.join(OUTPUT_DIR, 'dividends_summary.xlsx')
        try:
            df.to_excel(output_file, index=False)
            logger.info(f"结果已更新至 {output_file}")
        except Exception as e:
            logger.error(f"保存Excel失败 (可能文件被打开?): {e}")

    if processed_files is not None:
        state_file = os.path.join(DATA_DIR, 'processed_files.json')
        try:
            with open(state_file, 'w', encoding='utf-8') as f:
                json.dump(list(processed_files), f)
        except Exception as e:
            logger.error(f"保存状态失败: {e}")

def load_state():
    state_file = os.path.join(DATA_DIR, 'processed_files.json')
    processed_files = set()
    all_dividends = []
    
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r', encoding='utf-8') as f:
                processed_files = set(json.load(f))
            logger.info(f"已加载 {len(processed_files)} 个已处理文件记录")
        except Exception as e:
            logger.error(f"加载状态失败: {e}")

    output_file = os.path.join(OUTPUT_DIR, 'dividends_summary.xlsx')
    if os.path.exists(output_file):
        try:
            df = pd.read_excel(output_file)
            all_dividends = df.to_dict('records')
            logger.info(f"已加载 {len(all_dividends)} 条现有结果")
            
            # 识别提取失败（金额为0或N/A）的代码，从 processed_files 中移除，以便重试
            # 注意：只有当 PDF 确实存在时才重试
            failed_codes = set()
            for item in all_dividends:
                if str(item.get('amount')) == '0' or item.get('year') == 'N/A':
                    failed_codes.add(str(item.get('code')).zfill(6))
            
            if failed_codes:
                logger.info(f"发现 {len(failed_codes)} 个提取失败的记录，准备尝试重新解析...")
                to_remove = []
                for f in processed_files:
                    code = f.split('_')[0]
                    if code in failed_codes:
                        to_remove.append(f)
                
                for f in to_remove:
                    processed_files.remove(f)
                
                # 从 all_dividends 中移除这些失败记录，避免重复
                all_dividends = [item for item in all_dividends if str(item.get('code')).zfill(6) not in failed_codes]
                logger.info(f"已从处理列表中移除 {len(to_remove)} 个文件，将重新尝试解析")
                
        except Exception as e:
            logger.error(f"加载现有Excel失败: {e}")
            
    return processed_files, all_dividends

def generate_report(stock_list_path):
    logger.info("正在生成最终状态报告...")
    if not os.path.exists(stock_list_path):
        return

    try:
        stocks_df = pd.read_csv(stock_list_path)
        stocks_df['code'] = stocks_df['code'].apply(lambda x: str(x).zfill(6))
        
        output_file = os.path.join(OUTPUT_DIR, 'dividends_summary.xlsx')
        extraction_map = {} 
        if os.path.exists(output_file):
            res_df = pd.read_excel(output_file)
            res_df['code'] = res_df['code'].apply(lambda x: str(x).zfill(6))
            for _, row in res_df.iterrows():
                code = row['code']
                if code not in extraction_map:
                    extraction_map[code] = {'has_data': False, 'max_amount': 0}
                
                # Use pd.isna() or similar to check for valid amount
                try:
                    amt = float(row['amount'])
                    if amt > 0:
                        extraction_map[code]['has_data'] = True
                        extraction_map[code]['max_amount'] = max(extraction_map[code]['max_amount'], amt)
                except (ValueError, TypeError):
                    pass

        report_data = []
        pdf_files_set = set(os.listdir(PDF_DIR))
        
        for _, row in stocks_df.iterrows():
            code = row['code']
            name = row['name']
            industry = row.get('industry', 'Unknown') # Include industry in report
            
            pdf_exists = False
            for f in pdf_files_set:
                if f.startswith(code) and f.endswith('.pdf'):
                    pdf_exists = True
                    break
            
            status = 'Unknown'
            detail = ''
            
            if not pdf_exists:
                status = 'Missing PDF'
                detail = '未下载到招股书'
            else:
                if code in extraction_map:
                    info = extraction_map[code]
                    if info['has_data']:
                        status = 'Success'
                        detail = f"提取到分红 (最大 {info['max_amount']} 万元)"
                    else:
                        status = 'No Data'
                        detail = '提取结果为0或空'
                else:
                    status = 'Pending'
                    detail = '已下载但尚未解析'

            report_data.append({
                'code': code,
                'name': name,
                'industry': industry,
                'status': status,
                'detail': detail
            })
            
        report_df = pd.DataFrame(report_data)
        report_path = os.path.join(OUTPUT_DIR, 'status_report.csv')
        report_df.to_csv(report_path, index=False, encoding='utf-8-sig')
        logger.info(f"状态报告已保存至 {report_path}")
    except Exception as e:
        logger.error(f"生成报告失败: {e}")

def process_file(pdf_file, extractor, all_dividends):
    try:
        stock_code = pdf_file.split('_')[0]
        stock_name = pdf_file.split('_')[1].replace('.pdf', '') if '_' in pdf_file else 'Unknown'
        pdf_path = os.path.join(PDF_DIR, pdf_file)
        
        logger.info(f"正在解析: {pdf_file}")
        dividends = extractor.extract(pdf_path)
        
        if dividends:
            for div in dividends:
                div['code'] = stock_code
                div['name'] = stock_name
                div['source_file'] = pdf_file
                all_dividends.append(div)
        else:
            all_dividends.append({
                'code': stock_code,
                'name': stock_name,
                'year': 'N/A',
                'amount': 0,
                'page': 'N/A',
                'source_file': pdf_file,
                'note': '未提取到数据'
            })
    except Exception as e:
        logger.error(f"处理文件失败 {pdf_file}: {e}")
    return True

def run_pipeline(action='all', limit=None, csv_file='stock_list.csv', parallel=False):
    stock_list_path = os.path.join(DATA_DIR, csv_file)
    
    if not os.path.exists(stock_list_path):
        logger.error("股票列表不存在，请先运行 src/get_stock_list.py")
        return

    processed_files, all_dividends = load_state()

    if parallel and action == 'all':
        logger.info("=== 启动并行模式 (一边下载一边解析) ===")
        
        extractor = ProspectusExtractor()
        downloader = Downloader()
        
        download_thread = threading.Thread(target=downloader.run, args=(stock_list_path,))
        download_thread.daemon = True 
        download_thread.start()
        
        logger.info("下载线程已启动，主线程开始监听文件...")
        
        try:
            while download_thread.is_alive():
                current_files = set(f for f in os.listdir(PDF_DIR) if f.endswith('.pdf'))
                new_files = current_files - processed_files
                
                if new_files:
                    logger.info(f"发现 {len(new_files)} 个新文件待处理")
                    for f in sorted(list(new_files)):
                        if limit and len(processed_files) >= limit: break
                        
                        process_file(f, extractor, all_dividends)
                        processed_files.add(f)
                        
                        if len(processed_files) % 5 == 0:
                            save_results(all_dividends, processed_files)
                            
                    if limit and len(processed_files) >= limit:
                        logger.info("达到处理限制，停止")
                        break
                else:
                    time.sleep(2)
            
            logger.info("下载线程已结束，进行最终文件扫描...")
            current_files = set(f for f in os.listdir(PDF_DIR) if f.endswith('.pdf'))
            new_files = current_files - processed_files
            for f in sorted(list(new_files)):
                if limit and len(processed_files) >= limit: break
                process_file(f, extractor, all_dividends)
                processed_files.add(f)
            
            save_results(all_dividends, processed_files)
            generate_report(stock_list_path)
            logger.info("全部完成")
            
        except KeyboardInterrupt:
            logger.info("用户中断，保存结果...")
            save_results(all_dividends, processed_files)
            generate_report(stock_list_path)
            return
        except Exception as e:
            logger.critical(f"主循环发生未捕获异常: {e}")
            save_results(all_dividends, processed_files)
            raise

        return

    # Serial Mode
    if action in ['all', 'download']:
        logger.info("=== 开始下载阶段 ===")
        downloader = Downloader()
        downloader.run(stock_list_path)

    if action in ['all', 'extract']:
        logger.info("=== 开始解析阶段 ===")
        
        pdf_files = [f for f in os.listdir(PDF_DIR) if f.endswith('.pdf')]
        pdf_files = [f for f in pdf_files if f not in processed_files]
        
        if limit:
            pdf_files = pdf_files[:limit]
            
        logger.info(f"发现 {len(pdf_files)} 个未处理文件")
        
        if parallel:
            logger.info("=== 启动多进程解析模式 (Multiprocessing) ===")
            import multiprocessing
            # Import process_pdf_worker from src.extractor but handle potential circular or direct import issues
            try:
                from src.extractor import process_pdf_worker
            except ImportError:
                # Fallback if run as script
                from extractor import process_pdf_worker
            
            # Use 60% of CPU cores
            num_cores = max(1, int(multiprocessing.cpu_count() * 0.6))
            logger.info(f"使用 {num_cores} 个进程进行解析")
            
            # Using starmap requires arguments as tuples
            tasks = [(f, PDF_DIR) for f in pdf_files]
            
            with multiprocessing.Pool(processes=num_cores) as pool:
                # Use starmap to pass multiple arguments
                results = list(tqdm(pool.starmap(process_pdf_worker, tasks), total=len(tasks)))
                
                for pdf_file, dividends, error in results:
                    if error:
                        logger.error(f"处理文件失败 {pdf_file}: {error}")
                        # Don't mark as processed if error is severe, or maybe do? 
                        # For now, let's assume we want to skip it in next run if it's broken, 
                        # but here we just log. If we add to processed_files it won't be retried.
                        # Let's add it to processed_files so we don't get stuck on one file.
                    
                    if dividends:
                        all_dividends.extend(dividends)
                    
                    processed_files.add(pdf_file)
                    
                    if len(processed_files) % 100 == 0:
                         save_results(all_dividends, processed_files)
        else:
            # Original Serial Implementation
            extractor = ProspectusExtractor()
            for pdf_file in tqdm(pdf_files):
                process_file(pdf_file, extractor, all_dividends)
                processed_files.add(pdf_file)
                if len(processed_files) % 10 == 0:
                    save_results(all_dividends, processed_files)

        save_results(all_dividends, processed_files)
        generate_report(stock_list_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--action', choices=['download', 'extract', 'all'], default='all')
    parser.add_argument('--limit', type=int, help='限制处理数量(用于测试)', default=None)
    parser.add_argument('--csv', help='指定股票列表CSV文件', default='stock_list.csv')
    parser.add_argument('--parallel', action='store_true', help='启用并行模式：一边下载一边解析')
    args = parser.parse_args()
    
    run_pipeline(args.action, args.limit, args.csv, args.parallel)
