import os
import argparse
import pandas as pd
import logging
from tqdm import tqdm
from downloader import Downloader
from extractor import ProspectusExtractor
from config import DATA_DIR, PDF_DIR, OUTPUT_DIR

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

def run_pipeline(action='all', limit=None, csv_file='stock_list.csv'):
    stock_list_path = os.path.join(DATA_DIR, csv_file)
    
    if not os.path.exists(stock_list_path):
        logger.error("股票列表不存在，请先运行 src/get_stock_list.py")
        return

    # Step 1: Download
    if action in ['all', 'download']:
        logger.info("=== 开始下载阶段 ===")
        downloader = Downloader()
        downloader.run(stock_list_path)

    # Step 2: Extract
    if action in ['all', 'extract']:
        logger.info("=== 开始解析阶段 ===")
        extractor = ProspectusExtractor()
        
        # 获取所有 PDF 文件
        pdf_files = [f for f in os.listdir(PDF_DIR) if f.endswith('.pdf')]
        if limit:
            pdf_files = pdf_files[:limit]
            
        logger.info(f"发现 {len(pdf_files)} 个 PDF 文件待处理")
        
        all_dividends = []
        
        for pdf_file in tqdm(pdf_files):
            stock_code = pdf_file.split('_')[0]
            # 从文件名获取公司名 (假设格式 code_name.pdf)
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
                # 记录空结果，方便复核
                all_dividends.append({
                    'code': stock_code,
                    'name': stock_name,
                    'year': 'N/A',
                    'amount': 0,
                    'page': 'N/A',
                    'source_file': pdf_file,
                    'note': '未提取到数据'
                })

        # Save to Excel
        if all_dividends:
            df = pd.DataFrame(all_dividends)
            # Reorder columns
            cols = ['code', 'name', 'year', 'amount', 'page', 'source_file', 'note']
            # Ensure columns exist
            for c in cols:
                if c not in df.columns:
                    df[c] = ''
            df = df[cols]
            
            output_file = os.path.join(OUTPUT_DIR, 'dividends_summary.xlsx')
            df.to_excel(output_file, index=False)
            logger.info(f"结果已保存至 {output_file}")
        else:
            logger.warning("没有提取到任何数据")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--action', choices=['download', 'extract', 'all'], default='all')
    parser.add_argument('--limit', type=int, help='限制处理数量(用于测试)', default=None)
    parser.add_argument('--csv', help='指定股票列表CSV文件', default='stock_list.csv')
    args = parser.parse_args()
    
    run_pipeline(args.action, args.limit, args.csv)
