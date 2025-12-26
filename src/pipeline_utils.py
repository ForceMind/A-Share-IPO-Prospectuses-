import os
import logging
import pandas as pd
import json
from src.config import DATA_DIR, OUTPUT_DIR, PDF_DIR, LOG_FORMAT

logger = logging.getLogger(__name__)

def save_results(all_dividends, processed_files=None):
    if all_dividends:
        df = pd.DataFrame(all_dividends)
        # Ensure 'note' is included in columns, and others like 'status' if we added it
        cols = ['code', 'name', 'year', 'amount', 'page', 'method', 'context', 'source_file', 'note']
        
        # Add columns if they don't exist
        for c in cols:
            if c not in df.columns:
                df[c] = ''
        
        # Ensure code is 6 digits string
        if 'code' in df.columns:
            df['code'] = df['code'].apply(lambda x: str(x).zfill(6) if pd.notnull(x) else x)

        # Reorder columns
        df = df[cols]
        
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
            
        output_file = os.path.join(OUTPUT_DIR, 'dividends_summary.xlsx')
        try:
            # Use xlsxwriter to enforce text format for 'code' column to prevent Excel from stripping leading zeros
            # Note: This requires 'xlsxwriter' or 'openpyxl' installed. Assuming default engine is fine, 
            # but we explicitly convert to string. Excel might still be annoying.
            # To be safer, we can try to force string type in pandas.
            df = df.astype({'code': str})
            
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
            # Ensure code is string with zero padding
            if 'code' in df.columns:
                df['code'] = df['code'].apply(lambda x: str(x).zfill(6) if pd.notnull(x) else x)
            
            all_dividends = df.to_dict('records')
            logger.info(f"已加载 {len(all_dividends)} 条现有结果")
            
            # Identify failures or 'no data' records to retry
            # Retry if:
            # 1. amount is 0 or year is N/A
            # 2. OR status/note indicates failure (e.g. 'section_found_no_data', 'scanned_pdf', 'no_section_found')
            
            failed_files = set()
            for item in all_dividends:
                is_failed = False
                if str(item.get('amount')) == '0' or item.get('year') == 'N/A':
                    is_failed = True
                
                # Check note column for specific failure markers
                note = str(item.get('note', ''))
                if '未提取到数据' in note or '扫描件' in note or '未找到' in note:
                    is_failed = True
                
                if is_failed:
                    src_file = item.get('source_file')
                    if src_file:
                        failed_files.add(src_file)

            if failed_files:
                logger.info(f"发现 {len(failed_files)} 个之前提取失败/无数据的文件，准备重新解析...")
                to_remove = []
                for f in processed_files:
                    if f in failed_files:
                        to_remove.append(f)
                
                for f in to_remove:
                    processed_files.remove(f)
                
                # Remove these failed records from all_dividends so we don't duplicate them
                # (We will re-add them if extraction succeeds, or re-add failure record if it fails again)
                all_dividends = [item for item in all_dividends if item.get('source_file') not in failed_files]
                
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
        if os.path.exists(PDF_DIR):
            pdf_files_set = set(os.listdir(PDF_DIR))
        else:
            pdf_files_set = set()
        
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

def process_file_serial(pdf_file, extractor, all_dividends):
    """
    Serial processing of a single file, used when not in multiprocessing mode.
    """
    try:
        stock_code = pdf_file.split('_')[0]
        stock_name = pdf_file.split('_')[1].replace('.pdf', '') if '_' in pdf_file else 'Unknown'
        pdf_path = os.path.join(PDF_DIR, pdf_file)
        
        logger.info(f"正在解析: {pdf_file} (Code: {stock_code}, Size: {os.path.getsize(pdf_path)/1024:.2f} KB)")
        dividends = extractor.extract(pdf_path)
        
        if dividends:
            logger.info(f"解析成功 {pdf_file}: 提取到 {len(dividends)} 条分红记录")
            for div in dividends:
                div['code'] = stock_code
                div['name'] = stock_name
                div['source_file'] = pdf_file
                all_dividends.append(div)
        else:
            logger.warning(f"解析完成但无数据 {pdf_file}")
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
        logger.error(f"处理文件失败 {pdf_file}: {e}", exc_info=True)
    return True
