import os
import pandas as pd
import logging
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from src.config import OUTPUT_DIR, PDF_DIR
from src.extractor import ProspectusExtractor

logger = logging.getLogger(__name__)

def _backfill_worker(pdf_file, indices_info):
    """
    Worker function to re-extract data from a single PDF.
    indices_info: list of {'index': idx, 'year': year, 'amount': amount}
    """
    try:
        import os
        from src.extractor import ProspectusExtractor
        
        pdf_path = os.path.join(PDF_DIR, pdf_file)
        if not os.path.exists(pdf_path):
            return pdf_file, None, "File not found"

        extractor = ProspectusExtractor()
        extracted_items = extractor.extract(pdf_path)
        
        results = []
        for info in indices_info:
            row_idx = info['index']
            row_year = str(info['year']).split('.')[0]
            row_amt = info['amount']
            
            match = None
            if extracted_items:
                for item in extracted_items:
                    item_year = str(item.get('year', '')).split('.')[0]
                    item_amt = item.get('amount', 0)
                    
                    try:
                        if row_year == item_year and abs(float(row_amt) - float(item_amt)) < 0.1:
                            match = item
                            break
                    except: pass
            
            if match:
                results.append({
                    'index': row_idx,
                    'context': match.get('context', ''),
                    'method': match.get('method', 'Unknown')
                })
            else:
                results.append({
                    'index': row_idx,
                    'context': "Backfill Failed: No Match",
                    'method': "Unknown"
                })
        
        return pdf_file, results, None
    except Exception as e:
        return pdf_file, [], str(e)

class DataVerifier:
    def __init__(self):
        # 否定词：如果原文包含这些词，说明可能不是真实分红
        self.negative_keywords = [
            '拟', '预计', '不确定', '风险', '尚未', '暂无', '分红政策', 
            '分配原则', '未来规划', '章程', '决策程序', '若', '如果'
        ]
        # 肯定词：辅助确认
        self.positive_keywords = [
            '已实施', '派发', '现金分红金额', '实际', '报告期内', '分派'
        ]
        # self.extractor = None # Lazy init not needed for multiprocessing

    def verify_all(self, summary_file=None, concurrency=4):
        if summary_file is None:
            summary_file = os.path.join(OUTPUT_DIR, 'dividends_summary.xlsx')
        
        if not os.path.exists(summary_file):
            logger.error(f"找不到汇总文件: {summary_file}")
            return None

        try:
            df = pd.read_excel(summary_file)
            
            # Check if context column exists or needs backfilling
            need_backfill = False
            if 'context' not in df.columns:
                logger.warning("汇总文件中缺少 context 列，将尝试从原始 PDF 中回溯提取...")
                df['context'] = ''
                df['method'] = 'Unknown'
                need_backfill = True
            
            results = []
            
            # Optimization: If backfilling, we might want to process by file
            if need_backfill:
                # Create a map of file -> list of indices to update
                file_map = {}
                for idx, row in df.iterrows():
                    src = row.get('source_file')
                    if src and pd.notna(src):
                        if pd.isna(row.get('year')) or pd.isna(row.get('amount')):
                            df.at[idx, 'context'] = "Backfill Skipped: Invalid Year/Amount"
                            continue
                            
                        if src not in file_map: file_map[src] = []
                        file_map[src].append({
                            'index': idx,
                            'year': row['year'],
                            'amount': row['amount']
                        })
                
                logger.info(f"需要回溯 {len(file_map)} 个文件的上下文信息 (并发数: {concurrency})")
                
                # Submit tasks to pool
                with ProcessPoolExecutor(max_workers=concurrency) as executor:
                    futures = {
                        executor.submit(_backfill_worker, pdf_file, items): pdf_file
                        for pdf_file, items in file_map.items()
                    }
                    
                    completed_count = 0
                    total_count = len(futures)
                    
                    for future in as_completed(futures):
                        pdf_file = futures[future]
                        try:
                            _, match_results, error = future.result()
                            if error:
                                logger.error(f"回溯文件 {pdf_file} 失败: {error}")
                            else:
                                for res in match_results:
                                    df.at[res['index'], 'context'] = res['context']
                                    df.at[res['index'], 'method'] = res['method']
                        except Exception as e:
                            logger.error(f"处理回溯结果失败 {pdf_file}: {e}")
                            
                        completed_count += 1
                        if completed_count % 10 == 0:
                            logger.info(f"回溯进度: {completed_count}/{total_count}")

            # Now verify
            for _, row in df.iterrows():
                verify_result = self.verify_row(row)
                results.append(verify_result)
            
            # 将校验结果合并到 DataFrame
            verify_df = pd.DataFrame(results)
            
            # Remove old verify columns if they exist to avoid duplication
            cols_to_drop = [c for c in verify_df.columns if c in df.columns]
            if cols_to_drop:
                df = df.drop(columns=cols_to_drop)
                
            final_df = pd.concat([df, verify_df], axis=1)
            
            # 保存校验报告
            report_path = os.path.join(OUTPUT_DIR, 'verification_report.xlsx')
            final_df.to_excel(report_path, index=False)
            logger.info(f"校验完成，报告已保存至: {report_path}")
            return report_path
            
        except Exception as e:
            logger.error(f"校验过程中出错: {e}")
            return None

    def verify_row(self, row):
        context = str(row.get('context', ''))
        amount = row.get('amount', 0)
        year = str(row.get('year', ''))
        
        if not context or context == 'nan' or context == 'Backfill Failed: No Match':
            return {'verify_status': 'Manual Check', 'verify_note': '缺失原文内容'}

        if amount == 0:
            return {'verify_status': 'Skip', 'verify_note': '金额为0'}

        # 1. 检查否定关键词
        found_negatives = [kw for kw in self.negative_keywords if kw in context]
        
        # 2. 特殊逻辑：如果是“政策”或“规划”章节，极大概率是误报
        is_policy = any(kw in context for kw in ['分配政策', '利润分配规划', '分红回报规划'])
        
        # 3. 检查年份匹配
        # context might be long, check if year is present
        year_val = year.split('.')[0] # 2020.0 -> 2020
        year_match = year_val in context if year != 'N/A' else True
        
        # 判定结论
        status = 'Pass'
        notes = []
        
        if is_policy:
            status = 'Suspect'
            notes.append("原文疑似属于政策/规划描述")
        
        if found_negatives:
            status = 'Suspect'
            notes.append(f"包含否定词: {', '.join(found_negatives)}")
            
        if not year_match:
            status = 'Suspect'
            notes.append("原文中未发现对应年份")
            
        # 排除掉一些误报，比如“每10股派1.0元”，虽然有金额，但不是总额
        # But be careful, sometimes context is just the table row which might not say "total"
        if '每10股' in context and amount < 50: # 假设总分红通常大于50万
             status = 'Suspect'
             notes.append("可能提取的是每股分红而非总额")

        return {
            'verify_status': status,
            'verify_note': '; '.join(notes) if notes else 'OK'
        }

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    verifier = DataVerifier()
    verifier.verify_all()
