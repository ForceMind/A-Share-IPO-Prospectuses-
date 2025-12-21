import os
import logging
import pdfplumber
import pandas as pd
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed

# Add project root to path
# Assuming this script is run from project root, or src directory.
# Adjusting to allow running as "python src/audit_and_clean.py" from root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.downloader import Downloader
from src.config import PDF_DIR, DATA_DIR

logger = logging.getLogger(__name__)

def check_single_file(filename):
    """
    Check a single PDF file type.
    Returns filename if it's WRONG type, else None.
    """
    # wrong_keywords: keywords that indicate the file is definitely NOT a prospectus
    wrong_keywords = ["发行保荐书", "法律意见书", "审计报告", "核查意见", "律师工作报告", "上市公告书"]
    
    # appendix_keywords: keywords that indicate it's an appendix/summary/notice
    appendix_keywords = ["附录", "附件", "摘要", "提示", "意向书公告", "说明书公告"]
    
    filepath = os.path.join(PDF_DIR, filename)
    
    try:
        with pdfplumber.open(filepath) as pdf:
            if len(pdf.pages) > 0:
                text = pdf.pages[0].extract_text()
                if text:
                    # Check first 15 lines for title
                    lines = [line.strip() for line in text.split('\n')[:15] if line.strip()]
                    header_text = "".join(lines)
                    
                    # 1. Direct wrong keywords
                    for kw in wrong_keywords:
                        if kw in header_text:
                            if "招股说明书" not in header_text and "招股意向书" not in header_text:
                                return filename
                    
                    # 2. Appendix/Summary check
                    # If it says "招股说明书摘要" or has "附录" etc. without being the main doc
                    for kw in appendix_keywords:
                        if kw in header_text:
                            # Usually if it has "摘要" or "附录" in the first few lines as a standalone title
                            # it's not the main doc. The main doc would have "招股说明书" as the most prominent title.
                            if len(header_text) < 200: # Heuristic: if it's a short title page
                                return filename

                    # 3. Content-based sanity check
                    # Prospectus are usually very long. Appendices/summaries are shorter.
                    # But some summaries are also long. Let's use keyword ratio or presence.
                    if "招股说明书" not in header_text and "招股意向书" not in header_text:
                        # If neither is in the first page header, it's highly suspicious
                        return filename
                         
    except Exception:
        pass
        
    return None

def check_and_fix_pdf_type(concurrency=4):
    """
    Scans all PDFs in the data directory using multi-processing.
    If a file is identified as WRONG type, it deletes the file and triggers a re-download.
    """
    if not os.path.exists(PDF_DIR):
        logger.warning(f"PDF directory not found: {PDF_DIR}")
        return

    logger.info(f"开始全量检查本地 PDF 文件类型 (多进程并发数: {concurrency})...")
    
    files = [f for f in os.listdir(PDF_DIR) if f.endswith('.pdf')]
    total = len(files)
    wrong_files = []
    
    # 1. Audit Phase (Multi-Process)
    # Using ProcessPoolExecutor to truly utilize multiple cores
    with ProcessPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(check_single_file, f): f for f in files}
        
        completed = 0
        for future in as_completed(futures):
            res = future.result()
            if res:
                logger.warning(f"发现错误文件: {res}")
                wrong_files.append(res)
            
            completed += 1
            if completed % 50 == 0:
                logger.info(f"检查进度: {completed}/{total}...")

    if not wrong_files:
        logger.info("所有文件检查通过，未发现错误类型。")
        return

    logger.info(f"共发现 {len(wrong_files)} 个错误文件，准备删除并重新下载...")
    
    # 2. Fix Phase (Sequential download for safety, or we could parallelize this too)
    downloader = Downloader()
    
    for filename in wrong_files:
        filepath = os.path.join(PDF_DIR, filename)
        
        # Delete
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
                logger.info(f"已删除错误文件: {filename}")
        except Exception as e:
            logger.error(f"删除失败 {filename}: {e}")
            continue
            
        # Re-download
        try:
            if '_' in filename:
                code = filename.split('_')[0]
                name = filename.split('_')[1].replace('.pdf', '')
                logger.info(f"重新下载: {code} {name}")
                downloader.process_stock(code, name)
        except Exception as e:
            logger.error(f"重新下载失败 {filename}: {e}")

if __name__ == "__main__":
    from multiprocessing import freeze_support
    freeze_support()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - [PID:%(process)d] - %(message)s')
    check_and_fix_pdf_type(concurrency=8)
