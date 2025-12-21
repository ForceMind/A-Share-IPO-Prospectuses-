import os
import logging
import pdfplumber
import pandas as pd
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    wrong_keywords = ["发行保荐书", "法律意见书", "审计报告", "核查意见", "律师工作报告", "上市公告书"]
    filepath = os.path.join(PDF_DIR, filename)
    
    try:
        with pdfplumber.open(filepath) as pdf:
            if len(pdf.pages) > 0:
                text = pdf.pages[0].extract_text()
                if text:
                    # Check first 10 lines only for efficiency
                    lines = text.split('\n')[:10]
                    header_text = "\n".join(lines)
                    
                    for kw in wrong_keywords:
                        if kw in header_text:
                            # Double check: Ensure "招股说明书" is NOT in the header
                            if "招股说明书" not in header_text and "招股意向书" not in header_text:
                                logger.warning(f"发现错误文件: {filename} (检测到: {kw})")
                                return filename
    except Exception as e:
        # If file is corrupt, it might also be considered 'wrong' or 'broken'
        logger.warning(f"无法读取文件 {filename}: {e}")
        pass
        
    return None

def check_and_fix_pdf_type(concurrency=4):
    """
    Scans all PDFs in the data directory using multi-threading.
    If a file is identified as WRONG type, it deletes the file and triggers a re-download.
    """
    if not os.path.exists(PDF_DIR):
        logger.warning(f"PDF directory not found: {PDF_DIR}")
        return

    logger.info(f"开始全量检查本地 PDF 文件类型 (并发数: {concurrency})...")
    
    files = [f for f in os.listdir(PDF_DIR) if f.endswith('.pdf')]
    total = len(files)
    wrong_files = []
    
    # 1. Audit Phase (Multi-threaded)
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(check_single_file, f): f for f in files}
        
        completed = 0
        for future in as_completed(futures):
            res = future.result()
            if res:
                wrong_files.append(res)
            
            completed += 1
            if completed % 50 == 0:
                logger.info(f"检查进度: {completed}/{total}...")

    if not wrong_files:
        logger.info("所有文件检查通过，未发现错误类型。")
        return

    logger.info(f"共发现 {len(wrong_files)} 个错误文件，准备删除并重新下载...")
    
    # 2. Fix Phase (Sequential or Threaded download)
    # Downloader uses requests, can be threaded too but let's keep it simple or use Downloader logic
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
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    # Run standalone with default concurrency
    check_and_fix_pdf_type(concurrency=8)
