import os
import logging
import pdfplumber
import pandas as pd
import sys

# Add project root to path
# Assuming this script is run from project root, or src directory.
# Adjusting to allow running as "python src/audit_and_clean.py" from root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.downloader import Downloader
from src.config import PDF_DIR, DATA_DIR

logger = logging.getLogger(__name__)

def check_and_fix_pdf_type():
    """
    Scans all PDFs in the data directory.
    If a file is identified as 'Sponsorship Letter' (保荐书) or 'Legal Opinion' (法律意见书) 
    instead of 'Prospectus' (招股说明书), it deletes the file and triggers a re-download.
    """
    if not os.path.exists(PDF_DIR):
        logger.warning(f"PDF directory not found: {PDF_DIR}")
        return

    logger.info("开始全量检查本地 PDF 文件类型...")
    
    files = [f for f in os.listdir(PDF_DIR) if f.endswith('.pdf')]
    total = len(files)
    wrong_files = []
    
    # Keyword configuration
    wrong_keywords = ["发行保荐书", "法律意见书", "审计报告", "核查意见", "律师工作报告", "上市公告书"]
    
    # Optimize: Don't check files we've already successfully extracted data from? 
    # Or just check everything to be safe. Checking header of PDF is fast.
    
    for i, filename in enumerate(files):
        if i % 50 == 0:
            logger.info(f"Progress: {i}/{total}...")
            
        filepath = os.path.join(PDF_DIR, filename)
        is_wrong = False
        
        try:
            # Only read the first page to save time
            with pdfplumber.open(filepath) as pdf:
                if len(pdf.pages) > 0:
                    text = pdf.pages[0].extract_text()
                    if text:
                        # Check title
                        for kw in wrong_keywords:
                            if kw in text:
                                # Double check: Ensure "招股说明书" is NOT in the title line
                                # Some prospectuses mention these docs in TOC on page 1
                                # We look for the main title.
                                # Heuristic: If "保荐书" is present but "招股说明书" is NOT present in the first few lines
                                lines = text.split('\n')[:10] # Check first 10 lines
                                header_text = "\n".join(lines)
                                
                                if kw in header_text and "招股说明书" not in header_text and "招股意向书" not in header_text:
                                    is_wrong = True
                                    logger.warning(f"发现错误文件: {filename} (检测到: {kw})")
                                    break
        except Exception as e:
            logger.warning(f"无法读取文件 {filename}: {e}")
            # Corrupt file? Maybe delete too?
            pass
            
        if is_wrong:
            wrong_files.append(filename)

    if not wrong_files:
        logger.info("所有文件检查通过，未发现错误类型。")
        return

    logger.info(f"共发现 {len(wrong_files)} 个错误文件，准备删除并重新下载...")
    
    downloader = Downloader()
    
    for filename in wrong_files:
        filepath = os.path.join(PDF_DIR, filename)
        
        # 1. Delete
        try:
            os.remove(filepath)
            logger.info(f"已删除: {filename}")
        except Exception as e:
            logger.error(f"删除失败 {filename}: {e}")
            continue
            
        # 2. Re-download
        # Parse code and name from filename "code_name.pdf"
        try:
            code = filename.split('_')[0]
            name = filename.split('_')[1].replace('.pdf', '')
            
            # Use downloader to fetch correct one
            # The downloader has been updated to filter out "保荐书" etc.
            downloader.process_stock(code, name)
            
        except Exception as e:
            logger.error(f"重新下载失败 {filename}: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    check_and_fix_pdf_type()
