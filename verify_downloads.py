import sys
import os
import logging
import pdfplumber
import re

# Setup simple logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def check_pdf_title(file_path):
    """
    Check if the PDF title looks like a Prospectus (招股意向书/招股说明书)
    and NOT a Sponsorship Letter (保荐书) or Legal Opinion (法律意见书).
    """
    try:
        with pdfplumber.open(file_path) as pdf:
            # Check first 5 pages for title
            full_text = ""
            for i in range(min(5, len(pdf.pages))):
                text = pdf.pages[i].extract_text()
                if text:
                    full_text += text + "\n"
            
            # Keywords to identify file type
            is_prospectus = False
            if "招股说明书" in full_text or "招股意向书" in full_text:
                is_prospectus = True
                
            is_wrong_type = False
            wrong_keywords = ["发行保荐书", "法律意见书", "审计报告", "核查意见", "律师工作报告"]
            
            found_wrong_kws = []
            for kw in wrong_keywords:
                if kw in full_text:
                    # Make sure it's in the title/header area (usually large font or first few lines), 
                    # but simple text check is a good heuristic.
                    # Be careful: Prospectus mentions "保荐书" in TOC.
                    # Heuristic: Title usually appears at the top of first few pages.
                    # Let's check the very first page first.
                    first_page_text = pdf.pages[0].extract_text() if len(pdf.pages) > 0 else ""
                    if kw in first_page_text:
                        is_wrong_type = True
                        found_wrong_kws.append(kw)
                        break
            
            return is_prospectus, is_wrong_type, found_wrong_kws
            
    except Exception as e:
        logger.error(f"Error checking {file_path}: {e}")
        return False, False, []

def verify_specific_files():
    data_dir = r"e:\Privy\A-Share-IPO-Prospectuses\data\pdfs"
    target_stocks = ["605011", "002962", "300935", "601187"]
    
    logger.info(f"Verifying files for: {target_stocks}")
    
    for stock in target_stocks:
        # Find file
        found_file = None
        for f in os.listdir(data_dir):
            if f.startswith(stock) and f.endswith(".pdf"):
                found_file = f
                break
        
        if not found_file:
            logger.warning(f"File for {stock} not found.")
            continue
            
        file_path = os.path.join(data_dir, found_file)
        is_prospectus, is_wrong_type, wrong_kws = check_pdf_title(file_path)
        
        status = "OK"
        if is_wrong_type:
            status = f"WRONG TYPE ({wrong_kws})"
        elif not is_prospectus:
            status = "UNCERTAIN (No '招股说明书' found)"
            
        logger.info(f"[{stock}] {found_file}: {status}")

if __name__ == "__main__":
    verify_specific_files()
