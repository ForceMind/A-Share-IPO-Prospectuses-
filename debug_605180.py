
import sys
import os
import logging
import pdfplumber
import re

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from src.extractor import ProspectusExtractor

def debug_605180():
    target_code = '605180'
    pdf_dir = r"e:\Privy\A-Share-IPO-Prospectuses\data\pdfs"
    
    # Find file
    found_file = None
    for f in os.listdir(pdf_dir):
        if f.startswith(target_code):
            found_file = f
            break
            
    if not found_file:
        logger.error(f"File for {target_code} not found.")
        return

    file_path = os.path.join(pdf_dir, found_file)
    logger.info(f"Analyzing {found_file}...")
    
    extractor = ProspectusExtractor()
    
    with pdfplumber.open(file_path) as pdf:
        # Scan for keywords to find the page
        target_pages = []
        for i, page in enumerate(pdf.pages):
            text = page.extract_text()
            if not text: continue
            
            # The snippet user provided contains:
            # "最近三年实际股利分配情况"
            # "分配方案"
            # "占上一年净利润比例"
            if "最近三年实际股利分配情况" in text and "分配方案" in text:
                target_pages.append(i)
                logger.info(f"Found keyword match on page {i+1}")
                
                # Dump text to see layout
                logger.info("-" * 20 + f" Page {i+1} Text " + "-" * 20)
                print(text)
                logger.info("-" * 50)
                
                # Try table extraction
                tables = page.extract_tables()
                logger.info(f"Extracted {len(tables)} tables on page {i+1}")
                for idx, t in enumerate(tables):
                    logger.info(f"Table {idx}: {t}")

    # Run extraction
    logger.info("Running full extraction logic...")
    results = extractor.extract(file_path)
    logger.info(f"Extraction Result: {results}")

if __name__ == "__main__":
    debug_605180()
