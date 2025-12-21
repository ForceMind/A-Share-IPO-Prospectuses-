
import sys
import os
import logging
import pdfplumber
import re

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from src.extractor import ProspectusExtractor

def debug_text_extraction(stock_codes):
    data_dir = r"e:\Privy\A-Share-IPO-Prospectuses\data\pdfs"
    extractor = ProspectusExtractor()
    
    for code in stock_codes:
        # Find file
        found_file = None
        for f in os.listdir(data_dir):
            if f.startswith(code) and f.endswith(".pdf"):
                found_file = f
                break
        
        if not found_file:
            logger.warning(f"File for {code} not found.")
            continue
            
        file_path = os.path.join(data_dir, found_file)
        logger.info(f" Analyzing {found_file}...")
        
        # 1. Manual Scan for specific text patterns
        with pdfplumber.open(file_path) as pdf:
            # We suspect the info is in "股利分配政策" section
            # Let's scan pages that contain this title or similar
            
            target_pages = []
            for i, page in enumerate(pdf.pages):
                text = page.extract_text()
                if not text: continue
                
                # Check for section title roughly
                if "股利分配政策" in text or "股利分配情况" in text or "滚存利润" in text:
                    target_pages.append(i)
            
            logger.info(f"Target pages by keyword: {target_pages[:5]}...")
            
            # Extract from these pages using current logic first
            # To see what we miss
            
            # Also dump text snippets that contain numbers
            for p in target_pages[:3]: # check first few matches
                text = pdf.pages[p].extract_text()
                logger.info(f"--- Page {p+1} Snippet ---")
                lines = text.split('\n')
                for line in lines:
                    if any(k in line for k in ['分红', '分配', '派发', '现金']):
                        # Check for numbers
                        if re.search(r'\d+', line):
                            logger.info(f"  > {line.strip()[:150]}")

        # 2. Run current extraction
        results = extractor.extract(file_path)
        logger.info(f"Current Extraction Result: {results}")
        logger.info("="*50)

if __name__ == "__main__":
    debug_text_extraction(['605155', '605268', '001201'])
