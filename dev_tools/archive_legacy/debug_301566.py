
import sys
import os
import logging
import pdfplumber
import re

# Setup simple logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

# Add project root to path to import ProspectusExtractor if needed, 
# but for this debug script we can just inline or import.
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.extractor import ProspectusExtractor

def debug_301566():
    pdf_path = r"e:\Privy\A-Share-IPO-Prospectuses\data\pdfs\301566_达利凯普.pdf"
    
    if not os.path.exists(pdf_path):
        logger.error(f"File not found: {pdf_path}")
        # Try to find it via partial match if full name is wrong
        folder = os.path.dirname(pdf_path)
        for f in os.listdir(folder):
            if '301566' in f:
                logger.info(f"Found candidate: {f}")
                pdf_path = os.path.join(folder, f)
                break
        else:
            return

    logger.info(f"Analyzing {pdf_path}...")
    
    extractor = ProspectusExtractor()
    
    # 1. Test Page Location Logic
    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        logger.info(f"Total pages: {total_pages}")
        
        # Manually scan pages to see where keywords are
        logger.info("Scanning all pages for keywords (first 100 pages for speed)...")
        found_keywords = []
        
        for i in range(min(total_pages, 100)):
            page = pdf.pages[i]
            text = page.extract_text()
            if not text: continue
            
            score = 0
            hits = []
            if '现金分红' in text: 
                score += 15
                hits.append('现金分红')
            for kw in extractor.keywords:
                if kw in text:
                    score += 5
                    hits.append(kw)
            
            if score > 0:
                # Log pages with high scores
                logger.info(f"Page {i+1}: Score {score} - Keywords: {set(hits)}")
                
                # Print context snippet
                lines = text.split('\n')
                for line in lines:
                    if any(k in line for k in ['现金分红', '股利分配']):
                        logger.info(f"  Context: {line.strip()[:100]}...")

    # 2. Run actual extraction
    logger.info("-" * 50)
    logger.info("Running full extraction logic...")
    results = extractor.extract(pdf_path)
    logger.info(f"Extraction Result: {results}")

if __name__ == "__main__":
    debug_301566()
