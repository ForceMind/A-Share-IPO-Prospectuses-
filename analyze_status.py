import os
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

def analyze_missing_pdfs():
    stock_list_path = r"data/stock_list.csv"
    pdf_dir = r"data/pdfs"
    
    if not os.path.exists(stock_list_path):
        logger.error("Stock list not found.")
        return

    df = pd.read_csv(stock_list_path)
    df['code'] = df['code'].apply(lambda x: str(x).zfill(6))
    
    # 1. Identify missing PDFs
    existing_files = os.listdir(pdf_dir)
    existing_codes = set()
    for f in existing_files:
        if '_' in f:
            existing_codes.add(f.split('_')[0])
            
    missing_stocks = df[~df['code'].isin(existing_codes)]
    logger.info(f"Total stocks: {len(df)}")
    logger.info(f"Existing PDFs: {len(existing_codes)}")
    logger.info(f"Missing PDFs: {len(missing_stocks)}")
    
    # Output missing list for manual check
    missing_stocks.to_csv("data/missing_pdfs_list.csv", index=False)
    logger.info("Missing list saved to data/missing_pdfs_list.csv")
    
    # 2. Analyze potential reasons (simple heuristics based on name)
    # Stocks with "ST", "退" might be delisted or special treatment
    # But more importantly, check for "借壳", "吸收合并" keywords in recent announcements if we could (not implemented here)
    # Instead, we just list them.
    
    logger.info("\n--- Sample Missing Stocks ---")
    print(missing_stocks.head(10)[['code', 'name', 'industry']])

def analyze_extraction_failures():
    output_file = r"data/output/dividends_summary.xlsx"
    if not os.path.exists(output_file):
        logger.warning("No extraction results yet.")
        return
        
    df = pd.read_excel(output_file)
    if 'note' not in df.columns:
        return
        
    # Filter failures
    failures = df[df['note'].str.contains('未提取|扫描件|未找到', na=False)]
    logger.info(f"\nTotal Extraction Failures: {len(failures)}")
    
    # Group by failure type
    if not failures.empty:
        logger.info("\nFailure Types Breakdown:")
        print(failures['note'].value_counts())
        
        # Save for targeted processing
        failures.to_csv("data/extraction_failures_list.csv", index=False)
        logger.info("Failure list saved to data/extraction_failures_list.csv")

if __name__ == "__main__":
    analyze_missing_pdfs()
    analyze_extraction_failures()
