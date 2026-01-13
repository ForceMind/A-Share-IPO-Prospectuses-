import os
import pandas as pd
import logging
from src.config import OUTPUT_DIR, PDF_DIR

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

def find_all_failures():
    output_file = os.path.join(OUTPUT_DIR, 'dividends_summary.xlsx')
    if not os.path.exists(output_file):
        logger.warning("No summary file found.")
        return

    df = pd.read_excel(output_file)
    
    # Criteria for failure:
    # 1. amount is 0 OR year is N/A
    # 2. OR note contains failure keywords
    
    failed_mask = (
        (df['amount'] == 0) | 
        (df['year'].astype(str) == 'N/A') |
        (df['note'].str.contains('未提取|扫描件|未找到|错误', na=False))
    )
    
    failures = df[failed_mask]
    
    # Also check for files that are in PDF_DIR but not in dividends_summary.xlsx
    all_pdfs = [f for f in os.listdir(PDF_DIR) if f.endswith('.pdf')]
    processed_files = set(df['source_file'].unique())
    missing_from_summary = [f for f in all_pdfs if f not in processed_files]
    
    logger.info(f"Summary total records: {len(df)}")
    logger.info(f"Failures in summary: {len(failures)}")
    logger.info(f"PDFs missing from summary: {len(missing_from_summary)}")
    
    # Distinct files that failed
    failed_files = set(failures['source_file'].unique())
    for f in missing_from_summary:
        failed_files.add(f)
        
    logger.info(f"Total unique files to retry: {len(failed_files)}")
    
    # Save list
    retry_df = pd.DataFrame(list(failed_files), columns=['source_file'])
    retry_df.to_csv("data/full_retry_list.csv", index=False)
    logger.info("Retry list saved to data/full_retry_list.csv")
    
    if not failures.empty:
        logger.info("\nBreakdown of failures in summary:")
        print(failures['note'].fillna('None').value_counts())

if __name__ == "__main__":
    find_all_failures()
