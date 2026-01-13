import os
import json
import logging
import pandas as pd
from src.pipeline_utils import load_state, save_results, process_file_serial
from src.extractor import ProspectusExtractor
from src.config import DATA_DIR, PDF_DIR

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def rerun_all_failures():
    # 1. Load failures from our new list
    retry_list_path = "data/full_retry_list.csv"
    if not os.path.exists(retry_list_path):
        logger.error("Retry list not found. Run find_all_failures.py first.")
        return

    retry_df = pd.read_csv(retry_list_path)
    files_to_retry = set(retry_df['source_file'].tolist())
    logger.info(f"Loaded {len(files_to_retry)} files to retry.")

    # 2. Load current state
    # pipeline_utils.load_state already does some filtering, but we want to be more aggressive
    # and use our own list.
    
    # We'll manually manage the state here to ensure these specific files are processed.
    processed_files, all_dividends = load_state()
    
    # Remove files_to_retry from processed_files to force re-processing
    original_processed_count = len(processed_files)
    processed_files = processed_files - files_to_retry
    logger.info(f"Forced {original_processed_count - len(processed_files)} files back into pending queue.")

    # Remove existing entries for these files from all_dividends
    original_dividend_count = len(all_dividends)
    all_dividends = [d for d in all_dividends if d.get('source_file') not in files_to_retry]
    logger.info(f"Removed {original_dividend_count - len(all_dividends)} records from current summary.")

    # 3. Initialize Extractor
    extractor = ProspectusExtractor()
    
    # 4. Process
    count = 0
    total = len(files_to_retry)
    
    # Sort files to process consistently
    sorted_files = sorted(list(files_to_retry))
    
    try:
        for pdf_file in sorted_files:
            pdf_path = os.path.join(PDF_DIR, pdf_file)
            if not os.path.exists(pdf_path):
                logger.warning(f"File not found: {pdf_path}")
                continue
            
            process_file_serial(pdf_file, extractor, all_dividends)
            processed_files.add(pdf_file)
            count += 1
            
            if count % 10 == 0:
                logger.info(f"Progress: {count}/{total}")
                save_results(all_dividends, processed_files)
                
    except KeyboardInterrupt:
        logger.info("User interrupted. Saving current progress...")
    finally:
        save_results(all_dividends, processed_files)
        logger.info(f"Finished processing. Processed {count} files.")

if __name__ == "__main__":
    rerun_all_failures()
