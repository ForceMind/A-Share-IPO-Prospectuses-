import os
import json
import logging
import sys

# Add src to path if running directly
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import PDF_DIR, DATA_DIR

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def audit_and_clean():
    logger.info("Starting audit...")
    
    files_to_remove = []
    
    if not os.path.exists(PDF_DIR):
        logger.warning(f"PDF directory not found: {PDF_DIR}")
        return

    # 1. Scan files
    files = os.listdir(PDF_DIR)
    logger.info(f"Scanning {len(files)} files in {PDF_DIR}...")
    
    for filename in files:
        if not filename.endswith('.pdf'):
            continue
            
        filepath = os.path.join(PDF_DIR, filename)
        
        # Check 1: 92xxxx stocks (北交所/新三板 logic if user considers them 'bad' or unwanted for this run)
        if filename.startswith('92'):
            logger.info(f"Marking for removal (92xxxx stock): {filename}")
            files_to_remove.append(filename)
            continue

        # Check: Remove '意见', '摘要', '更正', '提示' files
        bad_keywords = ['意见', '摘要', '更正', '提示', '反馈', '回复']
        if any(kw in filename for kw in bad_keywords):
            logger.info(f"Marking for removal (Bad keyword in filename): {filename}")
            files_to_remove.append(filename)
            continue
            
        # Check 2: Small files (< 2000KB to be safe, sometimes empty PDFs are small)
        if os.path.getsize(filepath) < 2048000:
            logger.info(f"Marking for removal (File too small): {filename} ({os.path.getsize(filepath)} bytes)")
            files_to_remove.append(filename)
            continue
            
        # Check 3: PDF Header
        try:
            with open(filepath, 'rb') as f:
                header = f.read(4)
                if header != b'%PDF':
                    logger.info(f"Marking for removal (Invalid PDF header): {filename}")
                    files_to_remove.append(filename)
        except Exception as e:
            logger.error(f"Error reading {filename}: {e}")
            files_to_remove.append(filename)

    # 2. Remove files
    if not files_to_remove:
        logger.info("No bad files found.")
    else:
        logger.info(f"Found {len(files_to_remove)} bad files. Deleting...")
        for filename in files_to_remove:
            try:
                os.remove(os.path.join(PDF_DIR, filename))
                logger.info(f"Deleted: {filename}")
            except Exception as e:
                logger.error(f"Failed to delete {filename}: {e}")

    # 3. Update processed_files.json
    state_file = os.path.join(DATA_DIR, 'processed_files.json')
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r', encoding='utf-8') as f:
                processed_files = set(json.load(f))
            
            original_count = len(processed_files)
            removed_count = 0
            for filename in files_to_remove:
                if filename in processed_files:
                    processed_files.remove(filename)
                    removed_count += 1
            
            if removed_count > 0:
                with open(state_file, 'w', encoding='utf-8') as f:
                    json.dump(list(processed_files), f)
                logger.info(f"Updated processed_files.json. Removed {removed_count} entries from history.")
            else:
                logger.info("No entries in processed_files.json needed removal.")
                
        except Exception as e:
            logger.error(f"Failed to update processed_files.json: {e}")

if __name__ == '__main__':
    audit_and_clean()
