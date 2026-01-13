import os
import sys
import logging
from src.txt_process_manager import TxtProcessManager

# Setup basic logging
logging.basicConfig(level=logging.INFO)

def force_run():
    print("Force running TxtProcessManager extraction...")
    
    # 1. Remove existing excel to ensure clean write
    excel_path = "data/TXT/extracted_dividends.xlsx"
    if os.path.exists(excel_path):
        try:
            os.remove(excel_path)
            print(f"Deleted existing file: {excel_path}")
        except Exception as e:
            print(f"Failed to delete existing file: {e}")
            return

    # 2. Instantiate Manager
    manager = TxtProcessManager()
    
    # 3. Run extraction (limit to 10 for speed)
    # We call the internal run method directly or via start_tasks but wait for completion
    manager.set_concurrency(4)
    manager.start_tasks(limit=10)
    
    # 4. Wait loop
    import time
    while manager.get_status()["is_running"]:
        time.sleep(1)
        print("Processing...", end='\r')
        
    print("\nProcessing complete.")
    
    # 5. Verify result immediately
    if os.path.exists(excel_path):
        import pandas as pd
        xls = pd.ExcelFile(excel_path)
        print(f"Generated Sheet Names: {xls.sheet_names}")
        if 'Stock List' in xls.sheet_names:
            print("SUCCESS: 'Stock List' sheet found.")
        else:
            print("FAILURE: 'Stock List' sheet NOT found.")
    else:
        print("Error: Excel file was not generated.")

if __name__ == "__main__":
    force_run()
