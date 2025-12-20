
import logging
import os
import time
import pandas as pd
from src.task_manager import TaskManager, get_task_manager
from src.config import DATA_DIR

# Setup minimal logging to console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [PID:%(process)d] - %(levelname)s - %(message)s')

def create_dummy_data():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    
    # Create a dummy stock list with 20 items to ensure at least 2 chunks with concurrency 4 (20/4=5 items per chunk)
    stocks = []
    for i in range(20):
        stocks.append({
            'code': f'000{str(i).zfill(3)}',
            'name': f'Stock_{i}',
            'listing_date': '2020-01-01',
            'industry': 'Test'
        })
    
    df = pd.DataFrame(stocks)
    df.to_csv(os.path.join(DATA_DIR, 'stock_list.csv'), index=False)
    print("Created dummy stock_list.csv with 20 stocks.")

def verify_chunks():
    create_dummy_data()
    
    tm = get_task_manager()
    
    # Mock downloader to avoid actual network calls and sleep instead
    from unittest.mock import MagicMock
    tm.downloader = MagicMock()
    
    # We need to mock the _worker_download_chunk or the downloader inside it.
    # Since _worker_download_chunk creates a NEW Downloader instance inside the process,
    # simply mocking tm.downloader won't affect the worker processes.
    # We should rely on the logs to verify PIDs.
    
    # However, to avoid actual network calls, we might want to patch src.downloader.Downloader.run or process_stock?
    # But `_worker_download_chunk` imports Downloader inside the function.
    
    # Let's run it for real but short-lived, or just observe the logs.
    # Since we can't easily mock inside the spawned process without more complex setup, 
    # we will just trust the log output which prints [PID:xxxx].
    
    # We'll rely on the user running this and checking the output, or capture it here.
    
    print("Starting tasks...")
    tm.start_tasks(action='download', limit=20)
    
    # Poll for logs
    start_time = time.time()
    pids_seen = set()
    
    while time.time() - start_time < 10:
        logs = tm.get_logs()
        for log in logs:
            print(f"LOG: {log}")
            if "Download Worker [PID:" in log:
                # Extract PID
                import re
                match = re.search(r'PID:(\d+)', log)
                if match:
                    pids_seen.add(match.group(1))
        
        if len(pids_seen) >= 2:
            print(f"SUCCESS: Seen distinct PIDs: {pids_seen}")
            tm.stop_tasks()
            break
        
        if not tm.status['is_running'] and tm.status['completed_tasks'] > 0:
             break
             
        time.sleep(0.5)

    if len(pids_seen) < 2:
        print(f"WARNING: Only saw PIDs: {pids_seen}. (Expected multiple for concurrency)")
    
    # tm.stop_tasks() # Don't stop it if it fails on import, we already edited the file
    pass 
    print("Test finished.")

if __name__ == "__main__":
    verify_chunks()
