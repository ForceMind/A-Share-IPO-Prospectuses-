
import logging
import multiprocessing
import time
import os
import sys

# Mock Downloader
class Downloader:
    def process_stock(self, code, name):
        # Simulate work
        time.sleep(0.5)
        # logging.info(f"Processed {code}")

# Mock TaskManager internals for testing without full deps
from src.task_manager import TaskManager

def test_stop_logic():
    print("Testing Stop Logic...")
    tm = TaskManager()
    
    # Override downloader in worker with mock? 
    # It's hard to mock inside subprocess without patching in the worker function.
    # But we can test if the worker responds to the event if we run the actual function.
    
    from src.task_manager import _worker_download_chunk
    
    # Create a chunk that takes some time
    chunk = [('000001', 'Test1'), ('000002', 'Test2'), ('000003', 'Test3'), ('000004', 'Test4')]
    
    manager = multiprocessing.Manager()
    log_queue = manager.Queue()
    stop_event = manager.Event()
    
    # Start worker in a separate process
    p = multiprocessing.Process(target=_worker_download_chunk, args=(chunk, log_queue, stop_event))
    p.start()
    
    # Let it run for a bit
    time.sleep(1.2) # Should process 1 or 2 items (0.5s each)
    
    # Set stop event
    print("Setting stop event...")
    stop_event.set()
    
    # Wait for process to finish
    p.join(timeout=2)
    
    if p.is_alive():
        print("FAIL: Worker did not stop in time.")
        p.terminate()
    else:
        print("SUCCESS: Worker stopped gracefully.")
        
    # Check logs
    while not log_queue.empty():
        print("Log:", log_queue.get())

if __name__ == "__main__":
    test_stop_logic()
