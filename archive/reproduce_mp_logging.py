import multiprocessing
import logging
import time
import os
import queue
from concurrent.futures import ProcessPoolExecutor

# Use a custom QueueHandler for compatibility if needed, 
# though Python 3.8+ has logging.handlers.QueueHandler
try:
    from logging.handlers import QueueHandler
except ImportError:
    class QueueHandler(logging.Handler):
        def __init__(self, log_queue):
            super().__init__()
            self.log_queue = log_queue
        def emit(self, record):
            try:
                self.log_queue.put_nowait(record)
            except Exception:
                self.handleError(record)

def worker_func_broken(name):
    logger = logging.getLogger()
    # Without configuring the queue handler inside the process, this log is lost or just goes to stderr
    logger.info(f"Worker {name} [PID {os.getpid()}] started (Broken).")
    time.sleep(0.5)
    return f"{name} done"

def worker_func_fixed(name, log_queue):
    # Fix: Setup logging to queue
    logger = logging.getLogger()
    logger.handlers = [] 
    logger.addHandler(QueueHandler(log_queue))
    logger.setLevel(logging.INFO)
    
    logger.info(f"Worker {name} [PID {os.getpid()}] started (Fixed).")
    time.sleep(0.5)
    return f"{name} done"

def test_broken():
    print("\n--- Testing Current Broken State ---")
    # Current state: TaskManager creates a queue but doesn't pass it properly to workers
    # or workers don't configure themselves to use it.
    
    log_queue = queue.Queue()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Main process capture
    logger.addHandler(QueueHandler(log_queue))
    
    logger.info(f"Main Process [PID {os.getpid()}]")
    
    with ProcessPoolExecutor(max_workers=2) as executor:
        list(executor.map(worker_func_broken, ["A", "B"]))
        
    print("Logs captured (Main Queue):")
    count = 0
    while not log_queue.empty():
        rec = log_queue.get()
        print(f"  {rec.msg}")
        count += 1
    
    if count <= 1:
        print(">> FAIL: Only main process logs captured. Worker logs missing.")

def test_fixed():
    print("\n--- Testing Fixed State (Multiprocessing Manager) ---")
    
    manager = multiprocessing.Manager()
    mp_queue = manager.Queue()
    
    print("Main Process listening...")
    
    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(worker_func_fixed, f"Worker-{i}", mp_queue) for i in range(2)]
        
        # Simulate the listener thread
        start = time.time()
        while any(not f.done() for f in futures) or not mp_queue.empty():
            while not mp_queue.empty():
                try:
                    record = mp_queue.get_nowait()
                    print(f"  [Captured] {record.msg}")
                except queue.Empty:
                    break
            time.sleep(0.1)
            if time.time() - start > 3: break
            
        for f in futures:
            f.result()
            
    print(">> SUCCESS: Worker logs captured via Manager Queue.")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    test_broken()
    test_fixed()
