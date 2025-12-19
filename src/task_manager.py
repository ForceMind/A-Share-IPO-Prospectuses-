import os
import sys

# Add the project root to sys.path to allow absolute imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import threading
import queue
import logging
import time
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed, wait, FIRST_COMPLETED
from typing import Dict, List, Any, Optional
from src.downloader import Downloader
from src.extractor import ProspectusExtractor, process_pdf_worker
from src.config import PDF_DIR, DATA_DIR, OUTPUT_DIR
import pandas as pd
import json

class TaskManager:
    def __init__(self):
        self.status = {
            "is_running": False,
            "total_tasks": 0,
            "completed_tasks": 0,
            "failed_tasks": 0,
            "current_action": "Idle",
            "concurrency": 4,
            "start_time": None,
            "elapsed_time": 0
        }
        # Web UI Queue (Thread-safe)
        self.log_queue = queue.Queue(maxsize=1000)
        
        # Multiprocessing Queue (Process-safe)
        self.manager = multiprocessing.Manager()
        self.mp_log_queue = self.manager.Queue()
        self.mp_stop_event = self.manager.Event()
        
        self.executor = None
        self.stop_event = threading.Event()
        self._lock = threading.Lock()
        
        # Start Log Bridge
        threading.Thread(target=self._log_listener, daemon=True).start()
        
        # Initialize components
        self.downloader = Downloader()
        self.extractor = ProspectusExtractor()
        
        # Setup logging handler to capture logs into queue
        self._setup_logging()

    def _log_listener(self):
        """
        Continuously reads from the multiprocessing queue and pushes to the thread-safe queue.
        This bridges the gap between worker processes and the websocket.
        Also re-emits logs to the root logger to ensure they appear in the file/console.
        """
        root_logger = logging.getLogger()
        
        while True:
            try:
                # Blocking get
                record = self.mp_log_queue.get()
                if record is None: # Poison pill
                    break
                
                # If it's a LogRecord object (from QueueHandler)
                if hasattr(record, 'msg'):
                    # 1. Push formatted string to Web UI Queue
                    msg = record.getMessage() 
                    import datetime
                    t = datetime.datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
                    formatted = f"{t} - [PID:{record.process}] - {record.levelname} - {msg}"
                    self.log_queue.put(formatted)
                    
                    # 2. Re-emit to Root Logger (File/Console)
                    # Skip if the source logger is root (to avoid potential recursive loops if configured wrong, 
                    # though our main process root logger feeds self.log_queue not self.mp_log_queue)
                    
                    # Fix: Ensure we don't double log if the handler is already attached to root in main process?
                    # The main process root logger has StreamHandler/FileHandler.
                    # We want these handlers to see the worker's log.
                    
                    if root_logger.isEnabledFor(record.levelno):
                         for h in root_logger.handlers:
                             # Prevent duplicating logs to the Web UI (QueueHandler)
                             if h.__class__.__name__ == 'QueueHandler':
                                 continue
                             h.handle(record)

                else:
                    # Raw string
                    self.log_queue.put(str(record))
                    
            except Exception as e:
                print(f"Log bridge error: {e}")
                time.sleep(1)

    def _setup_logging(self):
        from src.config import LOG_FORMAT
        
        # Handler for Main Process -> Web UI (Direct)
        class QueueHandler(logging.Handler):
            def __init__(self, log_queue):
                super().__init__()
                self.log_queue = log_queue

            def emit(self, record):
                try:
                    msg = self.format(record)
                    self.log_queue.put_nowait(msg)
                except queue.Full:
                    try:
                        self.log_queue.get_nowait()
                        self.log_queue.put_nowait(msg)
                    except:
                        pass
                except Exception:
                    self.handleError(record)

        handler = QueueHandler(self.log_queue)
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logging.getLogger().addHandler(handler)

    def get_logs(self) -> List[str]:
        logs = []
        while not self.log_queue.empty():
            logs.append(self.log_queue.get())
        return logs

    def get_status(self) -> Dict[str, Any]:
        if self.status["is_running"] and self.status["start_time"]:
            self.status["elapsed_time"] = int(time.time() - self.status["start_time"])
        return self.status

    def set_concurrency(self, value: int):
        with self._lock:
            self.status["concurrency"] = max(1, min(value, 32))
            # Note: We don't dynamically resize the active executor easily in standard concurrent.futures
            # but we can use this for the next batch or next start.
            logging.info(f"Concurrency set to {self.status['concurrency']}")

    def start_tasks(self, action: str = "all", limit: Optional[int] = None):
        if self.status["is_running"]:
            logging.warning("Tasks are already running")
            return
        
        self.stop_event.clear()
        self.mp_stop_event.clear()
        self.status["is_running"] = True
        self.status["current_action"] = action
        self.status["start_time"] = time.time()
        self.status["completed_tasks"] = 0
        self.status["failed_tasks"] = 0
        
        threading.Thread(target=self._run_pipeline, args=(action, limit), daemon=True).start()

    def stop_tasks(self):
        self.stop_event.set()
        self.mp_stop_event.set()
        self.status["is_running"] = False
        self.status["current_action"] = "Stopping..."
        logging.info("Stopping tasks...")
        
        # Force terminate pool if possible
        # Since we use context managers for ProcessPoolExecutor in _run_pipeline, we can't easily access the executor instance here.
        # But we can store it in self.executor
        
        # Wait, the current implementation creates a new executor in `_run_download_phase` and `_run_extraction`.
        # To support forced termination, we should elevate the executor to instance level or ensure we can kill it.
        # However, Python's ProcessPoolExecutor doesn't expose terminate().
        # We might need to switch to multiprocessing.Pool or just accept that we have to wait for current tasks to finish (or timeout).
        # But the requirement says "Implement stop_task using terminate() for immediate stopping".
        # This implies we might need to track the active child processes.
        
        # Since we can't easily change the executor structure without major refactor, 
        # let's try to iterate over active children of the current process and kill them if they look like our workers.
        # BUT, `_run_pipeline` runs in a thread. The workers are children of the MAIN process.
        
        try:
            import psutil
            current_process = psutil.Process()
            children = current_process.children(recursive=True)
            if children:
                logging.info(f"Found {len(children)} child processes. Terminating...")
                for child in children:
                    try:
                        child.terminate()
                    except psutil.NoSuchProcess:
                        pass
                
                # Wait for them to die
                _, alive = psutil.wait_procs(children, timeout=3)
                for p in alive:
                    try:
                        p.kill() # Force kill if terminate fails
                    except psutil.NoSuchProcess:
                        pass
        except ImportError:
            logging.warning("psutil module not found. Immediate process termination might not work reliably.")
        except Exception as e:
            logging.error(f"Error terminating processes: {e}")

    def _run_pipeline(self, action: str, limit: Optional[int]):
        try:
            stock_list_path = os.path.join(DATA_DIR, 'stock_list.csv')
            if not os.path.exists(stock_list_path):
                logging.error("Stock list not found.")
                self.status["is_running"] = False
                return

            logging.info(f"Task started: Action={action}, Limit={limit}, Concurrency={self.status['concurrency']}")

            # 1. Download if needed
            if action in ["all", "download"] and not self.stop_event.is_set():
                self.status["current_action"] = "Downloading"
                logging.info(f"Step 1/2: Starting download phase [PID:{os.getpid()}] (Action: {action})...")
                self._run_download_phase(stock_list_path, limit)
                logging.info("Step 1/2: Download phase completed.")

            # 2. Extract if needed
            if action in ["all", "extract"] and not self.stop_event.is_set():
                logging.info(f"Step 2/2: Starting extraction phase [PID:{os.getpid()}] (Action: {action}, Limit: {limit})...")
                self._run_extraction(limit)
                logging.info("Step 2/2: Extraction phase completed.")

            logging.info("Pipeline finished successfully.")
        except Exception as e:
            logging.error(f"Pipeline error: {e}")
        finally:
            self.status["is_running"] = False
            self.status["current_action"] = "Idle"

    def _run_download_phase(self, stock_list_path: str, limit: Optional[int]):
        """
        Executes the download phase using chunk-based parallelism.
        """
        # Load stock list
        try:
            df = pd.read_csv(stock_list_path)
            if limit:
                df = df.head(limit)
            
            total_stocks = len(df)
            self.status["total_tasks"] = total_stocks
            self.status["completed_tasks"] = 0
            
            # Prepare chunks
            concurrency = self.status["concurrency"]
            chunk_size = (total_stocks + concurrency - 1) // concurrency  # Ceiling division
            
            # Split dataframe into list of list of dicts/tuples for pickling
            # Or just pass the sub-dataframe rows
            chunks = []
            for i in range(0, total_stocks, chunk_size):
                chunk_df = df.iloc[i:i + chunk_size]
                # Convert to list of (code, name) tuples to avoid passing large DF
                chunk_data = [(str(row['code']).zfill(6), row['name']) for _, row in chunk_df.iterrows()]
                chunks.append(chunk_data)
            
            logging.info(f"Splitting {total_stocks} stocks into {len(chunks)} chunks (Concurrency: {concurrency})")
            
            # Spawn processes
            with ProcessPoolExecutor(max_workers=concurrency) as executor:
                futures = {
                    executor.submit(_worker_download_chunk, chunk, self.mp_log_queue, self.mp_stop_event): i 
                    for i, chunk in enumerate(chunks)
                }
                
                # Monitor progress
                while futures and not self.stop_event.is_set():
                    done, _ = wait(futures.keys(), timeout=0.5, return_when=FIRST_COMPLETED)
                    
                    for future in done:
                        try:
                            # result is (completed_count, failed_count)
                            completed, failed = future.result()
                            self.status["completed_tasks"] += completed
                            self.status["failed_tasks"] += failed
                        except Exception as e:
                            logging.error(f"Chunk processing failed: {e}")
                        
                        del futures[future]
                
                if self.stop_event.is_set():
                    logging.warning("Stop signal received. Terminating download workers...")
                    for f in futures:
                        f.cancel()
                    # ProcessPoolExecutor shutdown(wait=False) in Py 3.9+ allows killing?
                    # Actually standard executor doesn't support immediate kill well. 
                    # But since we use daemon processes usually, or check flags.
                    # In _worker_download_chunk we should check a shared event or just rely on 'daemon'.
                    # But 'daemon' processes are not allowed to spawn children (though we don't spawn more).
                    # Ideally we need a Manager().Event() for stop_signal passed to workers.
                    pass

        except Exception as e:
            logging.error(f"Download phase error: {e}")
            raise

def _worker_download_chunk(stock_list_chunk, log_queue, stop_event=None):
    """
    Worker process for downloading a chunk of stocks.
    """
    import logging
    import os
    from src.downloader import Downloader
    
    # Setup logging
    logger = logging.getLogger()
    if not any(h.__class__.__name__ == 'QueueHandler' for h in logger.handlers):
        logger.handlers = []
        # Define QueueHandler locally if needed, or import
        class QueueHandler(logging.Handler):
            def __init__(self, q):
                super().__init__()
                self.q = q
            def emit(self, record):
                try:
                    self.q.put_nowait(record)
                except:
                    self.handleError(record)
        logger.addHandler(QueueHandler(log_queue))
        logger.setLevel(logging.INFO)
    
    logger.info(f"Download Worker [PID:{os.getpid()}] started with {len(stock_list_chunk)} tasks.")
    
    downloader = Downloader()
    completed_count = 0
    failed_count = 0
    
    for code, name in stock_list_chunk:
        if stop_event and stop_event.is_set():
            logger.info(f"Download Worker [PID:{os.getpid()}] stopping...")
            break

        # We can't easily check the parent's stop_event here without passing a Manager Event.
        # But if the main process terminates the pool, this might stop? 
        # Actually standard Pool waits. 
        # For now, we process until done or killed.
        try:
            downloader.process_stock(code, name)
            completed_count += 1
        except Exception as e:
            logger.error(f"Failed to process {code} {name}: {e}")
            failed_count += 1
            
    logger.info(f"Download Worker [PID:{os.getpid()}] finished.")
    return completed_count, failed_count


    def _run_extraction(self, limit: Optional[int]):
        self.status["current_action"] = "Extracting"
        
        # Load processed state
        from src.pipeline_utils import load_state, save_results, generate_report
        processed_files, all_dividends = load_state()
        
        pdf_files = [f for f in os.listdir(PDF_DIR) if f.endswith('.pdf')]
        pdf_files = [f for f in pdf_files if f not in processed_files]
        if limit:
            pdf_files = pdf_files[:limit]
        
        self.status["total_tasks"] = len(pdf_files)
        if not pdf_files:
            logging.info("No new files to extract.")
            return

        logging.info(f"Starting extraction for {len(pdf_files)} files with concurrency {self.status['concurrency']}")
        
        # Use ProcessPoolExecutor for CPU-bound extraction
        # Must use Manager Queue for logging in multiprocessing
        # Note: self.mp_log_queue is already a Manager Queue created in __init__
        
        with ProcessPoolExecutor(max_workers=self.status["concurrency"]) as executor:
            from functools import partial
            # Prepare tasks
            # Pass mp_log_queue to worker
            futures = {executor.submit(process_pdf_worker, f, PDF_DIR, self.mp_log_queue): f for f in pdf_files}
            
            while futures and not self.stop_event.is_set():
                # Check log queue while waiting
                # We need to drain the logs proactively if the listener thread isn't fast enough
                # OR just rely on listener thread. The listener thread is independent.
                
                # Wait for at least one future to complete or timeout to check stop_event
                done, not_done = wait(futures.keys(), timeout=0.5, return_when=FIRST_COMPLETED)
                
                for future in done:

                    try:
                        pdf_file, dividends, error = future.result()
                        if error:
                            logging.error(f"Error processing {pdf_file}: {error}")
                            self.status["failed_tasks"] += 1
                        else:
                            if dividends:
                                all_dividends.extend(dividends)
                            self.status["completed_tasks"] += 1
                        
                        processed_files.add(pdf_file)
                        
                        # Periodic save
                        if self.status["completed_tasks"] % 10 == 0:
                            save_results(all_dividends, processed_files)
                            
                    except Exception as e:
                        logging.error(f"Future result error: {e}")
                        self.status["failed_tasks"] += 1
                    
                    # Remove processed future from the dictionary
                    del futures[future]

            # If stopped, cancel remaining
            if self.stop_event.is_set():
                for f in futures:
                    f.cancel()
                logging.info("Task execution cancelled.")

        save_results(all_dividends, processed_files)
        generate_report(os.path.join(DATA_DIR, 'stock_list.csv'))
        logging.info(f"Extraction completed. Success: {self.status['completed_tasks']}, Failed: {self.status['failed_tasks']}")

# Singleton instance
# task_manager = TaskManager()
_task_manager_instance = None

def get_task_manager():
    global _task_manager_instance
    if _task_manager_instance is None:
        _task_manager_instance = TaskManager()
    return _task_manager_instance
