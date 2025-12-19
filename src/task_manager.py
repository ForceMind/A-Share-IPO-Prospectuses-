import os
import sys

# Add the project root to sys.path to allow absolute imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import threading
import queue
import logging
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
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
        self.log_queue = queue.Queue(maxsize=1000)
        self.executor = None
        self.stop_event = threading.Event()
        self._lock = threading.Lock()
        
        # Initialize components
        self.downloader = Downloader()
        self.extractor = ProspectusExtractor()
        
        # Setup logging handler to capture logs into queue
        self._setup_logging()

    def _setup_logging(self):
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
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
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
        self.status["is_running"] = True
        self.status["current_action"] = action
        self.status["start_time"] = time.time()
        self.status["completed_tasks"] = 0
        self.status["failed_tasks"] = 0
        
        threading.Thread(target=self._run_pipeline, args=(action, limit), daemon=True).start()

    def stop_tasks(self):
        self.stop_event.set()
        self.status["is_running"] = False
        self.status["current_action"] = "Stopping..."
        logging.info("Stopping tasks...")

    def _run_pipeline(self, action: str, limit: Optional[int]):
        try:
            stock_list_path = os.path.join(DATA_DIR, 'stock_list.csv')
            if not os.path.exists(stock_list_path):
                logging.error("Stock list not found.")
                self.status["is_running"] = False
                return

            # 1. Download if needed
            if action in ["all", "download"] and not self.stop_event.is_set():
                self.status["current_action"] = "Downloading"
                logging.info("Starting download phase...")
                self.downloader.run(stock_list_path)

            # 2. Extract if needed
            if action in ["all", "extract"] and not self.stop_event.is_set():
                self._run_extraction(limit)

            logging.info("Pipeline finished.")
        except Exception as e:
            logging.error(f"Pipeline error: {e}")
        finally:
            self.status["is_running"] = False
            self.status["current_action"] = "Idle"

    def _run_extraction(self, limit: Optional[int]):
        self.status["current_action"] = "Extracting"
        
        # Load processed state
        from src.main import load_state, save_results, generate_report
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
        with ProcessPoolExecutor(max_workers=self.status["concurrency"]) as executor:
            from functools import partial
            # Prepare tasks
            futures = []
            for f in pdf_files:
                if self.stop_event.is_set(): break
                futures.append(executor.submit(process_pdf_worker, f, PDF_DIR))
            
            for future in futures:
                if self.stop_event.is_set():
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
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

        save_results(all_dividends, processed_files)
        generate_report(os.path.join(DATA_DIR, 'stock_list.csv'))
        logging.info(f"Extraction completed. Success: {self.status['completed_tasks']}, Failed: {self.status['failed_tasks']}")

# Singleton instance
task_manager = TaskManager()
