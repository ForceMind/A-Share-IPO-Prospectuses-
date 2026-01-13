
import unittest
import threading
import time
import sys
from unittest.mock import MagicMock

# Mock the module before importing the function that uses it
mock_downloader_module = MagicMock()
sys.modules['src.downloader'] = mock_downloader_module

# Mock the Downloader class
class MockDownloader:
    def process_stock(self, code, name):
        time.sleep(0.2)

mock_downloader_module.Downloader = MockDownloader

# Now import the worker function
from src.task_manager import _worker_download_chunk

class TestWorkerStop(unittest.TestCase):
    def test_stop_logic(self):
        print("Testing Stop Logic with Thread...")
        
        # Create a long chunk
        chunk = [(f'{i}', f'Stock{i}') for i in range(20)]
        
        # Mock queues/events
        log_queue = MagicMock()
        stop_event = threading.Event()
        
        # Run worker in a thread
        worker_thread = threading.Thread(target=_worker_download_chunk, args=(chunk, log_queue, stop_event))
        worker_thread.start()
        
        # Let it process a few
        time.sleep(0.5)
        
        # Signal stop
        print("Signaling stop...")
        stop_event.set()
        
        # Wait for finish
        worker_thread.join(timeout=1.0)
        
        self.assertFalse(worker_thread.is_alive(), "Worker thread should have stopped")
        print("Worker stopped successfully.")

if __name__ == '__main__':
    unittest.main()
