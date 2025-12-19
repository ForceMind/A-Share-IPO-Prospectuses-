# Implementation Plan: Multiprocessing Refactor

## 1. Architecture Changes
The current architecture fails to capture logs from child processes because `logging` handlers are not automatically pickled/shared across process boundaries in Python (especially on Windows). We need a centralized `multiprocessing.Queue` to aggregate logs.

### New Flow:
1. `TaskManager` initializes a `multiprocessing.Manager().Queue()` (let's call it `mp_log_queue`).
2. `TaskManager` starts a background thread (`LogConsumer`) that:
   - Reads from `mp_log_queue`.
   - Pushes messages to the existing `self.log_queue` (used for WebSockets).
3. Worker processes (Downloader/Extractor) receive `mp_log_queue` as an argument.
4. Workers configure a `QueueHandler` at startup to send all logs to `mp_log_queue`.

## 2. File Discovery Optimization
**Current:** `Downloader.run()` calls `os.listdir(PDF_DIR)` inside the loop for every single stock code (5000+ times).
**Fix:** 
- Scan `PDF_DIR` *once* at the beginning.
- Store existing codes in a `set`.
- Check against the set in the loop.

## 3. Detailed Steps

### Step 1: Create Reproduction Script (Code Mode)
- Create `reproduce_mp_logging.py` to confirm the fix pattern (MP Queue vs Thread Queue).

### Step 2: Refactor `src/task_manager.py`
- Import `multiprocessing`.
- Initialize `self.manager = multiprocessing.Manager()` and `self.mp_log_queue = self.manager.Queue()`.
- Add `_log_listener` method to bridge `mp_log_queue` -> `self.log_queue`.
- Update `_run_extraction` to pass `self.mp_log_queue` to workers.

### Step 3: Refactor `src/extractor.py`
- Modify `process_pdf_worker` signature: `def process_pdf_worker(pdf_file, pdf_dir, log_queue=None):`.
- Inside worker:
  ```python
  if log_queue:
      root = logging.getLogger()
      if not any(isinstance(h, QueueHandler) for h in root.handlers):
          root.handlers = [] # Clear default stream handlers to avoid double printing/loss
          root.addHandler(QueueHandler(log_queue))
          root.setLevel(logging.INFO)
  ```

### Step 4: Refactor `src/downloader.py`
- Modify `run` method.
- Add `existing_codes = set()` logic at start.
- Optimizing checking loop.

### Step 5: Refactor `src/main.py`
- Ensure standalone CLI also supports the optimization (optional but recommended).

## 4. Verification
- Run `reproduce_mp_logging.py`.
- Run the full pipeline with a small limit (`limit=10`) via Web UI.
- Verify "Web interface shows multiple PIDs".
