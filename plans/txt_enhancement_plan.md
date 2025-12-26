# TXT Extraction Enhancement Plan

## Objective
Enhance the TXT extraction process by adding a web interface, multiprocessing support, metadata enrichment (Stock Code, IPO Date, Board), and Excel output.

## Architecture

### 1. Backend: `TxtProcessManager`
A new class `src/txt_process_manager.py` will manage the extraction lifecycle.
- **Responsibilities**:
    - Manage extraction state (Running, Idle, Stopping).
    - Handle multiprocessing using `ProcessPoolExecutor`.
    - Bridge logs from worker processes to the Web UI via `multiprocessing.Queue`.
    - Load and cache `stock_list.csv` for metadata enrichment.
- **Data Flow**:
    1. Scan `data/TXT` recursively.
    2. Parse file path to get `Board` and `Year`.
    3. Parse filename to get `Company Name`.
    4. Match `Company Name` against `stock_list.csv` to get `Stock Code` and `IPO Date`.
    5. Submit file to worker pool.
    6. Collect results and save to `data/TXT/extracted_dividends.xlsx`.

### 2. Extraction Logic: `src/txt_extractor.py`
- Refactor/Wrap existing logic to ensure it returns clean data structures.
- Ensure context text (sentences surrounding the match) is captured (already seems to be done in `raw_text`).

### 3. Web Server: `src/web_server.py`
- Integrate `TxtProcessManager`.
- Add API endpoints:
    - `POST /api/txt/start`
    - `POST /api/txt/stop`
    - `GET /api/txt/status`
    - `POST /api/txt/config` (Concurrency)
- Add WebSocket: `/ws/txt_logs`
- Add Route: `/txt_dashboard` serving `txt_dashboard.html`.

### 4. Frontend: `src/templates/txt_dashboard.html`
- Replicate `index.html` layout.
- Bind controls to the new `/api/txt/*` endpoints.
- Display "TXT Extraction" specific status.

## Implementation Steps

1.  **Create `src/txt_process_manager.py`**: Implement the manager class with multiprocessing and metadata lookup.
2.  **Update `src/web_server.py`**: Register routes and the new manager instance.
3.  **Create `src/templates/txt_dashboard.html`**: Build the UI.
4.  **Test**: Run the server and verify start/stop/logging and Excel output.

## Output Format (Excel)
Columns:
- Company Name
- Stock Code
- Board (e.g., 创业板, 科创板)
- IPO Date
- Year (of the document/event)
- Dividend Year (extracted)
- Amount
- Unit
- Raw Context (Source text)
- Filename
