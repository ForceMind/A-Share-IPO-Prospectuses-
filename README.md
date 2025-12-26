# A-Share IPO Prospectus Dividend Extractor

This project automatically extracts dividend information from A-Share IPO prospectuses (TXT format). It includes tools for downloading prospectuses, extracting dividend data, verifying results, and serving a web dashboard.

## Project Structure

*   `src/`: Source code for the application.
    *   `txt_extractor.py`: Core logic for parsing TXT files and regex matching.
    *   `txt_process_manager.py`: Manages multi-process extraction tasks.
    *   `enrich_data.py`: Fetches missing stock codes/names from Cninfo (Juchao) or EastMoney.
    *   `get_stock_list.py`: Fetches A-Share stock list.
    *   `web_server.py`: Flask-based web dashboard.
*   `data/`: Data storage (TXT files, Excel outputs).
*   `scripts/`: Utility scripts for debugging and maintenance.
*   `logs/`: Application logs.

## Setup

1.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Prepare Data:**
    Ensure your TXT files are located in `data/TXT/`.

## Usage

### 1. Extract Dividend Data
To run the main extraction process:
```bash
python src/main.py
```
Or use the launcher:
```bash
python src/launcher.py
```

### 2. Enrich Missing Data (Fix "Unknowns")
If the extraction results contain "Unknown" stock codes or names, run the enrichment script to query external sources (Cninfo):
```bash
python src/enrich_data.py
```

### 3. Web Dashboard
Start the web interface to view logs and status:
```bash
python src/web_server.py
```
Access at `http://localhost:5000`.

## Troubleshooting

*   **Encoding Issues:** The extractor handles UTF-8 and GBK encodings. If you see mojibake (garbled text), try running `src/enrich_data.py` to fix file metadata.
*   **Missing Stock Info:** Ensure `stock_list.csv` exists or run `src/enrich_data.py`.

## License

[License Name]
