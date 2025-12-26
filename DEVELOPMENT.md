# Development Guide

## Workflow

1.  **Extraction Logic (`src/txt_extractor.py`):**
    *   Uses regex to find patterns like `Year ... Keyword ... Amount`.
    *   Modify `extract_dividends` method to adjust sensitivity or add new keywords.

2.  **Process Management (`src/txt_process_manager.py`):**
    *   Uses `ProcessPoolExecutor` for parallel processing.
    *   Manages logging queue for web UI.

3.  **Data Enrichment (`src/enrich_data.py`):**
    *   Queries `http://www.cninfo.com.cn/new/information/topSearch/query`.
    *   Used to resolve "Unknown" stock codes by searching company names or filenames.

## Debugging

*   **Scripts:** Use `scripts/inspect_excel.py` to check Excel output integrity.
*   **Logs:** Check `logs/pipeline.log` for runtime errors.

## Directory Structure Strategy
*   `src/`: Application logic.
*   `data/`: Data persistence.
*   `scripts/`: Ad-hoc tools.
*   `archive/`: Deprecated or temporary files.

## Common Issues
*   **Encoding:** Windows systems often introduce GBK/CP936 issues with filenames. The `clean_filename_garbage` function in `enrich_data.py` attempts to fix this.
*   **Excel Locking:** Ensure `extracted_dividends.xlsx` is closed in Excel before running extraction.
