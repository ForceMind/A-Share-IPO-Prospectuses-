import os
import logging
import threading
import queue
import time
import pandas as pd
import multiprocessing
import re
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from typing import Dict, Any, Optional, List
from src.txt_extractor import TxtExtractor
from src.config import DATA_DIR
from src.enrich_data import search_stock_cninfo

class TxtProcessManager:
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
        
        # Multiprocessing Manager
        self.manager = multiprocessing.Manager()
        self.mp_log_queue = self.manager.Queue()
        self.mp_stop_event = self.manager.Event()
        
        self.stop_event = threading.Event()
        self._lock = threading.Lock()
        
        # Start Log Bridge
        threading.Thread(target=self._log_listener, daemon=True).start()
        
        self._setup_logging()
        
        # Cache for stock list
        self.stock_metadata = self._load_stock_metadata()

    def _load_stock_metadata(self):
        """
        Loads stock metadata from stock_list.csv for enrichment.
        Returns a dictionary containing 'by_code' and 'by_name' lookups.
        """
        metadata = {'by_code': {}, 'by_name': {}}
        try:
            stock_list_path = os.path.join(DATA_DIR, 'stock_list.csv')
            if os.path.exists(stock_list_path):
                df = pd.read_csv(stock_list_path)
                # Ensure code is 6 digits
                df['code'] = df['code'].astype(str).str.zfill(6)
                for _, row in df.iterrows():
                    info = {
                        'code': row['code'],
                        'name': row['name'], # Short name
                        'listing_date': row['listing_date'],
                        'industry': row.get('industry', 'Unknown')
                    }
                    metadata['by_code'][row['code']] = info
                    metadata['by_name'][row['name']] = info
                    
                    # Add cleaned name (remove *ST, ST, N, C, U, W, V prefixes)
                    # Also remove spaces
                    clean_name = re.sub(r'^(\*?ST|N|C|U|W|V)', '', row['name']).strip()
                    if clean_name and clean_name != row['name']:
                        metadata['by_name'][clean_name] = info
                        
                logging.info(f"Loaded metadata: {len(metadata['by_code'])} codes, {len(metadata['by_name'])} names (including aliases).")
            else:
                logging.warning("stock_list.csv not found. Metadata enrichment will be limited.")
        except Exception as e:
            logging.error(f"Error loading stock metadata: {e}")
        return metadata

    def _log_listener(self):
        root_logger = logging.getLogger()
        while True:
            try:
                record = self.mp_log_queue.get()
                if record is None:
                    break
                
                if hasattr(record, 'msg'):
                    msg = record.getMessage()
                    import datetime
                    t = datetime.datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
                    formatted = f"{t} - [PID:{record.process}] - {record.levelname} - {msg}"
                    self.log_queue.put(formatted)
                    
                    if root_logger.isEnabledFor(record.levelno):
                         for h in root_logger.handlers:
                             if h.__class__.__name__ == 'QueueHandler': continue
                             h.handle(record)
                else:
                    self.log_queue.put(str(record))
            except Exception:
                time.sleep(1)

    def _setup_logging(self):
        from src.config import LOG_FORMAT
        class QueueHandler(logging.Handler):
            def __init__(self, log_queue):
                super().__init__()
                self.log_queue = log_queue
            def emit(self, record):
                try:
                    msg = self.format(record)
                    self.log_queue.put_nowait(msg)
                except:
                    pass
        
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

    def set_concurrency(self, concurrency: int):
        with self._lock:
            self.status["concurrency"] = max(1, min(concurrency, 50))
            logging.info(f"TXT Extraction Concurrency updated: {self.status['concurrency']}")

    def start_tasks(self, limit: Optional[int] = None):
        if self.status["is_running"]:
            logging.warning("TXT tasks are already running")
            return
        
        self.stop_event.clear()
        self.mp_stop_event.clear()
        self.status["is_running"] = True
        self.status["current_action"] = "Extracting TXT"
        self.status["start_time"] = time.time()
        self.status["completed_tasks"] = 0
        self.status["failed_tasks"] = 0
        
        threading.Thread(target=self._run_extraction, args=(limit,), daemon=True).start()

    def stop_tasks(self):
        self.stop_event.set()
        self.mp_stop_event.set()
        self.status["is_running"] = False
        self.status["current_action"] = "Stopping..."
        logging.info("Stopping TXT tasks...")

    def _run_extraction(self, limit: Optional[int]):
        try:
            base_dir = os.path.join(DATA_DIR, "TXT")
            
            # --- ENHANCED FILE SELECTION LOGIC ---
            logging.info(f"Scanning {base_dir} for TXT files...")
            
            # 1. Scan all files
            all_candidates = []
            for root, dirs, files in os.walk(base_dir):
                for file in files:
                    if file.endswith(".txt") and "extracted_dividends" not in file:
                        all_candidates.append(os.path.join(root, file))
            
            # 2. Group by Company and Filter by Date
            company_files = {} # {company_name: (file_path, file_date_str)}
            
            # Helper to get date from filename
            def get_file_date(fname):
                match = re.search(r'(\d{4}-\d{2}-\d{2})', fname)
                return match.group(1) if match else "1900-01-01"

            # Filter criteria
            start_date = pd.Timestamp("2019-01-01")
            end_date = pd.Timestamp("2023-12-31")
            
            # Pre-process metadata for faster lookup
            valid_companies = set()
            for code, info in self.stock_metadata['by_code'].items():
                try:
                    l_date = pd.to_datetime(info['listing_date'])
                    if start_date <= l_date <= end_date:
                        valid_companies.add(info['name'])
                        # Also add cleaned name
                        clean_name = re.sub(r'^(\*?ST|N|C|U|W|V)', '', info['name']).strip()
                        if clean_name:
                            valid_companies.add(clean_name)
                except:
                    pass
            
            logging.info(f"Filtering for companies listed between 2019-2023. Found {len(valid_companies)} valid company names in metadata.")

            # --- NEW LOGIC: Merge PDF list if TXT is missing ---
            # We want to ensure we cover as many companies as possible.
            # If we have a TXT, great. If not, we should check if we have a PDF.
            # If PDF exists but TXT doesn't, we should ideally process the PDF (convert to TXT then extract).
            # For this "Enhanced" version, we will add a fallback:
            # If a company is in valid_companies but not in company_files, check PDF_DIR.
            
            from src.config import PDF_DIR
            
            # Scan PDF dir for missing companies
            pdf_candidates = []
            if os.path.exists(PDF_DIR):
                logging.info(f"Scanning {PDF_DIR} for fallback PDFs...")
                for root, dirs, files in os.walk(PDF_DIR):
                    for file in files:
                        if file.lower().endswith(".pdf"):
                            pdf_candidates.append(os.path.join(root, file))
            
            for pdf_path in pdf_candidates:
                filename = os.path.basename(pdf_path)
                parts = filename.split('_')
                company_name = parts[0]
                
                # Check if this company is already covered by TXT
                if company_name in company_files:
                    continue
                    
                # Check if it's a target company
                is_target = False
                if company_name in valid_companies:
                    is_target = True
                else:
                    for vc in valid_companies:
                        if vc == company_name or (len(vc) > 3 and vc in filename):
                            is_target = True
                            company_name = vc
                            break
                
                if is_target:
                    # We found a PDF for a missing company!
                    # We need to process this PDF.
                    # Since _process_txt_worker expects a TXT file, we need a way to handle PDF.
                    # We can either:
                    # 1. Convert PDF to TXT on the fly (slow)
                    # 2. Pass PDF path to worker and let worker handle it (requires worker update)
                    
                    # Let's update the worker to handle PDF files using pdfplumber if needed.
                    # We'll mark this as a PDF task.
                    f_date = get_file_date(filename)
                    
                    # Add to company_files, but note it's a PDF
                    if company_name not in company_files:
                        company_files[company_name] = (pdf_path, f_date)
                    else:
                        if f_date > company_files[company_name][1]:
                            company_files[company_name] = (pdf_path, f_date)

            final_files = [p[0] for p in company_files.values()]
            
            # Log missing companies
            found_companies = set(company_files.keys())
            missing_companies = valid_companies - found_companies
            logging.info(f"Coverage Report: Found Documents (TXT/PDF) for {len(found_companies)} / {len(valid_companies)} target companies.")
            if len(missing_companies) > 0:
                logging.info(f"Missing Documents for {len(missing_companies)} companies. Examples: {list(missing_companies)[:5]}")
            
            if limit:
                final_files = final_files[:limit]
                
            self.status["total_tasks"] = len(final_files)
            logging.info(f"Found {len(all_candidates)} total files. Selected {len(final_files)} unique files for processing after filtering.")
            
            results = []
            stock_info_list = []
            
            # Get API Key from env
            api_key = os.environ.get("DEEPSEEK_API_KEY")
            if api_key:
                logging.info("DeepSeek API Key detected. AI Enhanced extraction enabled.")
            else:
                logging.info("No DeepSeek API Key found. Using Regex extraction only.")

            with ProcessPoolExecutor(max_workers=self.status["concurrency"]) as executor:
                futures = {
                    executor.submit(_process_txt_worker, f, self.mp_log_queue, self.stock_metadata, api_key): f 
                    for f in final_files
                }
                
                while futures and not self.stop_event.is_set():
                    done, _ = wait(futures.keys(), timeout=0.5, return_when=FIRST_COMPLETED)
                    
                    for future in done:
                        try:
                            res_dividends, res_stock_info = future.result()
                            if res_dividends:
                                results.extend(res_dividends)
                            if res_stock_info:
                                stock_info_list.append(res_stock_info)
                            self.status["completed_tasks"] += 1
                        except Exception as e:
                            logging.error(f"Task failed: {e}")
                            self.status["failed_tasks"] += 1
                        
                        del futures[future]

            if self.stop_event.is_set():
                 logging.warning("TXT extraction stopped by user.")
            
            # Save results to Excel
            self._save_to_excel(results, stock_info_list, base_dir)

        except Exception as e:
            logging.error(f"TXT Pipeline error: {e}")
        finally:
            self.status["is_running"] = False
            self.status["current_action"] = "Idle"
            logging.info("TXT Extraction finished.")

    def _save_to_excel(self, dividends, stock_infos, base_dir):
        try:
            output_file = os.path.join(base_dir, "extracted_dividends.xlsx")
            
            # Ensure lists
            dividends = dividends or []
            stock_infos = stock_infos or []
            
            logging.info(f"Saving to Excel. Dividends count: {len(dividends)}, Stock Info count: {len(stock_infos)}")
            
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # 1. Stock Info Sheet
                if stock_infos:
                    df_info = pd.DataFrame(stock_infos)
                    df_info = df_info.drop_duplicates(subset=['company_name', 'filename']) # Use keys available in dict before rename
                    
                    cols_map = {
                        'stock_name': 'Stock Name',
                        'stock_code': 'Stock Code',
                        'board': 'Board',
                        'industry': 'Industry',
                        'ipo_date': 'IPO Date',
                        'company_name': 'Full Company Name',
                        'filename': 'Source File'
                    }
                    
                    # Ensure columns exist
                    for k in cols_map.keys():
                        if k not in df_info.columns:
                            df_info[k] = "Unknown"
                            
                    # Rename and Reorder
                    df_info = df_info.rename(columns=cols_map)
                    target_cols = list(cols_map.values())
                    df_info = df_info[target_cols]
                    
                    df_info.to_excel(writer, sheet_name='Stock List', index=False)
                else:
                    logging.warning("No stock info to save. Creating empty Stock List sheet.")
                    pd.DataFrame(columns=['Stock Name', 'Stock Code', 'Board', 'Industry', 'IPO Date', 'Full Company Name', 'Source File']).to_excel(writer, sheet_name='Stock List', index=False)

                # 2. Dividends Sheet
                if dividends:
                    df_div = pd.DataFrame(dividends)
                    
                    cols_map_div = {
                        'stock_name': 'Stock Name',
                        'stock_code': 'Stock Code',
                        'dividend_year': 'Dividend Year',
                        'amount_with_unit': 'Dividend Amount',
                        'raw_context': 'Context Source',
                        'filename': 'Source File'
                    }
                    
                    # Ensure columns
                    for k in cols_map_div.keys():
                        if k not in df_div.columns:
                            df_div[k] = None

                    # Rename and Reorder
                    df_div = df_div.rename(columns=cols_map_div)
                    target_cols_div = list(cols_map_div.values())
                    df_div = df_div[target_cols_div]
                    
                    df_div.to_excel(writer, sheet_name='Dividends', index=False)
                else:
                    logging.warning("No dividends to save. Creating empty Dividends sheet.")
                    pd.DataFrame(columns=['Stock Name', 'Stock Code', 'Dividend Year', 'Dividend Amount', 'Context Source', 'Source File']).to_excel(writer, sheet_name='Dividends', index=False)
            
            logging.info(f"Results saved to {output_file}")
        except Exception as e:
            logging.error(f"Failed to save Excel: {e}")
            import traceback
            logging.error(traceback.format_exc())

def _process_txt_worker(file_path, log_queue, metadata, api_key=None):
    """
    Worker function for processing a single TXT file.
    Returns a tuple: (list_of_dividends, stock_info_dict)
    """
    import logging
    
    # Setup worker logging
    logger = logging.getLogger()
    if not any(h.__class__.__name__ == 'QueueHandler' for h in logger.handlers):
        class QueueHandler(logging.Handler):
            def __init__(self, q):
                super().__init__()
                self.q = q
            def emit(self, record):
                try:
                    self.q.put_nowait(record)
                except:
                    pass
        logger.addHandler(QueueHandler(log_queue))
        logger.setLevel(logging.INFO)

    try:
        # Log start of processing for this file
        logger.info(f"Processing file: {os.path.basename(file_path)}")
        
        extractor = TxtExtractor()
        
        # Check if file is PDF
        if file_path.lower().endswith('.pdf'):
            # On-the-fly PDF text extraction
            try:
                import pdfplumber
                content = ""
                with pdfplumber.open(file_path) as pdf:
                    # Limit pages to avoid memory issues on huge docs? 
                    # Or just read all text. Prospectuses are large.
                    # Let's try reading text from all pages.
                    for page in pdf.pages:
                        text = page.extract_text()
                        if text:
                            content += text + "\n"
                
                if not content:
                    logger.warning(f"No text extracted from PDF {os.path.basename(file_path)} (Scanned?)")
                    return [], None
                    
                # Use the content for extraction
                # We need to manually call extract_dividends_enhanced since extract_from_file expects a file path to read
                use_ai = bool(api_key)
                dividends = extractor.extract_dividends_enhanced(content, use_ai=use_ai, api_key=api_key)
                
                # Construct data dict manually
                filename = os.path.basename(file_path)
                parts = filename.split('_')
                company_name = parts[0] if len(parts) >= 1 else filename.replace(".pdf", "")
                
                data = {
                    "company_name": company_name,
                    "filename": filename,
                    "dividends": dividends
                }
                
            except Exception as e:
                logger.error(f"Error converting PDF {os.path.basename(file_path)}: {e}")
                return [], None
        else:
            # Normal TXT processing
            # Pass API Key to extractor
            data = extractor.extract_from_file(file_path, api_key=api_key)
        
        if not data:
            logger.warning(f"No data extracted from {os.path.basename(file_path)}")
            return [], None
            
        full_company_name = data['company_name']
        filename = data['filename']
        
        # Parse Board from path if possible
        # Expected: .../data/TXT/{Board}/{Year}/Filename.txt
        path_parts = os.path.normpath(file_path).split(os.sep)
        board = "Unknown"
        try:
            # Case-insensitive search for 'TXT'
            path_parts_upper = [p.upper() for p in path_parts]
            if 'TXT' in path_parts_upper:
                txt_index = path_parts_upper.index('TXT')
                if len(path_parts) > txt_index + 1:
                    board = path_parts[txt_index + 1]
        except:
            pass
            
        # --- ENHANCED MATCHING LOGIC ---
        matched_info = None
        
        # 1. Try to find Stock Code in Filename (Most reliable if file is named like '300001_Name.txt')
        code_match = re.search(r'(\d{6})', filename)
        if code_match:
            code_candidate = code_match.group(1)
            matched_info = metadata['by_code'].get(code_candidate)
            
        # 2. If not found, try to match Full Company Name with Short Names in metadata
        if not matched_info and full_company_name:
            # Find the longest matching short name to avoid partial matches
            candidates = []
            for short_name, info in metadata['by_name'].items():
                if short_name in full_company_name:
                    candidates.append((short_name, info))
            
            if candidates:
                # Sort by length of short_name descending to pick the most specific match
                candidates.sort(key=lambda x: len(x[0]), reverse=True)
                matched_info = candidates[0][1]
        
        # 3. Last resort: Try if Full Name contains any code (unlikely but possible)
        if not matched_info and full_company_name:
             code_in_name = re.search(r'(\d{6})', full_company_name)
             if code_in_name:
                 matched_info = metadata['by_code'].get(code_in_name.group(1))

        # 4. External Fallback: Search Cninfo
        if not matched_info and full_company_name:
            # Only search if we have a reasonable name
            # Strip common suffixes to improve search quality
            clean_search_name = full_company_name.replace("股份有限公司", "").replace("有限责任公司", "")
            
            search_queries = [clean_search_name]
            
            # Try stripping common city/province prefixes
            prefixes = ["北京", "上海", "深圳", "广东", "江苏", "浙江", "安徽", "山东", "四川", "湖北", "湖南", "福建", "河南", "河北", "天津", "重庆"]
            for p in prefixes:
                if clean_search_name.startswith(p):
                    search_queries.append(clean_search_name[len(p):])
                    break # Only strip one prefix
            
            if len(clean_search_name) > 2:
                # logger is not directly available here as it's inside worker, but we set it up
                logger.info(f"Local match failed for {full_company_name}, searching Cninfo with queries: {search_queries}")
                try:
                    for query in search_queries:
                        cninfo_result = search_stock_cninfo(query)
                        if cninfo_result:
                            matched_info = {
                                'code': cninfo_result['code'],
                                'name': cninfo_result['name'],
                                'industry': 'Unknown', # Cninfo search doesn't return industry directly in simple search
                                'listing_date': 'Unknown'
                            }
                            logger.info(f"Cninfo found: {matched_info['name']} ({matched_info['code']}) using query '{query}'")
                            break
                except Exception as e:
                    logger.warning(f"Cninfo search failed for {full_company_name}: {e}")

        # --- CONSTRUCT BASIC INFO ---
        stock_name = matched_info['name'] if matched_info else "Unknown"
        stock_code = matched_info['code'] if matched_info else (code_match.group(1) if code_match else "Unknown")
        industry = matched_info['industry'] if matched_info else "Unknown"
        ipo_date = matched_info['listing_date'] if matched_info else "Unknown"

        # Infer Board from Stock Code if available, otherwise keep folder-based board
        def infer_board(code):
            if not code or not code.isdigit() or len(code) != 6:
                return None
            if code.startswith(('600', '601', '603', '605')): return "沪市主板"
            if code.startswith('688'): return "科创板"
            if code.startswith(('000', '001', '002', '003')): return "深市主板"
            if code.startswith(('300', '301')): return "创业板"
            if code.startswith(('4', '8', '92')): return "北交所"
            return None

        inferred_board = infer_board(stock_code)
        if inferred_board:
            board = inferred_board

        # Special fallback for Name if still unknown but we have full name
        if stock_name == "Unknown" and full_company_name:
            stock_name = full_company_name  # Use full name as fallback

        stock_info_record = {
            "stock_name": stock_name,
            "stock_code": stock_code,
            "board": board,
            "industry": industry,
            "ipo_date": ipo_date,
            "company_name": full_company_name,
            "filename": filename
        }

        if not data['dividends']:
            return [], stock_info_record
            
        results = []
        for div in data['dividends']:
            # Combine amount and unit if unit exists
            amount_val = div.get('amount_text', '')
            unit_val = div.get('unit', '')
            if unit_val and unit_val not in amount_val:
                amount_str = f"{amount_val}{unit_val}"
            else:
                amount_str = amount_val

            results.append({
                "stock_name": stock_name,
                "stock_code": stock_code,
                "dividend_year": div.get('year', ''),
                "amount_with_unit": amount_str,
                "raw_context": div.get('raw_text', ''),
                "filename": filename
            })
            
        return results, stock_info_record
        
    except Exception as e:
        logger.error(f"Error processing {os.path.basename(file_path)}: {e}")
        return [], None

# Singleton
_txt_manager_instance = None
def get_txt_manager():
    global _txt_manager_instance
    if _txt_manager_instance is None:
        _txt_manager_instance = TxtProcessManager()
    return _txt_manager_instance
