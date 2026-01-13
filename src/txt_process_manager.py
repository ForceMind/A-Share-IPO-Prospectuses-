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

try:
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
except ImportError:
    from openpyxl.utils.cell import ILLEGAL_CHARACTERS_RE

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
            "elapsed_time": 0,
            "total_ai_cost": 0.0,
            "ai_cost_limit": 10.0
        }
        
        self.log_queue = queue.Queue(maxsize=1000)
        self.manager = None
        self.mp_log_queue = None
        
        self.stop_event = threading.Event()
        self._lock = threading.Lock()
        
        self._setup_logging()
        self.stock_metadata = self._load_stock_metadata()

    def _load_stock_metadata(self):
        metadata = {'by_code': {}, 'by_name': {}}
        try:
            stock_list_path = os.path.join(DATA_DIR, 'stock_list.csv')
            if os.path.exists(stock_list_path):
                df = pd.read_csv(stock_list_path)
                df['code'] = df['code'].astype(str).str.zfill(6)
                for _, row in df.iterrows():
                    info = {
                        'code': row['code'],
                        'name': row['name'],
                        'listing_date': row['listing_date'],
                        'industry': row.get('industry', 'Unknown')
                    }
                    metadata['by_code'][row['code']] = info
                    metadata['by_name'][row['name']] = info
                    
                    clean_name = re.sub(r'^(\*?ST|N|C|U|W|V)', '', row['name']).strip()
                    if clean_name and clean_name != row['name']:
                        metadata['by_name'][clean_name] = info
                # logging.info(f"Loaded metadata: {len(metadata['by_code'])} codes.")
            else:
                logging.warning("stock_list.csv not found.")
        except Exception as e:
            logging.error(f"Error loading stock metadata: {e}")
        return metadata

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

    def _log_listener(self, queue):
        while True:
            try:
                record = queue.get()
                if record is None: break
                if hasattr(record, 'msg'):
                    msg = record.getMessage()
                    import datetime
                    t = datetime.datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
                    formatted = f"{t} - [PID:{record.process}] - {record.levelname} - {msg}"
                    self.log_queue.put(formatted)
                else:
                    self.log_queue.put(str(record))
            except (EOFError, BrokenPipeError):
                break
            except Exception:
                time.sleep(0.1)

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

    def set_cost_limit(self, limit: float):
        with self._lock:
            self.status["ai_cost_limit"] = max(0.0, limit)
            logging.info(f"AI Cost Limit updated: ¥{self.status['ai_cost_limit']:.4f}")

    def start_tasks(self, limit: Optional[int] = None):
        if self.status["is_running"]:
            logging.warning("TXT tasks are already running")
            return
        
        self.stop_event.clear()
        self.status["is_running"] = True
        self.status["current_action"] = "Extracting TXT"
        self.status["start_time"] = time.time()
        self.status["completed_tasks"] = 0
        self.status["failed_tasks"] = 0
        self.status["total_ai_cost"] = 0.0 
        
        threading.Thread(target=self._run_extraction, args=(limit,), daemon=True).start()

    def stop_tasks(self):
        self.stop_event.set()
        self.status["is_running"] = False
        self.status["current_action"] = "Stopping..."
        logging.info("Stopping TXT tasks...")

    def _run_extraction(self, limit: Optional[int]):
        try:
            base_dir = os.path.join(DATA_DIR, "TXT")
            logging.info(f"Scanning {base_dir} for TXT files...")
            
            all_candidates = []
            if os.path.exists(base_dir):
                for root, dirs, files in os.walk(base_dir):
                    for file in files:
                        if file.endswith(".txt") and "extracted_dividends" not in file:
                            all_candidates.append(os.path.join(root, file))

            valid_companies = set()
            for code, info in self.stock_metadata['by_code'].items():
                try:
                    # Logic simplified - accept all valid companies
                    valid_companies.add(info['name'])
                    clean_name = re.sub(r'^(\*?ST|N|C|U|W|V)', '', info['name']).strip()
                    if clean_name: valid_companies.add(clean_name)
                except: pass

            company_files = {}
            for fpath in all_candidates:
                 filename = os.path.basename(fpath)
                 parts = filename.split('_')
                 company_name = parts[0]
                 company_files[company_name] = fpath

            # PDF Fallback
            from src.config import PDF_DIR
            if os.path.exists(PDF_DIR):
                for root, dirs, files in os.walk(PDF_DIR):
                    for file in files:
                        if file.lower().endswith(".pdf"):
                            parts = file.split('_')
                            cname = parts[0]
                            if cname not in company_files: # Simple check
                                company_files[cname] = os.path.join(root, file)

            final_files = list(company_files.values())
            if limit: final_files = final_files[:limit]
            
            self.status["total_tasks"] = len(final_files)
            logging.info(f"Selected {len(final_files)} files for processing.")

            api_key = os.environ.get("DEEPSEEK_API_KEY")
            
            # --- MULTIPROCESSING EXECUTION ---
            # Lazy init Manager to avoid Fork Bomb
            with multiprocessing.Manager() as manager:
                self.manager = manager
                self.mp_log_queue = manager.Queue()
                
                log_thread = threading.Thread(target=self._log_listener, args=(self.mp_log_queue,), daemon=True)
                log_thread.start()
                
                try:
                    with ProcessPoolExecutor(max_workers=self.status["concurrency"]) as executor:
                        futures = {
                            executor.submit(_process_txt_worker, f, self.mp_log_queue, self.stock_metadata, api_key, self.status["ai_cost_limit"], self.status["total_ai_cost"]): f 
                            for f in final_files
                        }
                        
                        results = []
                        stock_info_list = []
                        
                        while futures and not self.stop_event.is_set():
                            done, _ = wait(futures.keys(), timeout=0.5, return_when=FIRST_COMPLETED)
                            for future in done:
                                try:
                                    res_dividends, res_stock_info, cost_incurred = future.result()
                                    if res_dividends: results.extend(res_dividends)
                                    if res_stock_info: stock_info_list.append(res_stock_info)
                                    
                                    self.status["total_ai_cost"] += cost_incurred
                                    self.status["completed_tasks"] += 1
                                    
                                    self._save_to_excel(results, stock_info_list, base_dir)
                                except Exception as e:
                                    logging.error(f"Task failed: {e}")
                                    self.status["failed_tasks"] += 1
                                del futures[future]
                finally:
                    try: self.mp_log_queue.put(None)
                    except: pass
                    log_thread.join(timeout=1.0)
            
            self._save_to_excel(results, stock_info_list, base_dir)

        except Exception as e:
            logging.error(f"TXT Pipeline error: {e}")
            import traceback
            logging.error(traceback.format_exc())
        finally:
            self.manager = None
            self.status["is_running"] = False
            self.status["current_action"] = "Idle"
            logging.info("TXT Extraction finished.")

    def _save_to_excel(self, dividends, stock_infos, base_dir):
        try:
            output_file = os.path.join(base_dir, "extracted_dividends.xlsx")
            dividends = dividends or []
            stock_infos = stock_infos or []
            
            def sanitize_df(df):
                return df.map(lambda x: ILLEGAL_CHARACTERS_RE.sub('', str(x)) if isinstance(x, str) else x)

            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                if stock_infos:
                    df_info = pd.DataFrame(stock_infos)
                    df_info = df_info.drop_duplicates(subset=['company_name', 'filename'])
                    cols_map = {
                        'stock_name': '股票简称', 'stock_code': '股票代码', 'board': '板块',
                        'industry': '行业', 'ipo_date': '上市日期', 'company_name': '公司全称',
                        'filename': '来源文件'
                    }
                    for k in cols_map.keys():
                        if k not in df_info.columns: df_info[k] = "Unknown"
                    df_info = df_info.rename(columns=cols_map)
                    df_info = sanitize_df(df_info[[v for k,v in cols_map.items()]])
                    df_info.to_excel(writer, sheet_name='Stock List', index=False)
                else:
                    pd.DataFrame(columns=['股票简称', '股票代码', '板块', '行业', '上市日期', '公司全称', '来源文件']).to_excel(writer, sheet_name='Stock List', index=False)

                if dividends:
                    df_div = pd.DataFrame(dividends)
                    cols_map_div = {
                        'stock_name': '股票简称', 'stock_code': '股票代码', 'year': '会计年度',
                        'metric': '指标类型', 'amount_with_unit': '金额(万元)', 'raw_context': '原文上下文',
                        'filename': '来源文件', 'is_ai': '是否AI提取', 'ai_prompt': 'AI提示词',
                        'ai_response': 'AI响应内容', 'ai_cost': 'AI成本(元)'
                    }
                    if 'dividend_year' in df_div.columns: df_div['year'] = df_div['dividend_year']
                    
                    if 'metric' in df_div.columns:
                        metric_map = {'dividend': '现金分红总额', 'net_profit': '归母净利润', 'operating_cash_flow': '经营活动现金流净额', 'total_dividend': '现金分红总额'}
                        df_div['metric'] = df_div['metric'].map(lambda x: metric_map.get(x, x))
                    
                    if 'ai_response' in df_div.columns:
                        df_div['ai_response'] = df_div['ai_response'].astype(str).str.replace(r'[{}]', '', regex=True)

                    df_div = df_div.rename(columns=cols_map_div)
                    available = [c for c in cols_map_div.values() if c in df_div.columns]
                    df_div = sanitize_df(df_div[available])
                    df_div.to_excel(writer, sheet_name='Financial Data', index=False)
                else:
                     pd.DataFrame(columns=['股票简称', '股票代码', '会计年度', '指标类型', '金额(万元)', '原文上下文', '来源文件', '是否AI提取', 'AI提示词', 'AI响应内容', 'AI成本(元)']).to_excel(writer, sheet_name='Financial Data', index=False)
            
                pd.DataFrame([{
                    "累计AI成本(元)": self.status.get("total_ai_cost", 0.0),
                    "成本上限(元)": self.status.get("ai_cost_limit", 0.0),
                    "总任务数": self.status.get("total_tasks", 0),
                    "已完成任务": self.status.get("completed_tasks", 0)
                }]).to_excel(writer, sheet_name='Summary', index=False)
        except Exception as e:
            logging.error(f"Failed to save Excel: {e}")

def _process_txt_worker(file_path, log_queue, metadata, api_key=None, cost_limit=0.0, current_cost=0.0):
    import logging
    logger = logging.getLogger()
    if not any(h.__class__.__name__ == 'QueueHandler' for h in logger.handlers):
        class QueueHandler(logging.Handler):
            def __init__(self, q):
                super().__init__()
                self.q = q
            def emit(self, record):
                try: self.q.put_nowait(record)
                except: pass
        logger.addHandler(QueueHandler(log_queue))
        logger.setLevel(logging.INFO)

    try:
        logger.info(f"Processing file: {os.path.basename(file_path)}")
        extractor = TxtExtractor()
        dividends = []
        cost = 0.0

        if file_path.lower().endswith('.pdf'):
            try:
                import pdfplumber
                content = ""
                with pdfplumber.open(file_path) as pdf:
                    for page in pdf.pages:
                        t = page.extract_text()
                        if t: content += t + "\\n"
                
                if not content: return [], None, 0.0
                
                force = True if api_key else False
                dividends, cost = extractor.extract_financial_data(content, use_ai=bool(api_key), api_key=api_key, cost_limit=cost_limit, current_cost=current_cost, force_ai=force)
            except Exception as e:
                logger.error(f"Error converting PDF {os.path.basename(file_path)}: {e}")
                return [], None, 0.0
        else:
            force = True if api_key else False
            data = extractor.extract_from_file(file_path, api_key=api_key, cost_limit=cost_limit, current_cost=current_cost, force_ai=force)
            if data:
                dividends = data['dividends']
                cost = data['cost']

        full_company_name = "Unknown"
        filename = os.path.basename(file_path)
        
        parts = filename.split('_')
        if len(parts) >= 1: full_company_name = parts[0]

        # Basic Info Construction
        code_match = re.search(r'(\d{6})', filename)
        code = code_match.group(1) if code_match else "Unknown"
        stock_name = full_company_name
        
        # Metadata Match
        matched = None
        if code != "Unknown" and code in metadata['by_code']:
            matched = metadata['by_code'][code]
        elif full_company_name in metadata['by_name']:
            matched = metadata['by_name'][full_company_name]
        else:
            # Fallback: Fuzzy match (Short name inside Long name)
            # This is O(N) but N is small (~5000)
            for short_name, info in metadata['by_name'].items():
                if short_name in full_company_name:
                    matched = info
                    break
            
        if matched:
            stock_name = matched['name']
            code = matched['code']
            
        stock_info = {
            "stock_name": stock_name,
            "stock_code": code,
            "board": "Unknown", # Simplified inference
            "industry": matched.get('industry', 'Unknown') if matched else 'Unknown',
            "ipo_date": matched.get('listing_date', 'Unknown') if matched else 'Unknown',
            "company_name": full_company_name,
            "filename": filename
        }

        results = []
        for div in dividends:
            amount_str = str(div.get('amount_text', '')) + str(div.get('unit', ''))
            results.append({
                "stock_name": stock_name, "stock_code": code,
                "year": div.get('year', ''),
                "amount_with_unit": amount_str,
                "metric": div.get('metric', 'dividend'),
                "raw_context": div.get('raw_text', ''),
                "filename": filename,
                "is_ai": div.get('is_ai', False),
                "ai_prompt": div.get('ai_prompt', ''),
                "ai_response": div.get('ai_response', ''),
                "ai_cost": div.get('ai_cost', 0.0)
            })
            
        return results, stock_info, cost

    except Exception as e:
        logger.error(f"Error processing {os.path.basename(file_path)}: {e}")
        return [], None, 0.0

_txt_manager_instance = None
def get_txt_manager():
    global _txt_manager_instance
    if _txt_manager_instance is None:
        _txt_manager_instance = TxtProcessManager()
    return _txt_manager_instance
