import pdfplumber
import re
import logging
import pandas as pd
import os

try:
    import pytesseract
    from PIL import Image
    HAS_OCR = True
except ImportError:
    HAS_OCR = False

logger = logging.getLogger(__name__)

class ProspectusExtractor:
    def __init__(self):
        self.keywords = ['股利分配', '现金分红', '利润分配']
        self.context_positive = ['每10股', '派发现金', '含税', '实施完毕', '分红金额', '现金分红', '报告期', '最近三年', '分配方案']
        self.context_negative = ['风险', '不确定性', '......', '目录', '详见', '参见', '分配政策', '分配原则', '章程', '规划', '未来']
        self.year_pattern = re.compile(r'(201[5-9]|202[0-9])') 
        self.amount_pattern = re.compile(r'(\d{1,3}(,\d{3})*(\.\d+)?)')
        
    def extract(self, pdf_path):
        result = []
        try:
            import os
            
            # Using pdfplumber to open the file.
            # Some PDFs might have restrictions. If extract_text fails for all pages, 
            # we might need to consider if it's a scanned PDF or has permissions issues.
            with pdfplumber.open(pdf_path) as pdf:
                # 0. Check if PDF is text-searchable
                has_text_content = False
                # Check first 20 pages or all pages if less
                check_pages = range(min(20, len(pdf.pages)))
                for i in check_pages:
                    if pdf.pages[i].extract_text():
                        has_text_content = True
                        break
                
                if not has_text_content:
                    logger.warning(f"文件似乎是纯图片/扫描件或有权限限制 (File: {os.path.basename(pdf_path)})")
                    # Even if no text, try to locate pages using image features or just return scanned_pdf
                    return [{'note': '扫描件/无法提取文本，需人工处理', 'status': 'scanned_pdf'}]

                # 1. Locate target pages
                logger.debug(f"Scanning {pdf_path} for dividend sections...")
                target_pages = self._locate_target_pages(pdf)
                
                if not target_pages:
                    logger.warning(f"未定位到分红章节: {pdf_path}")
                    # Broad search as fallback: look for ANY page with "201" and "派发"
                    fallback_pages = []
                    for i in range(min(len(pdf.pages), 500)): # Cap at 500 pages
                        try:
                            text = pdf.pages[i].extract_text()
                            if text and "201" in text and "派发" in text:
                                fallback_pages.append(i)
                        except: pass
                    
                    if fallback_pages:
                         target_pages = fallback_pages[:5]
                         logger.info(f"使用备选搜索逻辑定位到页面: {target_pages}")
                    else:
                        return [{'note': '可提取文本，但未找到分红章节关键字', 'status': 'no_section_found'}]
                
                logger.info(f"定位到目标页面: {target_pages} (File: {os.path.basename(pdf_path)})")

                pages_to_scan = set()
                for p in target_pages:
                    # Scan current page + next 2 pages (reduced to avoid noise, but enough for tables)
                    for offset in range(3): 
                        if p + offset < len(pdf.pages):
                            pages_to_scan.add(p + offset)
                
                scan_list = sorted(list(pages_to_scan))
                found_data = False
                
                # Keep track of previous page text/header logic if needed for cross-page tables
                last_page_header_years = []

                for idx, page_num in enumerate(scan_list):
                    logger.info(f"正在处理页面 {page_num + 1}/{len(pdf.pages)} ({idx + 1}/{len(scan_list)}) - {os.path.basename(pdf_path)}")
                    page = pdf.pages[page_num]
                    
                    # A. Table Extraction
                    tables = page.extract_tables()
                    data_from_table = self._process_tables(tables, page_num)
                    if data_from_table:
                        result.extend(data_from_table)
                        found_data = True
                    
                    # B. Text Extraction (Fallback)
                    text = page.extract_text()
                    
                    # C. OCR Fallback (DISABLED)
                    # ...

                    # Pass previous page info if needed? 
                    # For now, _process_text tries to look up many lines.
                    # We might need to pass the *previous page's text* to _process_text if we want to support cross-page headers.
                    prev_text = ""
                    if idx > 0 and scan_list[idx-1] == page_num - 1:
                         # If consecutive page, get its text
                         try:
                             prev_text = pdf.pages[scan_list[idx-1]].extract_text()
                         except: pass

                    data_from_text = self._process_text(text, page_num, prev_text)
                    if data_from_text:
                        result.extend(data_from_text)
                        found_data = True
                
                if result:
                    return self._clean_result(result)
                else:
                    return [{'note': '找到分红章节，但未提取到有效数字数据', 'status': 'section_found_no_data'}]

        except Exception as e:
            logger.error(f"解析 PDF 失败 {pdf_path}: {e}")
            return [{'note': f'解析出错: {str(e)}', 'status': 'error'}]

    def _ocr_page(self, page):
        """Perform OCR on a pdfplumber page object"""
        if not HAS_OCR:
            return ""
        try:
            # High resolution for better OCR
            # Some PDFs have very small text
            im = page.to_image(resolution=300)
            # Use custom config for Tesseract to handle financial numbers better
            # --oem 1 (LSTM), --psm 6 (Assume a single uniform block of text)
            custom_config = r'--oem 1 --psm 6'
            text = pytesseract.image_to_string(im.original, lang='chi_sim+eng', config=custom_config)
            return text
        except Exception as e:
            # Suppress noisy tesseract not found errors if we know it might fail
            if "tesseract is not installed" in str(e):
                logger.warning("OCR skipped: Tesseract not found.")
            else:
                logger.warning(f"OCR 失败: {e}")
            return ""

    def _locate_target_pages(self, pdf):
        scores = {}
        total_pages = len(pdf.pages)
        # Scan from page 5 to 98%
        start_page = 5 
        end_page = max(total_pages - 2, int(total_pages * 0.99))

        # Heuristic: Check if this looks like a scanned PDF
        is_scanned_pdf = False
        # Even if HAS_OCR is False, we check if text extraction works
        empty_pages_count = 0
        check_sample_indices = list(range(start_page, min(start_page + 10, end_page)))
        if check_sample_indices:
            for i in check_sample_indices:
                if not pdf.pages[i].extract_text():
                    empty_pages_count += 1
            if empty_pages_count / len(check_sample_indices) > 0.8:
                is_scanned_pdf = True
                if HAS_OCR:
                    logger.info("检测到可能是扫描版/纯图片PDF，启用OCR搜索模式...")
                else:
                    logger.warning("检测到可能是扫描版/纯图片PDF，但未启用OCR，可能无法定位章节")

        # In OCR mode, we increase step to avoid taking forever
        step = 5 if (is_scanned_pdf and HAS_OCR) else 1

        for i in range(start_page, end_page, step):
            try:
                page = pdf.pages[i]
                text = page.extract_text()
                
                # Fallback to OCR if text is missing and we suspect scanned PDF
                if (not text or len(text.strip()) < 10) and is_scanned_pdf and HAS_OCR:
                    text = self._ocr_page(page)

                if not text:
                    continue
                
                score = 0
                
                # Check for "Cash Dividend" keywords specifically for high score
                if '现金分红' in text:
                    score += 15
                
                for kw in self.keywords:
                    if kw in text:
                        score += 5
                
                if score == 0:
                    continue

                if self.year_pattern.search(text):
                    score += 10
                else:
                    score -= 5

                for cw in self.context_positive:
                    if cw in text:
                        score += 5
                
                for nw in self.context_negative:
                    if nw in text:
                        score -= 15
                
                lines = text.split('\n')
                for line in lines:
                    line = line.strip()
                    # Check for section titles
                    if any(kw in line for kw in self.keywords):
                        if len(line) < 60 and (
                            line.startswith('十') or 
                            line.startswith('九') or 
                            line.startswith('八') or 
                            line.startswith('七') or 
                            line.startswith('（') or 
                            line[0].isdigit() or
                            re.match(r'^[一二三四五六七八九十]、', line)
                        ):
                            score += 30
                        if '......' in line: 
                            score -= 50 # TOC
                        if '政策' in line or '规划' in line or '原则' in line:
                            score -= 5

                if score > 8:
                    scores[i] = score

            except Exception:
                continue
        
        # Return top 15 candidates
        sorted_pages = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [p[0] for p in sorted_pages[:15]]

    def _process_tables(self, tables, page_num):
        extracted_data = []
        if not tables:
            return []

        for i, table in enumerate(tables):
            # Pre-filter table: must contain keywords to be relevant?
            table_str = str(table)
            table_content_str = table_str.replace('\n', '')
            
            # Helper to detect global unit in table
            global_unit = 1.0
            if '万元' in table_content_str:
                global_unit = 1.0
            elif '亿元' in table_content_str:
                global_unit = 10000.0

            # Strategy 1: Vertical (Header has Years)
            header_year_map = {} 
            header_row_idx = -1
            
            for r_idx, row in enumerate(table):
                current_row_years = {}
                for c_idx, cell in enumerate(row):
                    if not cell: continue
                    cell_str = str(cell).replace('\n', '')
                    matches = self.year_pattern.findall(cell_str)
                    if matches:
                        valid_years = [y for y in matches if 2010 <= int(y) <= 2030]
                        if valid_years:
                            # Use the last valid year found in the cell (e.g. 2022.12.31/2022年度 -> 2022)
                            current_row_years[c_idx] = valid_years[-1]
                
                if len(current_row_years) >= 1:
                    header_year_map = current_row_years
                    header_row_idx = r_idx
                    break 
            
            if header_year_map:
                for r_idx in range(header_row_idx + 1, len(table)):
                    row = table[r_idx]
                    row_text = ''.join([str(c) for c in row if c])
                    
                    # Relaxed keywords check for the row
                    if any(kw in row_text for kw in ['分红', '股利', '派发', '现金', '利润分配']):
                        for col_idx, year in header_year_map.items():
                            if col_idx < len(row):
                                cell_val = row[col_idx]
                                amount = self._parse_amount(cell_val)
                                if amount is not None:
                                    # If unit is 亿元, adjust
                                    if global_unit > 1.0:
                                        amount = amount * global_unit
                                    
                                    if amount > 10: 
                                        extracted_data.append({
                                            'year': year,
                                            'amount': amount,
                                            'page': page_num + 1,
                                            'type': 'table_vertical'
                                        })
            
            # Strategy 2: Horizontal (Year in Row)
            for row in table:
                row_clean = [str(c).replace('\n', ' ').strip() if c else '' for c in row]
                row_text = ' '.join(row_clean)
                
                year_matches = self.year_pattern.findall(row_text)
                if not year_matches:
                    continue
                years = sorted(list(set([y for y in year_matches if 2010 <= int(y) <= 2030])))
                if not years: continue
                
                year = years[0] # Take the first found year in the row

                # Check keywords again
                has_keyword = False
                if any(kw in row_text for kw in ['分红', '股利', '派发', '现金', '利润分配']):
                    has_keyword = True
                
                if not has_keyword:
                     continue
                
                amounts = []
                for cell in row_clean:
                    # Enhanced extraction for mixed text cells
                    matches = re.findall(r'(\d{1,3}(,\d{3})*(\.\d+)?)\s*(万?元|亿元)', cell)
                    if matches:
                        for amt_str, _, _, unit in matches:
                            try:
                                val = float(amt_str.replace(',', ''))
                                final_val = 0
                                if '亿' in unit:
                                    final_val = val * 10000
                                elif '万' in unit:
                                    final_val = val
                                else:
                                    final_val = val / 10000
                                
                                if 2010 <= final_val <= 2030 and abs(final_val - float(year)) < 0.1:
                                    continue
                                    
                                if final_val > 0:
                                    amounts.append(final_val)
                            except: pass
                    
                    # Fallback: Parse number without explicit unit in cell
                    if not matches:
                        amt = self._parse_amount(cell)
                        if amt is not None:
                            if 2010 <= amt <= 2030: continue
                            
                            # Apply global unit if detected
                            if global_unit > 1.0:
                                amt = amt * global_unit
                                
                            amounts.append(amt)
                
                if amounts:
                     val = max(amounts) # Best guess
                     extracted_data.append({
                        'year': year,
                        'amount': val,
                        'page': page_num + 1,
                        'type': 'table_horizontal_mixed'
                    })

        return extracted_data

    def _parse_amount(self, cell_text):
        if not cell_text: return None
        text = str(cell_text).replace('\n', '').replace(' ', '')
        
        if text in ['-', '/', '—', 'None', 'N/A']:
            return 0.0
            
        clean_text = text.replace(',', '').replace('万元', '').replace('元', '').replace('%', '')
        
        try:
            val = float(clean_text)
            if val < 0: return 0.0
            
            is_yuan = '元' in str(cell_text) and '万元' not in str(cell_text)
            
            # 200,000 threshold
            if is_yuan or val > 200000: 
                val = val / 10000
                
            return val
        except ValueError:
            return None

    def _process_text(self, text, page_num, prev_text=""):
        results = []
        full_text = text
        
        # Combine with previous page text to handle cross-page headers
        if prev_text:
            prev_lines = prev_text.split('\n')[-20:]
            full_text = '\n'.join(prev_lines) + '\n' + text
            
        if not full_text: return []
        
        lines = full_text.split('\n')
        
        for i, line in enumerate(lines):
            # 1. Standard Pattern: Same line has Year and Amount
            if ('分红' in line or '派发' in line or '利润分配' in line) and self.year_pattern.search(line):
                year = self.year_pattern.search(line).group()
                # Pattern: 1000.00万元
                matches = re.findall(r'(\d{1,3}(,\d{3})*(\.\d+)?)\s*(万?元|亿元)', line)
                if matches:
                    for m in matches:
                        amt_str = m[0].replace(',', '')
                        unit = m[3]
                        try:
                            val = float(amt_str.replace(',', ''))
                            final_val = val
                            if '亿' in unit:
                                final_val = val * 10000
                            elif '万' not in unit: 
                                final_val = val / 10000
                            
                            if final_val > 1:
                                results.append({
                                    'year': year,
                                    'amount': final_val,
                                    'page': page_num + 1,
                                    'type': 'text'
                                })
                        except:
                            pass
            
            # 2. Paragraph/Heading Pattern: Year is in a separate line (header), amount is in the paragraph below
            # Example: 002962
            # （二）2017年
            # ... 派发现金股利14,000.00万元 ...
            # Look for line that is JUST a year or year with small prefix
            clean_line = line.strip()
            # Relax length and pattern to catch headers like "（二）2017年"
            if ('201' in clean_line or '202' in clean_line) and len(clean_line) < 30:
                year_match = self.year_pattern.search(clean_line)
                if year_match:
                    year = year_match.group()
                    # Look ahead up to 15 lines for amount keywords
                    for j in range(1, 16):
                        if i + j < len(lines):
                            next_line = lines[i + j]
                            # Broaden keywords for descriptive paragraphs
                            if any(kw in next_line for kw in ['派发', '分红', '分配', '股利', '利润分配']):
                                amt_matches = re.findall(r'(\d{1,3}(,\d{3})*(\.\d+)?)\s*(万?元|亿元)', next_line)
                                if amt_matches:
                                    for m in amt_matches:
                                        try:
                                            amt_val = float(m[0].replace(',', ''))
                                            unit = m[3]
                                            if '亿' in unit: amt_val *= 10000
                                            elif '万' not in unit: amt_val /= 10000
                                            
                                            if amt_val > 1:
                                                results.append({
                                                    'year': year,
                                                    'amount': amt_val,
                                                    'page': page_num + 1,
                                                    'type': 'text_heading_paragraph'
                                                })
                                        except: pass
                                    break 

            # 3. Heuristic Pattern: "Cash Dividend ... 3200.00" without year in line, infer from context
            elif ('现金分红' in line or '分红金额' in line or '利润分配' in line or '股利分配' in line) and not self.year_pattern.search(line):
                # Check for amount with optional unit
                matches = re.findall(r'(\d{1,3}(,\d{3})*(\.\d+)?)\s*(万?元|亿元)?', line)
                valid_matches = []
                for m in matches:
                    try:
                        val = float(m[0].replace(',', ''))
                        # Heuristic: if it's a raw number > 100, might be an amount
                        if val > 100: 
                            valid_matches.append(val)
                    except: pass
                
                if valid_matches:
                    amount = max(valid_matches)
                    
                    found_years = [] 
                    # Look back more lines (up to 50) to catch distant headers
                    for offset in range(1, 50): 
                        if i - offset >= 0:
                            prev_line = lines[i - offset]
                            yms = self.year_pattern.findall(prev_line)
                            if yms:
                                found_years = [y for y in yms if 2010 <= int(y) <= 2030]
                                if found_years:
                                    break
                    
                    if found_years:
                        # Normalize amount
                        final_amt = amount
                        # If unit is missing, check context
                        if '亿元' in line:
                            final_amt = final_amt * 10000
                        elif '万元' in line:
                             pass
                        elif final_amt > 200000:
                            final_amt = final_amt / 10000
                        elif 100 < final_amt < 100000:
                            # Assume it's already in 万元 if it's in this range and no unit
                            pass 

                        results.append({
                            'year': found_years[0],
                            'amount': final_amt,
                            'page': page_num + 1,
                            'type': 'text_context_heuristic'
                        })

            # 4. Pattern: "Year ... Distribute ... Amount" (Long description)
            if ('分红' in line or '分配' in line) and ('万元' in line or '亿元' in line):
                 year_match = self.year_pattern.search(line)
                 if year_match:
                     year = year_match.group()
                     amount_matches = re.findall(r'(\d{1,3}(,\d{3})*(\.\d+)?)\s*(万?元|亿元)', line)
                     for amt_str, _, _, unit in amount_matches:
                         try:
                             val = float(amt_str.replace(',', ''))
                             final_val = 0
                             if '亿' in unit:
                                 final_val = val * 10000
                             elif '万' in unit:
                                 final_val = val
                             else:
                                 final_val = val / 10000
                             
                             if final_val > 10:
                                 results.append({
                                    'year': year,
                                    'amount': final_val,
                                    'page': page_num + 1,
                                    'type': 'text_description'
                                })
                         except: pass

        return results

    def _clean_result(self, raw_results):
        final_data = {}
        for item in raw_results:
            year = item['year']
            if '年' in year:
                year = year.split('年')[0]
            
            amount = item['amount']
            
            # Ignore tiny
            if 0 < amount < 1: 
                continue

            if year in final_data:
                if final_data[year]['amount'] == 0 and amount > 0:
                    final_data[year] = item
                elif amount > final_data[year]['amount']:
                     if amount < 2000000: # Reasonable cap
                        final_data[year] = item
            else:
                final_data[year] = item

        return sorted(final_data.values(), key=lambda x: x['year'], reverse=True)

def process_pdf_worker(pdf_file, pdf_dir, log_queue=None):
    """
    Worker function for multiprocessing.
    Instantiates its own extractor to avoid pickling issues and ensure thread/process safety.
    """
    try:
        import os
        
        # Configure logging if log_queue is provided
        if log_queue:
            logger = logging.getLogger()
            # Clear default handlers to avoid duplicates or lost logs
            # Check if QueueHandler is already attached (though usually it's clean in new process)
            is_configured = any(h.__class__.__name__ == 'QueueHandler' for h in logger.handlers)
            if not is_configured:
                logger.handlers = []
                
                # Define simple QueueHandler locally to avoid import issues or dependency
                class QueueHandler(logging.Handler):
                    def __init__(self, q):
                        super().__init__()
                        self.q = q
                    def emit(self, record):
                        try:
                            self.q.put_nowait(record)
                        except Exception:
                            self.handleError(record)
                            
                logger.addHandler(QueueHandler(log_queue))
                logger.setLevel(logging.INFO)

        # Log startup with PID
        logger = logging.getLogger()
        logger.info(f"Worker process started [PID: {os.getpid()}] processing: {pdf_file}")

        pdf_path = os.path.join(pdf_dir, pdf_file)
        
        # Initialize extractor inside the process
        extractor = ProspectusExtractor()
        dividends = extractor.extract(pdf_path)
        
        stock_code = pdf_file.split('_')[0]
        stock_name = pdf_file.split('_')[1].replace('.pdf', '') if '_' in pdf_file else 'Unknown'
        
        cleaned_results = []
        if dividends:
            for div in dividends:
                # If dividend entry has a 'status' or 'note', propagate it
                div['code'] = stock_code
                div['name'] = stock_name
                div['source_file'] = pdf_file
                
                # Ensure fields exist even if it's an error/note object
                if 'year' not in div: div['year'] = 'N/A'
                if 'amount' not in div: div['amount'] = 0
                if 'page' not in div: div['page'] = 'N/A'
                
                cleaned_results.append(div)
        else:
            # Fallback for empty list (should be covered by extract returning list of 1 dict now)
            cleaned_results.append({
                'code': stock_code,
                'name': stock_name,
                'year': 'N/A',
                'amount': 0,
                'page': 'N/A',
                'source_file': pdf_file,
                'note': '未提取到数据'
            })
            
        return pdf_file, cleaned_results, None
    except Exception as e:
        return pdf_file, [], str(e)
