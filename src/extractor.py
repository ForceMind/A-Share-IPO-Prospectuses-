import pdfplumber
import re
import logging
import pandas as pd
import os

try:
    import pytesseract
    from PIL import Image
    # Help Tesseract find its path on Windows
    common_tesseract_paths = [
        r'C:\Program Files\Tesseract-OCR\tesseract.exe',
        r'C:\Users\wxx11\AppData\Local\Tesseract-OCR\tesseract.exe',
        r'D:\Program Files\Tesseract-OCR\tesseract.exe'
    ]
    for path in common_tesseract_paths:
        if os.path.exists(path):
            pytesseract.pytesseract.tesseract_cmd = path
            break
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
                    # Broad search as fallback: look for ANY page with "201" and "派发" or "股利"
                    fallback_pages = []
                    # Try last 100 pages first, then first 500
                    check_indices = list(range(len(pdf.pages)-1, max(0, len(pdf.pages)-100), -1)) + list(range(min(len(pdf.pages), 500)))
                    seen_fallback = set()
                    for i in check_indices:
                        if i in seen_fallback: continue
                        seen_fallback.add(i)
                        try:
                            page = pdf.pages[i]
                            text = page.extract_text()
                            if (not text or len(text.strip()) < 50) and HAS_OCR:
                                text = self._ocr_page(page)
                            if text and "201" in text and ("派发" in text or "股利" in text or "现金分红" in text):
                                fallback_pages.append(i)
                                if len(fallback_pages) >= 10: break
                        except: pass
                    
                    if fallback_pages:
                         target_pages = fallback_pages
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
                    # Capture table context
                    table_context = ""
                    if tables:
                        # Extract some text context from around tables or just use page text
                        table_context = page.extract_text() or ""
                    
                    data_from_table = self._process_tables(tables, page_num, table_context)
                    if data_from_table:
                        result.extend(data_from_table)
                        found_data = True
                    
                    # B. Text Extraction
                    text = page.extract_text()
                    extract_method = "Text"
                    
                    # C. OCR Fallback (ENABLED)
                    if (not text or len(text.strip()) < 50) and HAS_OCR:
                        logger.info(f"页面 {page_num + 1} 文本较少，尝试 OCR 识别...")
                        text = self._ocr_page(page)
                        extract_method = "OCR"

                    prev_text = ""
                    if idx > 0 and scan_list[idx-1] == page_num - 1:
                         try:
                             prev_text = pdf.pages[scan_list[idx-1]].extract_text()
                         except: pass

                    data_from_text = self._process_text(text, page_num, prev_text, extract_method)
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
                    # Final attempt: use empty check but if it's the last few pages, 
                    # we might still want to try OCR once
                    if i > end_page - 10 and HAS_OCR:
                         text = self._ocr_page(page)
                    else:
                         continue
                
                if not text: continue
                
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

    def _process_tables(self, tables, page_num, context_text=""):
        extracted_data = []
        if not tables:
            return []

        # Keywords that MUST be present in the row for it to be a valid dividend row
        # We need a stricter check because tables often have "Cash Flow" or "Assets" mixed in
        strict_keywords = ['分红', '股利', '利润分配', '现金分配']
        
        # Keywords that, if found in the row, might invalidate it as a "dividend" row 
        # unless strongly overridden (e.g., "Cash received" -> invalid)
        negative_keywords = ['收到', '流入', '流出', '支付', '筹资', '投资', '资产', '余额', '净额', '费用', '收入', '成本', '总额', '净利润', '未分配利润']

        for i, table in enumerate(tables):
            # Pre-filter table: must contain keywords to be relevant?
            table_str = str(table)
            table_content_str = table_str.replace('\n', '')
            
            # Use part of context if table is too long
            context_snippet = context_text[:2000] if context_text else table_content_str[:1000]

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
                        # Valid years: 2015-2024 (approx) - filter out future years or too old
                        valid_years = [y for y in matches if 2015 <= int(y) <= 2024]
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
                    
                    # STRICTER CHECK:
                    # 1. Must have at least one strict keyword (Dividend/Cash Dividend)
                    # 2. Must NOT have negative keywords (received, flow, assets) UNLESS explicitly "Cash Dividend" is there
                    
                    has_strict_kw = any(kw in row_text for kw in strict_keywords)
                    has_negative_kw = any(nkw in row_text for nkw in negative_keywords)
                    
                    # Special Case: "现金分红" is very strong, overrides negative keywords (rare but possible)
                    # But usually "支付其他与筹资活动有关的现金" contains "现金", so we must be careful.
                    # "派发现金股利" is safe. "现金分红" is safe.
                    
                    is_valid_row = False
                    if has_strict_kw:
                        is_valid_row = True
                        # Double check for false positives like "Cash received from dividend" (investing activity)
                        if '收到' in row_text or '流入' in row_text:
                            is_valid_row = False
                            
                    if not is_valid_row:
                        continue

                    for col_idx, year in header_year_map.items():
                        if col_idx < len(row):
                            cell_val = row[col_idx]
                            amount = self._parse_amount(cell_val)
                            
                            # Negative amount check inside parse_amount, but also check if cell string has '-'
                            if amount is not None and amount > 0:
                                # Double check: if cell text explicitly has brackets or minus sign for negative
                                if self._is_negative_value(cell_val):
                                    continue
                                
                                # If unit is 亿元, adjust
                                if global_unit > 1.0:
                                    amount = amount * global_unit
                                
                                if amount > 10: 
                                    extracted_data.append({
                                        'year': year,
                                        'amount': amount,
                                        'page': page_num + 1,
                                        'type': 'table_vertical',
                                        'method': 'Table',
                                        'context': context_snippet
                                    })
            
            # Strategy 2: Horizontal (Year in Row)
            for row in table:
                row_clean = [str(c).replace('\n', ' ').strip() if c else '' for c in row]
                row_text = ' '.join(row_clean)
                
                year_matches = self.year_pattern.findall(row_text)
                if not year_matches:
                    continue
                # Year validation
                years = sorted(list(set([y for y in year_matches if 2015 <= int(y) <= 2024])))
                if not years: continue
                
                year = years[0] # Take the first found year in the row

                # STRICTER CHECK for Horizontal Rows too
                has_strict_kw = any(kw in row_text for kw in strict_keywords)
                has_negative_kw = any(nkw in row_text for nkw in negative_keywords)
                
                if not has_strict_kw:
                     continue
                if '收到' in row_text or '流入' in row_text:
                     continue
                
                amounts = []
                for cell in row_clean:
                    # Enhanced extraction for mixed text cells
                    matches = re.findall(r'(\d{1,3}(,\d{3})*(\.\d+)?)\s*(万?元|亿元)', cell)
                    if matches:
                        for amt_str, _, _, unit in matches:
                            try:
                                # Check negative before parsing
                                if self._is_negative_value(cell):
                                    continue

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
                        if self._is_negative_value(cell): continue
                        
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
                        'type': 'table_horizontal_mixed',
                        'method': 'Table',
                        'context': row_text
                    })

        return extracted_data

    def _is_negative_value(self, cell_text):
        """Check if cell text implies a negative number (brackets or minus sign)"""
        if not cell_text: return False
        s = str(cell_text).strip()
        if s.startswith('-') or s.startswith('(') or s.startswith('（'):
            return True
        return False

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

    def _process_text(self, text, page_num, prev_text="", method="Text"):
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
            # STRICTER: Must NOT contain negative keywords like "Cash Flow", "Assets" unless "Dividend" is explicit
            if ('分红' in line or '派发' in line or '利润分配' in line) and self.year_pattern.search(line):
                # Filter out obvious false positives
                if any(bad in line for bad in ['现金流量', '资产总额', '净利润', '筹资', '投资', '流入', '流出']):
                    if '分红' not in line and '股利' not in line: # If no explicit dividend keyword, skip
                        continue

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
                            
                            # Year Check
                            if 2015 <= int(year[:4]) <= 2024:
                                if final_val > 1:
                                    results.append({
                                        'year': year,
                                        'amount': final_val,
                                        'page': page_num + 1,
                                        'type': 'text',
                                        'method': method,
                                        'context': line.strip()
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
                    if not (2015 <= int(year[:4]) <= 2024):
                        continue

                    # Look ahead up to 15 lines for amount keywords
                    context_paragraph = [clean_line]
                    for j in range(1, 16):
                        if i + j < len(lines):
                            next_line = lines[i + j]
                            context_paragraph.append(next_line.strip())
                            # Broaden keywords for descriptive paragraphs
                            if any(kw in next_line for kw in ['派发', '分红', '分配', '股利', '利润分配']):
                                # Reject negative context
                                if any(bad in next_line for bad in ['现金流量', '资产', '筹资', '投资']):
                                    continue
                                
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
                                                    'type': 'text_heading_paragraph',
                                                    'method': method,
                                                    'context': '\n'.join(context_paragraph)
                                                })
                                        except: pass
                                    break 
                                    
            # 3. Heuristic Pattern: "Cash Dividend ... 3200.00" without year in line, infer from context
            elif ('现金分红' in line or '分红金额' in line or '利润分配' in line or '股利分配' in line) and not self.year_pattern.search(line):
                # STRICTER: Must not contain negative keywords
                if any(bad in line for bad in ['现金流量', '资产', '筹资', '投资', '流入', '流出']):
                    continue
                
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
                    context_lines = []
                    # Look back more lines (up to 50) to catch distant headers
                    for offset in range(1, 50): 
                        if i - offset >= 0:
                            prev_line = lines[i - offset]
                            context_lines.insert(0, prev_line.strip())
                            yms = self.year_pattern.findall(prev_line)
                            if yms:
                                found_years = [y for y in yms if 2015 <= int(y) <= 2024]
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
                            'type': 'text_context_heuristic',
                            'method': method,
                            'context': '\n'.join(context_lines[-5:] + [line.strip()])
                        })

            # 4. Pattern: "Year ... Distribute ... Amount" (Long description)
            if ('分红' in line or '分配' in line) and ('万元' in line or '亿元' in line):
                 # Reject negative context
                 if any(bad in line for bad in ['现金流量', '资产', '筹资', '投资']):
                    continue

                 year_match = self.year_pattern.search(line)
                 if year_match:
                     year = year_match.group()
                     if not (2015 <= int(year[:4]) <= 2024):
                        continue

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
                                    'type': 'text_description',
                                    'method': method,
                                    'context': line.strip()
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
                if 'method' not in div: div['method'] = 'Unknown'
                if 'context' not in div: div['context'] = ''
                
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
                'method': 'N/A',
                'context': '',
                'note': '未提取到数据'
            })
            
        return pdf_file, cleaned_results, None
    except Exception as e:
        return pdf_file, [], str(e)
