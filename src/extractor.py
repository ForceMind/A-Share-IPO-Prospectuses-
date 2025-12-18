import pdfplumber
import re
import logging
import pandas as pd

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
            with pdfplumber.open(pdf_path) as pdf:
                # 1. Locate target pages
                target_pages = self._locate_target_pages(pdf)
                if not target_pages:
                    logger.warning(f"未定位到分红章节: {pdf_path}")
                    return []
                
                logger.info(f"定位到目标页面: {target_pages}")

                pages_to_scan = set()
                for p in target_pages:
                    # Scan current page + next 2 pages (reduced to avoid noise, but enough for tables)
                    for offset in range(3): 
                        if p + offset < len(pdf.pages):
                            pages_to_scan.add(p + offset)
                
                for page_num in sorted(list(pages_to_scan)):
                    page = pdf.pages[page_num]
                    
                    # A. Table Extraction
                    tables = page.extract_tables()
                    data_from_table = self._process_tables(tables, page_num)
                    if data_from_table:
                        result.extend(data_from_table)
                    
                    # B. Text Extraction (Fallback)
                    text = page.extract_text()
                    data_from_text = self._process_text(text, page_num)
                    if data_from_text:
                        result.extend(data_from_text)
                
                return self._clean_result(result)

        except Exception as e:
            logger.error(f"解析 PDF 失败 {pdf_path}: {e}")
            return []

    def _locate_target_pages(self, pdf):
        scores = {}
        total_pages = len(pdf.pages)
        # Scan from page 5 to 90%
        # Important: Many prospectus have "Major Financial Indicators" in pages 10-30
        start_page = 5 
        end_page = max(total_pages - 10, int(total_pages * 0.95))

        for i in range(start_page, end_page):
            try:
                page = pdf.pages[i]
                text = page.extract_text()
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
                    score -= 10 

                for cw in self.context_positive:
                    if cw in text:
                        score += 5
                
                for nw in self.context_negative:
                    if nw in text:
                        score -= 15
                
                lines = text.split('\n')
                for line in lines:
                    line = line.strip()
                    # Check for section titles like "九、股利分配情况"
                    if any(kw in line for kw in self.keywords):
                        if len(line) < 40 and (line.startswith('十') or line.startswith('九') or line.startswith('（') or line[0].isdigit()):
                            score += 25
                        if '......' in line: 
                            score -= 50 # TOC
                        if '政策' in line or '规划' in line or '原则' in line:
                            score -= 10

                if score > 15:
                    scores[i] = score

            except Exception:
                continue
        
        # Return top 5 candidates now, to catch both Summary and Detailed sections
        sorted_pages = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [p[0] for p in sorted_pages[:5]]

    def _process_tables(self, tables, page_num):
        extracted_data = []
        if not tables:
            return []

        for i, table in enumerate(tables):
            table_str = str(table)
            # Pre-filter table: must contain keywords to be relevant?
            # Not necessarily, sometimes the header is outside.
            # But usually it contains "分红" or "金额" or years.
            
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
                    
                    if '分红' in row_text or '股利' in row_text or '派发' in row_text:
                        for col_idx, year in header_year_map.items():
                            if col_idx < len(row):
                                cell_val = row[col_idx]
                                amount = self._parse_amount(cell_val)
                                if amount is not None:
                                    extracted_data.append({
                                        'year': year,
                                        'amount': amount,
                                        'page': page_num + 1,
                                        'type': 'table_vertical'
                                    })
            
            # Strategy 2: Horizontal (Year in Row)
            for row in table:
                row_clean = [str(c).replace('\n', '').strip() if c else '' for c in row]
                row_text = ' '.join(row_clean)
                
                year_matches = self.year_pattern.findall(row_text)
                if not year_matches:
                    continue
                years = sorted(list(set([y for y in year_matches if 2010 <= int(y) <= 2030])))
                if not years: continue
                
                year = years[0]

                # Row must contain keyword
                if not ('分红' in row_text or '股利' in row_text or '派发' in row_text):
                     continue
                
                amounts = []
                for cell in row_clean:
                    amt = self._parse_amount(cell)
                    if amt is not None:
                        # Ensure it's not the year
                        if 2010 <= amt <= 2030: continue
                        amounts.append(amt)
                
                if amounts:
                     val = max(amounts) # Best guess
                     extracted_data.append({
                        'year': year,
                        'amount': val,
                        'page': page_num + 1,
                        'type': 'table_horizontal'
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

    def _process_text(self, text, page_num):
        results = []
        if not text: return []
        
        lines = text.split('\n')
        for line in lines:
            if ('分红' in line or '派发' in line) and self.year_pattern.search(line):
                year = self.year_pattern.search(line).group()
                # Pattern: 1000.00万元
                matches = re.findall(r'(\d{1,3}(,\d{3})*(\.\d+)?)\s*(万?元)', line)
                if matches:
                    for m in matches:
                        amt_str = m[0].replace(',', '')
                        unit = m[3]
                        try:
                            val = float(amt_str)
                            if '万' not in unit: 
                                val = val / 10000
                            results.append({
                                'year': year,
                                'amount': val,
                                'page': page_num + 1,
                                'type': 'text'
                            })
                        except:
                            pass
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
