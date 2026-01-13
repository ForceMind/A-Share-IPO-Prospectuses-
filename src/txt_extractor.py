import re
import os
import time
import random
import json
import requests
import logging

class TxtExtractor:
    def __init__(self):
        pass

    def extract_from_file(self, file_path, api_key=None, cost_limit=0.0, current_cost=0.0, force_ai=False):
        """
        Extracts company financial information from a single TXT file.
        Returns dict with data and cost incurred.
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except UnicodeDecodeError:
            try:
                with open(file_path, 'r', encoding='gbk') as f:
                    content = f.read()
            except Exception as e:
                try:
                    # Final attempt with gb18030 (superset of gbk)
                    with open(file_path, 'r', encoding='gb18030') as f:
                        content = f.read()
                except Exception as final_e:
                    logging.error(f"读取文件失败 {file_path}: {final_e}")
                    return None

        # Extract company name from filename
        filename = os.path.basename(file_path)
        parts = filename.split('_')
        if len(parts) >= 1:
            company_name = parts[0]
        else:
            company_name = filename.replace(".txt", "")
        
        use_ai = bool(api_key)
        # Pass force_ai down
        financials, cost = self.extract_financials_enhanced(
            content, 
            use_ai=use_ai, 
            api_key=api_key,
            cost_limit=cost_limit,
            current_cost=current_cost,
            force_ai=force_ai
        )
        
        return {
            "company_name": company_name,
            "filename": filename,
            "dividends": financials, # Keeping key 'dividends' for compatibility, but contains all info
            "cost": cost
        }

    def extract_dividends(self, content):
        """
        Legacy wrapper.
        """
        d, c = self.extract_financials_enhanced(content)
        return d

    def extract_dividends_enhanced(self, content, use_ai=False, api_key=None, cost_limit=0.0, current_cost=0.0, force_ai=False):
        """
        Legacy wrapper mapped to new financial extraction.
        """
        return self.extract_financials_enhanced(content, use_ai, api_key, cost_limit, current_cost, force_ai)

    def extract_financials_enhanced(self, content, use_ai=False, api_key=None, cost_limit=0.0, current_cost=0.0, force_ai=False):
        """
        Enhanced extraction of financial information (Dividends, Net Profit, Cash Flow).
        Returns a tuple: (data_list, cost_incurred)
        """
        data_list = []
        cost_incurred = 0.0
        
        # Keywords for relevant paragraphs
        # Financials Keywords
        keywords = [
            "分红", "股利分配", "现金分红", "派发现金", "利润分配", "股利支付", 
            "权益分派", "分配方案", "每10股", "利益分配", 
            "归属于母公司所有者的净利润", "归母净利润", "净利润",
            "经营活动产生的现金流量净额", "经营现金净流", "现金流量净额"
        ]
        keyword_pattern = "|".join(keywords)
        
        # Split content into paragraphs or chunks
        chunks = re.split(r'\n\s*\n', content) 
        
        relevant_chunks = []
        for chunk in chunks:
            if re.search(keyword_pattern, chunk):
                clean_chunk = chunk.strip()
                if len(clean_chunk) > 10 and len(clean_chunk) < 3000: # Increased limit slightly for context
                    relevant_chunks.append(clean_chunk)
        
        if not relevant_chunks:
            # Fallback for splitting failure
            matches = re.finditer(f"(.{{0,200}})({keyword_pattern})(.{{0,300}})", content, re.DOTALL)
            for m in matches:
                relevant_chunks.append(m.group(0).strip())

        # Process chunks
        for chunk in relevant_chunks:
            extracted = []
            is_ai_used = False
            
            # 1. Try Regex First (Always try regex to have a baseline context)
            regex_results = self._extract_financials_with_regex(chunk)
            if regex_results:
                 logging.debug(f"正则提取到 {len(regex_results)} 候选条目 - 原文片段: {chunk[:20]}...")

            # 2. Decide AI usage
            # Use AI if:
            # - Force AI is ON (and check for extracting extracted text in prompt)
            # - OR (Use AI is ON AND (Regex failed or not perfect) AND Cost limit ok)
            
            should_use_ai = False
            ai_reason = ""
            
            if use_ai:
                if force_ai:
                    # If force AI is on, we process ALL relevant chunks with AI
                    should_use_ai = True
                    ai_reason = "强制AI模式开启"
                elif ((current_cost + cost_incurred) < cost_limit):
                     # If not forced, use heuristic: Use AI if regex didn't find good data
                     # What is "good data"? Let's say if we found nothing valid.
                     if not regex_results:
                         should_use_ai = True
                         ai_reason = "正则未提取到数据且费用额度充足"
            
            if should_use_ai:
                logging.info(f"调用 AI 提取 ({ai_reason})...")
                ai_results, cost, prompt_used, raw_resp = self._extract_with_ai(chunk, api_key)
                cost_incurred += cost
                
                if ai_results:
                    logging.info(f"AI 提取成功: 找到 {len(ai_results)} 条记录, 本次费用: ¥{cost:.4f}")
                    extracted = ai_results
                    is_ai_used = True
                    for item in extracted:
                        item['ai_prompt'] = prompt_used
                        item['ai_response'] = raw_resp
                        item['ai_cost'] = cost
                else:
                    logging.warning(f"AI 提取失败或返回空结果。")
                    # AI failed, fallback to regex
                    extracted = regex_results
                    if extracted:
                        for item in extracted:
                            item['ai_prompt'] = prompt_used
                            item['ai_response'] = raw_resp
                            item['ai_cost'] = cost
                            item['is_ai_fallback'] = True
            else:
                extracted = regex_results
                if not extracted and use_ai:
                     if (current_cost + cost_incurred) >= cost_limit:
                         logging.info("跳过 AI: 费用达到上限")

            if extracted:
                for item in extracted:
                    item['is_ai'] = is_ai_used
                    item['is_forced_ai'] = force_ai
                data_list.extend(extracted)

        # Deduplicate Logic
        merged_data = {} # Year -> Data Dict
        
        for d in data_list:
            year = d.get('year')
            if not year: continue
            
            if year not in merged_data:
                merged_data[year] = d
            else:
                # Merge fields if missing in current
                curr = merged_data[year]
                
                def update_field(field):
                    val = d.get(field)
                    if val and str(val).lower() != 'nan' and str(val).lower() != 'null' and str(val) != '' and str(val) != 'None':
                         if not curr.get(field) or str(curr.get(field)) == '':
                             curr[field] = val
                
                update_field('amount_text') # Dividend
                update_field('net_profit')
                update_field('operating_cash_flow')
                
                # Keep the one with AI if possible
                if d.get('is_ai') and not curr.get('is_ai'):
                    curr.update(d)
                
                # Combine raw texts for context
                if d.get('raw_text') and d['raw_text'] not in curr.get('raw_text', ''):
                    curr['raw_text'] = (curr.get('raw_text', '') + " || " + d['raw_text']).strip(" || ")

        return list(merged_data.values()), cost_incurred

    def _extract_financials_with_regex(self, text):
        """
        Regex extraction for Dividends, Net Profit, and Cash Flow.
        """
        results = []
        year_pattern = r"(20(?:1[7-9]|2[0-5]))年(?:度)?"
        years = re.findall(year_pattern, text)
        if not years:
            return []
        year = years[0]
        
        data = {"year": year, "raw_text": text, "unit": "万元"}
        
        # Helper Patterns
        amount_num = r"(-?\d{1,4}(?:,\d{3})*(?:\.\d+)?)"
        amount_unit = r"(?:万?元|亿元|亿)"

        # 1. Dividends
        total_keywords = r"(?:合计|共计|总额|总计|派发现金|现金分红)"
        p_div = re.compile(f"({total_keywords}[^0-9\n]{{0,50}}?{amount_num}\s*({amount_unit}))")
        m_div = p_div.findall(text)
        if m_div:
            val, unit = m_div[0][1], m_div[0][2]
            data['amount_text'] = self._normalize_amount(val, unit)

        # 2. Net Profit (归母净利润)
        p_np = re.compile(f"(归.*?净利润)[^0-9\n]{{0,30}}?{amount_num}\s*({amount_unit})")
        m_np = p_np.findall(text)
        if m_np:
            val, unit = m_np[0][1], m_np[0][2]
            data['net_profit'] = self._normalize_amount(val, unit)
        
        # 3. Operating Cash Flow (经营现金流)
        p_ocf = re.compile(f"(经营.*?现金流量净额)[^0-9\n]{{0,30}}?{amount_num}\s*({amount_unit})")
        m_ocf = p_ocf.findall(text)
        if m_ocf:
            val, unit = m_ocf[0][1], m_ocf[0][2]
            data['operating_cash_flow'] = self._normalize_amount(val, unit)

        if 'amount_text' in data or 'net_profit' in data or 'operating_cash_flow' in data:
            return [data]
        return []

    def _normalize_amount(self, val_str, unit):
        try:
            # Clean string
            clean_str = str(val_str).replace(',', '').replace(' ', '')
            val = float(clean_str)
            
            unit = str(unit).strip()
            if unit == '元':
                val = val / 10000.0
            elif unit in ['亿元', '亿']:
                val = val * 10000.0
            # Return string formatted, stripped of trailing zeros
            res = f"{val:.6f}".rstrip('0').rstrip('.')
            return res if res != '' else '0'
        except:
            return '0'

    def _extract_with_ai(self, text, api_key):
        time.sleep(random.uniform(0.5, 1.5))
        url = "https://api.deepseek.com/chat/completions"
        headers = { "Content-Type": "application/json", "Authorization": f"Bearer {api_key}" }
        
        prompt_content = f"""
        请从以下文本中提取公司财务信息。
        文本: "{text}"
        
        任务:
        1. 提取 **年份** (会计年度)。
        2. 提取 **现金分红总金额** (amount)。
        3. 提取 **归属于母公司所有者的净利润** (net_profit)。
        4. 提取 **经营活动产生的现金流量净额** (operating_cash_flow)。
        
        要求:
        - 统一将金额转换为 **万元**。如果是元除以10000，如果是亿元乘以10000。
        - **分红总金额**: 忽略每股金额，只取总额。若无总额但有每股及股本可估算，若不可估算则留空。
        - **金额格式**: 纯数字，不要千分位逗号。
        - **JSON格式**: 返回列表，如: [{{"year": "2020", "amount": "1000", "net_profit": "5000", "operating_cash_flow": "2000"}}]
        - 如果某项信息缺失，对应字段填 null 或空字符串 ""。
        - 仅返回 JSON 列表 array，不要包含 Markdown 格式 (如 ```json ... ```)。不要包含其他文字。
        """
        
        data = {
            "model": "deepseek-chat",
            "messages": [{"role": "user", "content": prompt_content}],
            "temperature": 0.1,
            "stream": False
        }
        
        cost = 0.0
        raw_response = ""
        prompt_used = prompt_content
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(data), timeout=30)
            if response.status_code == 200:
                result = response.json()
                
                usage = result.get('usage', {})
                prompt_tokens = usage.get('prompt_tokens', len(prompt_content))
                completion_tokens = usage.get('completion_tokens', 100)
                
                cost = (prompt_tokens / 1_000_000 * 2.0) + (completion_tokens / 1_000_000 * 3.0)
                
                content = result['choices'][0]['message']['content']
                raw_response = content
                
                # Cleanup output
                content_clean = content.replace("```json", "").replace("```", "").strip()
                
                try:
                    extracted_data = json.loads(content_clean)
                    # Normalize keys
                    for item in extracted_data:
                        item['amount_text'] = str(item.get('amount', '') or '')
                        item['net_profit'] = str(item.get('net_profit', '') or '')
                        item['operating_cash_flow'] = str(item.get('operating_cash_flow', '') or '')
                        item['unit'] = '万元'
                        item['raw_text'] = text
                        
                    return extracted_data, cost, prompt_used, raw_response
                except json.JSONDecodeError:
                    return [], cost, prompt_used, raw_response
            else:
                return [], 0.0, prompt_used, f"Error: {response.status_code} - {response.text}"
        except Exception as e:
            return [], 0.0, prompt_used, f"Exception: {str(e)}"

if __name__ == "__main__":
    pass
