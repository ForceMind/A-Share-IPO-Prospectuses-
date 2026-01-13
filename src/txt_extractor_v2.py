import re
import os
import time
import random
import json
import requests

class TxtExtractor:
    def __init__(self):
        pass

    def extract_from_file(self, file_path, api_key=None, cost_limit=0.0, current_cost=0.0):
        """
        Extracts company name and dividend information from a single TXT file.
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
                    print(f"Error reading file {file_path}: {final_e}")
                    return None

        # Extract company name from filename
        filename = os.path.basename(file_path)
        parts = filename.split('_')
        if len(parts) >= 1:
            company_name = parts[0]
        else:
            company_name = filename.replace(".txt", "")
        
        use_ai = bool(api_key)
        financial_data, cost = self.extract_financial_data(
            content, 
            use_ai=use_ai, 
            api_key=api_key,
            cost_limit=cost_limit,
            current_cost=current_cost
        )
        
        return {
            "company_name": company_name,
            "filename": filename,
            "dividends": financial_data, # Keeping key 'dividends' for compatibility, includes all metrics
            "cost": cost
        }

    def extract_dividends(self, content):
        """
        Extracts dividend information using regex patterns.
        Focuses on sentences mentioning "分红", "股利", "利润分配" and amounts.
        """
        return self.extract_financial_data(content)[0]

    def extract_dividends_enhanced(self, content, use_ai=False, api_key=None, cost_limit=0.0, current_cost=0.0):
        """
        Compatibility wrapper for extract_financial_data
        """
        return self.extract_financial_data(content, use_ai, api_key, cost_limit, current_cost)

    def extract_financial_data(self, content, use_ai=False, api_key=None, cost_limit=0.0, current_cost=0.0):
        """
        Enhanced extraction of financial information (Dividends, Net Profit, Operating Cash Flow).
        Can optionally use DeepSeek API for better parsing of complex text.
        Returns a tuple: (data_list, cost_incurred)
        """
        extracted_data = []
        cost_incurred = 0.0
        
        # Keywords
        keywords_map = {
            "dividend": ["分红", "股利分配", "现金分红", "派发现金", "利润分配", "股利支付"],
            "net_profit": ["归属于母公司所有者的净利润", "归属于母公司股东的净利润", "归母净利润", "净利润"],
            "operating_cash_flow": ["经营活动产生的现金流量净额", "经营现金流", "经营活动现金流"]
        }
        
        # Build one big regex for efficient chunk finding
        all_keywords = []
        for v in keywords_map.values():
            all_keywords.extend(v)
        keyword_pattern = "|".join(all_keywords)
        
        # Split content into paragraphs or chunks
        chunks = re.split(r'\n\s*\n', content) # Split by empty lines
        
        relevant_chunks = []
        for chunk in chunks:
            if re.search(keyword_pattern, chunk):
                clean_chunk = chunk.strip()
                if len(clean_chunk) > 10 and len(clean_chunk) < 2000:
                    relevant_chunks.append(clean_chunk)
        
        if not relevant_chunks:
            matches = re.finditer(f"(.{{0,200}})({keyword_pattern})(.{{0,200}})", content, re.DOTALL)
            for m in matches:
                relevant_chunks.append(m.group(0).strip())

        # Process chunks
        for chunk in relevant_chunks:
            chunk_results = []
            is_ai_used = False
            
            # 1. Try Regex First for all metrics
            regex_results = []
            regex_results.extend(self._extract_metric_regex(chunk, "dividend", keywords_map["dividend"]))
            regex_results.extend(self._extract_metric_regex(chunk, "net_profit", keywords_map["net_profit"]))
            regex_results.extend(self._extract_metric_regex(chunk, "operating_cash_flow", keywords_map["operating_cash_flow"]))

            # --- Normalize and Validate Regex Results ---
            found_metrics_quality = {} # metric -> bool (is_good)
            
            normalized_regex = []
            if regex_results:
                for res in regex_results:
                    try:
                        val_str = str(res['amount_text']).replace(',', '')
                        val = float(val_str)
                        
                        unit = res['unit']
                        
                        # Normalize to 万元 (Ten Thousand Yuan)
                        if unit == '元':
                            val = val / 10000.0
                            unit = '万元'
                        elif unit in ['亿元', '亿']:
                            val = val * 10000.0
                            unit = '万元'
                        
                        # Update the result with normalized value
                        res['amount_text'] = f"{val:.6f}".rstrip('0').rstrip('.')
                        res['unit'] = '万元'
                        res['amount_with_unit'] = f"{res['amount_text']}万元" # Ensure compatibility
                        
                        # Quality Check
                        if abs(val) > 10: 
                            found_metrics_quality[res['metric']] = True
                        
                        normalized_regex.append(res)
                    except Exception as e:
                        pass
            
            # 3. Decide whether to use AI
            missing_extraction = False
            for metric, kws in keywords_map.items():
                if any(kw in chunk for kw in kws):
                    # Keyword present, but no good regex?
                    if not found_metrics_quality.get(metric, False):
                        missing_extraction = True

            should_use_ai = use_ai and missing_extraction and ((current_cost + cost_incurred) < cost_limit)
            
            if should_use_ai:
                ai_results, cost, prompt_used, raw_resp = self._extract_with_ai(chunk, api_key)
                cost_incurred += cost
                
                if ai_results:
                    chunk_results = ai_results
                    is_ai_used = True
                    for item in chunk_results:
                        item['ai_prompt'] = prompt_used
                        item['ai_response'] = raw_resp
                        item['ai_cost'] = cost
                        if 'metric' not in item:
                            item['metric'] = 'dividend'
                        if 'amount_with_unit' not in item:
                            item['amount_with_unit'] = f"{item.get('amount_text', '0')}万元"
                else:
                    chunk_results = normalized_regex
                    if chunk_results:
                        for item in chunk_results:
                            item['ai_prompt'] = prompt_used
                            item['ai_response'] = raw_resp
                            item['ai_cost'] = cost
                            item['is_ai_fallback'] = True
            else:
                chunk_results = normalized_regex

            if chunk_results:
                for item in chunk_results:
                    item['is_ai'] = is_ai_used
                extracted_data.extend(chunk_results)

        # Deduplicate based on year, amount and metric
        unique_data = []
        seen = set()
        for d in extracted_data:
            try:
                amt = float(d['amount_text'].replace(',', ''))
            except:
                amt = d['amount_text']
            
            # Keep unique per year+metric+amount
            key = (d['year'], d.get('metric', 'dividend'), amt)
            if key not in seen:
                seen.add(key)
                unique_data.append(d)
                
        return unique_data, cost_incurred

    def _extract_metric_regex(self, text, metric_name, keywords):
        results = []
        year_pattern = r"(20(?:1[0-9]|2[0-5]))年(?:度)?"
        years = re.findall(year_pattern, text)
        if not years:
            return []
        year = years[0]
        
        kw_pattern_str = "|".join(keywords)
        # Allow negative numbers
        amount_num = r"(-?\d{1,3}(?:,\d{3})*(?:\.\d+)?)"
        amount_unit = r"(?:万?元|亿元|亿)"
        
        # 1. Total Amount Pattern: Keyword ... Value
        p_total = re.compile(f"({kw_pattern_str})[^0-9]{{0,50}}?{amount_num}\s*({amount_unit})")
        
        matches = p_total.findall(text)
        candidates = []
        for m in matches:
            kw, val, unit = m
            candidates.append((val, unit, 10))

        if candidates:
            candidates.sort(key=lambda x: x[2], reverse=True)
            best_val, best_unit, _ = candidates[0]
            results.append({
                "year": year,
                "amount_text": best_val,
                "unit": best_unit,
                "raw_text": text,
                "metric": metric_name
            })
        return results

    def _extract_with_regex_v2(self, text):
        # Deprecated
        return self._extract_metric_regex(text, "dividend", ["分红", "现金分红", "派发现金"])

    def _extract_with_ai(self, text, api_key):
        # Rate limiting: Sleep 0.5-1.5s to prevent hitting API limits across multiple processes
        time.sleep(random.uniform(0.5, 1.5))
        
        url = "https://api.deepseek.com/chat/completions"
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        
        prompt_content = f"请从以下文本中提取公司财务信息。文本: \"{text}\"\n\n要求:\n1. 提取年份（会计年度）和以下三项指标（数值和单位）：\n   - 分红总金额 (metric: \"dividend\")\n   - 归属于母公司所有者的净利润 (metric: \"net_profit\")\n   - 经营活动产生的现金流量净额 (metric: \"operating_cash_flow\")\n2. **只提取分红总金额**，忽略“每10股派发”或“每股派发”的单价金额。\n3. **统一将金额转换为“万元”**。\n4. 净利润和现金流**可能为负数**，请保留负号。分红总金额如果是负数（如表示流出），提取绝对值。\n5. 如果文本中包含多个年份的数据，请分别提取。\n6. 返回格式必须为 JSON 列表，例如: \n   [\n     {{\"year\": \"2020\", \"metric\": \"dividend\", \"amount\": \"1000\", \"unit\": \"万元\"}},\n     {{\"year\": \"2020\", \"metric\": \"net_profit\", \"amount\": \"2000\", \"unit\": \"万元\"}}\n   ]\n7. 只返回 JSON，不要包含其他文字。"
        
        data = {
            "model": "deepseek-chat",
            "messages": [{"role": "user", "content": prompt_content}],
            "temperature": 0.1,
            "stream": False
        }
        
        cost = 0.0
        raw_response = ""
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(data), timeout=30)
            if response.status_code == 200:
                result = response.json()
                
                # Calculate Cost (CNY)
                # Input: 2 CNY / 1M tokens
                # Output: 3 CNY / 1M tokens
                usage = result.get('usage', {})
                prompt_tokens = usage.get('prompt_tokens', len(prompt_content))
                completion_tokens = usage.get('completion_tokens', 100)
                
                cost = (prompt_tokens / 1_000_000 * 2.0) + (completion_tokens / 1_000_000 * 3.0)
                
                content = result['choices'][0]['message']['content']
                raw_response = content
                content = content.replace("```json", "").replace("```", "").strip()
                
                try:
                    extracted_data = json.loads(content)
                    normalized = []
                    for item in extracted_data:
                         if 'year' in item and 'amount' in item:
                            normalized.append({
                                "year": str(item['year']).replace('年',''),
                                "amount_text": str(item['amount']),
                                "unit": item.get('unit', '万元'),
                                "metric": item.get('metric', 'dividend'), 
                                "raw_text": "AI Generated",
                                "amount_with_unit": f"{item['amount']}万元"
                            })
                    return normalized, cost, prompt_content, raw_response
                except json.JSONDecodeError:
                    return [], cost, prompt_content, raw_response
            else:
                return [], 0.0, prompt_content, f"Error: {response.status_code} - {response.text}"
        except Exception as e:
            return [], 0.0, prompt_content, f"Exception: {str(e)}"
