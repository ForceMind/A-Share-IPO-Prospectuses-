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
        dividends, cost = self.extract_dividends_enhanced(
            content, 
            use_ai=use_ai, 
            api_key=api_key,
            cost_limit=cost_limit,
            current_cost=current_cost
        )
        
        return {
            "company_name": company_name,
            "filename": filename,
            "dividends": dividends,
            "cost": cost
        }
        
        return {
            "company_name": company_name,
            "filename": filename,
            "dividends": dividends
        }

    def extract_dividends(self, content):
        """
        Extracts dividend information using regex patterns.
        Focuses on sentences mentioning "分红", "股利", "利润分配" and amounts.
        """
        return self.extract_dividends_enhanced(content)

    def extract_dividends_enhanced(self, content, use_ai=False, api_key=None, cost_limit=0.0, current_cost=0.0):
        """
        Enhanced extraction of dividend information.
        Can optionally use DeepSeek API for better parsing of complex text.
        Returns a tuple: (dividends_list, cost_incurred)
        """
        dividends_data = []
        cost_incurred = 0.0
        
        # Keywords to locate relevant paragraphs
        keywords = ["分红", "股利分配", "现金分红", "派发现金", "利润分配", "股利支付"]
        keyword_pattern = "|".join(keywords)
        
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
            extracted = []
            is_ai_used = False
            
            # 1. Try Regex First
            regex_results = self._extract_with_regex_v2(chunk)
            
            # --- Normalize and Validate Regex Results ---
            is_good_quality = False
            if regex_results:
                for res in regex_results:
                    try:
                        val_str = res['amount_text'].replace(',', '')
                        val = float(val_str)
                        # Handle negative numbers (cash flow outflow) -> convert to absolute
                        val = abs(val)
                        
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
                        
                        # Quality Check:
                        # If value > 50 (500,000 RMB), we consider it a likely valid Total Amount.
                        # If value is small (e.g. 0.5), it might be a per-share amount that Regex wrongly picked up,
                        # or a very small dividend. We mark it as low quality to trigger AI verification.
                        if val > 50:
                            is_good_quality = True
                            
                    except Exception as e:
                        # If conversion fails, treat as low quality
                        pass
            
            # 3. Decide whether to use AI
            # Use AI if:
            # - AI is enabled
            # - Regex failed OR Regex result is "low quality" (small amount or parse error)
            # - Cost limit not reached
            
            should_use_ai = use_ai and (not is_good_quality) and ((current_cost + cost_incurred) < cost_limit)
            
            if should_use_ai:
                ai_results, cost, prompt_used, raw_resp = self._extract_with_ai(chunk, api_key)
                cost_incurred += cost
                
                if ai_results:
                    extracted = ai_results
                    is_ai_used = True
                    for item in extracted:
                        item['ai_prompt'] = prompt_used
                        item['ai_response'] = raw_resp
                        item['ai_cost'] = cost
                else:
                    # Fallback to regex if AI fails
                    extracted = regex_results
                    # Attach AI failure info to regex results if available
                    if extracted:
                        for item in extracted:
                            item['ai_prompt'] = prompt_used
                            item['ai_response'] = raw_resp
                            item['ai_cost'] = cost
                            item['is_ai_fallback'] = True
            else:
                extracted = regex_results

            if extracted:
                for item in extracted:
                    item['is_ai'] = is_ai_used
                dividends_data.extend(extracted)

        # Deduplicate based on year and amount
        unique_dividends = []
        seen = set()
        for d in dividends_data:
            try:
                amt = float(d['amount_text'].replace(',', ''))
            except:
                amt = d['amount_text']
            
            key = (d['year'], amt)
            if key not in seen:
                seen.add(key)
                unique_dividends.append(d)
                
        return unique_dividends, cost_incurred

    def _extract_with_regex_v2(self, text):
        """
        Improved Regex extraction prioritizing total amounts over per-share amounts.
        """
        results = []
        year_pattern = r"(20(?:1[7-9]|2[0-5]))年(?:度)?"
        
        # Find years
        years = re.findall(year_pattern, text)
        if not years:
            return []
        year = years[0] # Assume first year found is the context
        
        # Pattern 1: Explicit Total Amount (High Priority)
        # "合计派发...X万元", "共计...X万元", "派发现金...X万元"
        # We look for "Total" keywords near numbers
        total_keywords = r"(?:合计|共计|总额|总计|派发现金|现金分红)"
        # Allow negative numbers (e.g. cash flow outflow)
        amount_num = r"(-?\d{1,3}(?:,\d{3})*(?:\.\d+)?)"
        amount_unit = r"(?:万?元|亿元|亿)"
        
        # Search for: Keyword ... Number ... Unit
        # We want to capture the number that follows a "Total" keyword
        p_total = re.compile(f"({total_keywords}[^0-9]{{0,50}}?{amount_num}\s*({amount_unit}))")
        
        matches = p_total.findall(text)
        
        candidates = []
        for m in matches:
            full_str, val, unit = m
            candidates.append((val, unit, 10)) # Priority 10
            
        # Pattern 2: Fallback - Any number with "万元" or "亿元" (Medium Priority)
        # If we didn't find explicit "Total" keyword, but we see "3000万元", it's likely the total.
        if not candidates:
            p_large_unit = re.compile(f"({amount_num}\s*(万元|亿元|亿))")
            matches_large = p_large_unit.findall(text)
            for m in matches_large:
                full_str, val, unit = m
                candidates.append((val, unit, 5)) # Priority 5

        # Pattern 3: Fallback - Any large number > 10000 with "元" (Low Priority)
        if not candidates:
            p_yuan = re.compile(f"({amount_num}\s*(元))")
            matches_yuan = p_yuan.findall(text)
            for m in matches_yuan:
                full_str, val, unit = m
                try:
                    v_float = float(val.replace(',', ''))
                    if v_float > 10000:
                        candidates.append((val, unit, 1)) # Priority 1
                except:
                    pass

        # If we still have nothing, maybe it's just per share? 
        # User wants to avoid per share if possible, but if that's all we have...
        # Actually user said "You didn't extract correctly" implying we missed the total.
        # So we should NOT return per share if we are looking for total.
        
        if candidates:
            # Sort by priority
            candidates.sort(key=lambda x: x[2], reverse=True)
            best_val, best_unit, _ = candidates[0]
            
            results.append({
                "year": year,
                "amount_text": best_val,
                "unit": best_unit,
                "raw_text": text
            })
            
        return results

    def _extract_with_regex(self, text):
        # Deprecated, mapped to v2
        return self._extract_with_regex_v2(text)

    def _extract_with_ai(self, text, api_key):
        # Rate limiting: Sleep 0.5-1.5s to prevent hitting API limits across multiple processes
        time.sleep(random.uniform(0.5, 1.5))
        
        url = "https://api.deepseek.com/chat/completions"
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        
        prompt_content = f"""
        请从以下文本中提取公司分红信息。
        文本: "{text}"
        
        要求:
        1. 提取年份（会计年度）和分红总金额（数值和单位）。
        2. **只提取分红总金额**，忽略“每10股派发”或“每股派发”的单价金额。
        3. **统一将金额转换为“万元”**。如果原文是“元”，请除以10000；如果是“亿元”，请乘以10000。
        4. **如果金额为负数（如表示现金流出），请提取其绝对值**。
        5. 如果文本中包含多个年份的分红，请分别提取。
        6. 如果没有分红总金额信息，返回空列表。
        7. 返回格式必须为 JSON 列表，例如: [{{"year": "2020", "amount": "1000", "unit": "万元"}}]
        8. 只返回 JSON，不要包含其他文字。
        """
        
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
                    for item in extracted_data:
                        item['raw_text'] = text
                        item['amount_text'] = str(item.get('amount', ''))
                        if 'unit' not in item:
                            item['unit'] = ''
                    return extracted_data, cost, prompt_content, raw_response
                except json.JSONDecodeError:
                    return [], cost, prompt_content, raw_response
            else:
                return [], 0.0, prompt_content, f"Error: {response.status_code} - {response.text}"
        except Exception as e:
            return [], 0.0, prompt_content, f"Exception: {str(e)}"



if __name__ == "__main__":
    # Test on a file
    extractor = TxtExtractor()
    # Replace with a valid path for testing if needed, or rely on runner
    pass
