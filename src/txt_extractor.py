import re
import os

class TxtExtractor:
    def __init__(self):
        pass

    def extract_from_file(self, file_path, api_key=None):
        """
        Extracts company name and dividend information from a single TXT file.
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
        # Expected formats:
        # "CompanyName_DocType_Date.txt"
        # "CompanyName_DocType_Date_Date.txt"
        # "CompanyName_DocType.txt"
        
        # Strategy:
        # 1. Split by underscore
        # 2. Take the first part as company name candidate
        # 3. Clean up any trailing stuff if needed (though usually first part is clean)
        
        parts = filename.split('_')
        if len(parts) >= 1:
            company_name = parts[0]
        else:
            company_name = filename.replace(".txt", "")
        
        # Remove common non-company prefixes/suffixes if they got attached (rare in this dataset)
        # e.g. "S*ST" prefix handling if needed, but for now exact match is better.
        
        use_ai = bool(api_key)
        dividends = self.extract_dividends_enhanced(content, use_ai=use_ai, api_key=api_key)
        
        return {
            "company_name": company_name,
            "filename": filename,
            "dividends": dividends
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

    def extract_dividends_enhanced(self, content, use_ai=False, api_key=None):
        """
        Enhanced extraction of dividend information.
        Can optionally use DeepSeek API for better parsing of complex text.
        """
        dividends_data = []
        
        # Keywords to locate relevant paragraphs
        keywords = ["分红", "股利分配", "现金分红", "派发现金", "利润分配", "股利支付"]
        keyword_pattern = "|".join(keywords)
        
        # Split content into paragraphs or chunks
        # A simple split by newline might be too granular, maybe split by double newline or indent
        # For now, let's try to find sentences/paragraphs containing keywords.
        
        # We'll look for a window around the keyword.
        # Or better, split by common delimiters and check each chunk.
        chunks = re.split(r'\n\s*\n', content) # Split by empty lines
        
        relevant_chunks = []
        for chunk in chunks:
            if re.search(keyword_pattern, chunk):
                # Clean up chunk
                clean_chunk = chunk.strip()
                if len(clean_chunk) > 10 and len(clean_chunk) < 2000: # Reasonable length
                    relevant_chunks.append(clean_chunk)
        
        # If no paragraphs found with simple split, try a sliding window or regex search
        if not relevant_chunks:
            # Fallback to regex finding context around keywords
            matches = re.finditer(f"(.{{0,200}})({keyword_pattern})(.{{0,200}})", content, re.DOTALL)
            for m in matches:
                relevant_chunks.append(m.group(0).strip())

        # Process chunks
        for chunk in relevant_chunks:
            extracted = []
            if use_ai and api_key:
                extracted = self._extract_with_ai(chunk, api_key)
            else:
                extracted = self._extract_with_regex(chunk)
            
            if extracted:
                dividends_data.extend(extracted)

        # Deduplicate based on year and amount
        unique_dividends = []
        seen = set()
        for d in dividends_data:
            # Normalize amount for dedup
            try:
                amt = float(d['amount_text'].replace(',', ''))
            except:
                amt = d['amount_text']
            
            key = (d['year'], amt)
            if key not in seen:
                seen.add(key)
                unique_dividends.append(d)
                
        return unique_dividends

    def _extract_with_regex(self, text):
        results = []
        # Years: 2017-2025
        year_pattern = r"(20(?:1[7-9]|2[0-5]))年(?:度)?"
        # Amount: 1,000.00 or 1000 or 10.5
        amount_num_pattern = r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?)"
        amount_unit_pattern = r"(?:万?元|亿元|亿)"
        
        years = re.findall(year_pattern, text)
        if not years:
            return []
            
        # Simple heuristic: if we find a year and an amount in the same chunk, associate them.
        # This is less precise than the previous strict regex but works on smaller chunks.
        
        amounts = re.finditer(f"{amount_num_pattern}\s*({amount_unit_pattern})", text)
        
        found_amounts = []
        for m in amounts:
            val = m.group(1)
            unit = m.group(2)
            # Filter years captured as amounts
            try:
                val_float = float(val.replace(",", ""))
                if unit == "元" and 2000 <= val_float <= 2100 and val_float.is_integer():
                    continue
            except:
                pass
            found_amounts.append((val, unit))
            
        if found_amounts:
            # If multiple years, it's ambiguous which amount belongs to which year without NLP.
            # We'll take the most recent year mentioned in the text as the primary candidate 
            # or create entries for all pairs if it looks like a table row.
            # For safety in regex mode, we might just take the first year and first amount if close.
            
            # Let's try to be slightly smarter: find the year closest to the amount?
            # For now, just return all combinations found in this relevant chunk? No, that creates noise.
            # Let's return the first year and first amount found.
            
            year = years[0] # Take the first year found
            amount_val, unit = found_amounts[0]
            
            results.append({
                "year": year,
                "amount_text": amount_val,
                "unit": unit,
                "raw_text": text
            })
            
        return results

    def _extract_with_ai(self, text, api_key):
        import requests
        import json
        
        url = "https://api.deepseek.com/v1/chat/completions" # Standard DeepSeek API endpoint
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        
        prompt = f"""
        请从以下文本中提取公司分红信息。
        文本: "{text}"
        
        要求:
        1. 提取年份（会计年度）和分红金额（数值和单位）。
        2. 如果文本中包含多个年份的分红，请分别提取。
        3. 如果没有分红信息，返回空列表。
        4. 返回格式必须为 JSON 列表，例如: [{{"year": "2020", "amount": "1000", "unit": "万元"}}]
        5. 只返回 JSON，不要包含其他文字。
        """
        
        data = {
            "model": "deepseek-chat",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1
        }
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(data), timeout=10)
            if response.status_code == 200:
                result = response.json()
                content = result['choices'][0]['message']['content']
                # Clean up markdown code blocks if present
                content = content.replace("```json", "").replace("```", "").strip()
                try:
                    extracted_data = json.loads(content)
                    # Add raw text to each entry
                    for item in extracted_data:
                        item['raw_text'] = text
                        item['amount_text'] = str(item.get('amount', ''))
                        # Ensure keys match what we expect
                        if 'unit' not in item:
                            item['unit'] = ''
                    return extracted_data
                except json.JSONDecodeError:
                    print(f"AI returned invalid JSON: {content}")
                    return []
            else:
                print(f"AI Request failed: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            print(f"AI Extraction error: {e}")
            return []


if __name__ == "__main__":
    # Test on a file
    extractor = TxtExtractor()
    # Replace with a valid path for testing if needed, or rely on runner
    pass
