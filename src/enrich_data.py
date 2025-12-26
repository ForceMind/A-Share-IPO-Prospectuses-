import pandas as pd
import requests
import time
import logging
import os
import re

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
CNINFO_SEARCH_URL = "http://www.cninfo.com.cn/new/information/topSearch/query"
# EastMoney might be harder to scrape without specific API, sticking to Cninfo which usually has a public search endpoint.
# Actually Cninfo TopSearch is good.

def search_stock_cninfo(keyword):
    """
    Search for stock info on Cninfo using a keyword (company name or code).
    Returns a dict with 'code', 'orgId', 'category', 'pinyin', 'zwjc' (short name).
    """
    if not keyword or len(keyword) < 2:
        return None
        
    try:
        # Cninfo search API
        # params: keyWord=...&maxNum=10
        payload = {
            'keyWord': keyword,
            'maxNum': 5
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'http://www.cninfo.com.cn/new/index'
        }
        
        response = requests.post(CNINFO_SEARCH_URL, data=payload, headers=headers, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, list) and len(data) > 0:
                # Return the best match. 
                # Usually the first one is best.
                best_match = data[0]
                return {
                    'code': best_match.get('code'),
                    'name': best_match.get('zwjc'),
                    'orgId': best_match.get('orgId')
                }
    except Exception as e:
        logger.error(f"Error searching Cninfo for '{keyword}': {e}")
        
    return None

def clean_filename_garbage(text):
    """
    Attempts to fix common encoding mojibake if possible, or just strips known bad chars.
    Actually, if it's already '????', it's lost.
    If it's 'ÐÂÀû', it might be recoverable.
    """
    # Simple heuristic: if it looks like UTF-8 bytes decoded as Latin1
    try:
        # Try to encode latin1 then decode gbk (common in Windows zip issues)
        fixed = text.encode('latin1').decode('gbk')
        if len(fixed) < len(text): # usually CJK is shorter than mojibake
            return fixed
    except:
        pass
    return text

def get_search_candidates(raw_name):
    """
    Generate a list of search keywords from a company name, 
    starting from the most specific to more generic.
    """
    candidates = []
    
    # 1. Original
    candidates.append(raw_name)
    
    # 2. Strip standard legal suffixes
    # Order matters: strip longer ones first
    legal_suffixes = ['股份有限公司', '有限责任公司', '有限公司', '集团']
    name_no_suffix = raw_name
    for suffix in legal_suffixes:
        name_no_suffix = name_no_suffix.replace(suffix, '')
    
    if name_no_suffix != raw_name:
        candidates.append(name_no_suffix)
        
    # 3. Strip City/Province prefixes from the no_suffix version
    # (Only if the remaining part is still substantial, e.g., > 2 chars)
    geo_prefixes = [
        '上海', '北京', '深圳', '广东', '江苏', '浙江', '山东', '四川', 
        '湖南', '湖北', '福建', '安徽', '吉林', '辽宁', '黑龙江', '广西', 
        '云南', '贵州', '陕西', '甘肃', '宁夏', '新疆', '西藏', '青海', 
        '内蒙古', '天津', '重庆', '海南', '河北', '河南', '山西', '江西'
    ]
    
    name_no_geo = name_no_suffix
    for prefix in geo_prefixes:
        if name_no_geo.startswith(prefix):
            name_no_geo = name_no_geo[len(prefix):]
            break # Assume only one prefix
            
    if name_no_geo != name_no_suffix and len(name_no_geo) >= 2:
        candidates.append(name_no_geo)
        
    # 4. Handle specific large group prefixes that confuse search
    # e.g., "中船重工汉光..." -> "汉光..."
    group_prefixes = ["中船重工", "中国船舶", "中国", "中船", "中星"]
    name_no_group = name_no_suffix
    for prefix in group_prefixes:
        if name_no_group.startswith(prefix):
             stripped = name_no_group[len(prefix):]
             if len(stripped) >= 2:
                 # Prefer adding this as a candidate
                 candidates.append(stripped)
             # Don't break immediately, maybe multiple prefixes? 
             # Actually for now just taking the first match is safer to avoid over-stripping.
             break
    
    # 5. Fallback: try taking the first 2-4 chars if long enough?
    # Or maybe "中船汉光" manual combination for that specific case if "汉光科技" fails?
    # Let's add specific known problematic mappings if generic rules fail.
    # This is a bit hacky but effective for stubborn cases.
    manual_mappings = {
        "中船重工汉光科技": ["中船汉光", "汉光科技"],
        "中星技术": ["中星"],
    }
    
    for key, vals in manual_mappings.items():
        if key in raw_name:
            candidates.extend(vals)

    # Deduplicate and preserve order
    seen = set()
    final_candidates = []
    for c in candidates:
        if c and c not in seen and len(c) > 1:
            seen.add(c)
            final_candidates.append(c)
            
    return final_candidates

def enrich_data(file_path):
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return

    logger.info(f"Processing {file_path}...")
    
    # Load Excel with openpyxl to save formatting if needed, but pandas is easier for data manip
    try:
        xls = pd.ExcelFile(file_path)
        sheet_map = {}
        for sheet_name in xls.sheet_names:
            sheet_map[sheet_name] = pd.read_excel(xls, sheet_name=sheet_name)
            
    except Exception as e:
        logger.error(f"Failed to read Excel: {e}")
        return

    # Process 'Stock List' sheet if exists
    if 'Stock List' in sheet_map:
        df = sheet_map['Stock List']
        updated_count = 0
        
        for index, row in df.iterrows():
            stock_code = str(row.get('Stock Code', ''))
            stock_name = str(row.get('Stock Name', ''))
            full_name = str(row.get('Full Company Name', ''))
            filename = str(row.get('Source File', ''))
            
            # Check if we need to enrich
            needs_enrich = False
            # Check if code/name is missing OR literally 'Unknown'
            if (pd.isna(row.get('Stock Code')) or 
                not stock_code.strip() or 
                'Unknown' in stock_code or 
                'Unknown' in stock_name):
                needs_enrich = True
                
            if not needs_enrich:
                continue
                
            base_query = None
            
            # 1. Try to extract from Full Name if reasonable
            if full_name and 'Unknown' not in full_name and len(full_name) > 1:
                base_query = full_name
            # 2. Try to clean filename and use it
            elif filename:
                # Remove extension and date patterns
                clean_name = re.sub(r'_\d{4}-\d{2}-\d{2}.*', '', filename) # Remove date suffix
                clean_name = clean_name.replace('.txt', '')
                
                # Attempt encoding fix
                clean_name = clean_filename_garbage(clean_name)
                
                # Split underscores and take first part (usually the name or code)
                # But sometimes filename is Code_Name. Check if part 0 is digit.
                parts = clean_name.split('_')
                if parts:
                    if parts[0].isdigit() and len(parts) > 1:
                        # If filename starts with code, we can actually just use that code!
                        # But here we want to find the Name to verify.
                        # Let's assume part 1 is name.
                        base_query = parts[1]
                    else:
                        base_query = parts[0]
            
            if base_query:
                # Generate candidates
                candidates = get_search_candidates(base_query)
                
                match_found = False
                for query in candidates:
                    logger.info(f"Searching Cninfo for: {query}")
                    result = search_stock_cninfo(query)
                    
                    if result:
                        logger.info(f"Match found: {result['name']} ({result['code']}) using '{query}'")
                        df.at[index, 'Stock Code'] = result['code']
                        df.at[index, 'Stock Name'] = result['name']
                        updated_count += 1
                        match_found = True
                        break # Stop after first match
                    
                    time.sleep(0.5) # Small delay between attempts for same row
                
                if not match_found:
                    logger.warning(f"No match found for any candidate of: {base_query}")
            
            time.sleep(0.5) # Rate limit politeness
            
        logger.info(f"Updated {updated_count} rows in Stock List.")
        sheet_map['Stock List'] = df

    # Process 'Dividends' sheet to sync with Stock List?
    # Usually we want Dividends sheet to also have the correct Code/Name.
    # We can join/map from Stock List using filename as key.
    
    if 'Dividends' in sheet_map and 'Stock List' in sheet_map:
        df_div = sheet_map['Dividends']
        df_stock = sheet_map['Stock List']
        
        # Create a lookup dict: Filename -> (Code, Name)
        lookup = {}
        for _, row in df_stock.iterrows():
            fname = row.get('Source File')
            if fname:
                lookup[fname] = (row.get('Stock Code'), row.get('Stock Name'))
                
        # Apply to Dividends
        updated_divs = 0
        for index, row in df_div.iterrows():
            fname = row.get('Source File')
            if fname in lookup:
                new_code, new_name = lookup[fname]
                current_code = str(row.get('Stock Code', ''))
                
                if 'Unknown' in current_code or pd.isna(row.get('Stock Code')):
                    df_div.at[index, 'Stock Code'] = new_code
                    df_div.at[index, 'Stock Name'] = new_name
                    updated_divs += 1
                    
        logger.info(f"Synced {updated_divs} rows in Dividends sheet.")
        sheet_map['Dividends'] = df_div

    # Save back
    try:
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            for sheet_name, df in sheet_map.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        logger.info(f"Successfully saved enriched data to {file_path}")
    except Exception as e:
        logger.error(f"Failed to save Excel: {e}")

if __name__ == "__main__":
    enrich_data('data/TXT/extracted_dividends.xlsx')
