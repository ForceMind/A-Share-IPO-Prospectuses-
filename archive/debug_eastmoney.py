import requests
import re
import json
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Force utf-8 for stdout
sys.stdout.reconfigure(encoding='utf-8')

def debug_eastmoney_download(code):
    print(f"\n--- Testing EastMoney Download for {code} ---")
    
    # EastMoney API parameters (F10 Notice List)
    # The previous URL was returning HTML because of missing headers or wrong endpoint.
    # Let's try the PC version API often used by browsers.
    
    url = "https://np-anotice-stock.eastmoney.com/api/security/ann"
    
    # 603171 -> 603171.SH
    # 300929 -> 300929.SZ
    # Need to map code to symbol
    symbol = f"{code}.SH" if code.startswith("6") else f"{code}.SZ"
    if code.startswith("4") or code.startswith("8"): # BSE
        symbol = f"{code}.BJ"
    
    params = {
        'sr': '-1',
        'page_size': '100',
        'page_index': '1',
        'ann_type': 'A', # All? or A-Share?
        'client_source': 'web',
        'stock_list': symbol,
        'f_node': '0', # 0 = All? 1 = ?
        's_node': '0'
    }
    
    try:
        # Need headers probably
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': f'https://data.eastmoney.com/notices/stock/{code}.html'
        }
        
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        content = resp.text
        
        try:
            data = json.loads(content)
        except:
            print(f"Failed to parse JSON: {content[:100]}")
            return
        
        notices = data.get('data', {}).get('list', [])
        print(f"Found {len(notices)} notices on first page.")
        
        found = False
        for notice in notices:
            title = notice.get('title')
            date = notice.get('notice_date')
            url_part = notice.get('art_code') # Might be full path or code
            
            if "招股说明书" in title:
                print(f"✅ Found candidate: {title} ({date})")
                # Need to check how to build URL. 
                # Usually columns is 'codes' like 'AN202106161498308728'
                # PDF link pattern: https://pdf.dfcfw.com/pdf/H2_{art_code}_1.pdf
                if url_part:
                     print(f"   URL: https://pdf.dfcfw.com/pdf/H2_{url_part}_1.pdf")
                found = True
        
        if not found:
            print("❌ No prospectus found in first 100 results.")
            
            # Fetch last page?
            # We can't easily jump to last page without knowing total count.
            # 'data' -> 'total_hits'
            total_hits = data.get('data', {}).get('total_hits', 0)
            if total_hits > 0:
                import math
                total_pages = math.ceil(total_hits / 100)
                if total_pages > 1:
                    print(f"   Total Pages: {total_pages}. Trying last page...")
                    params['page_index'] = total_pages
                    resp = requests.get(url, params=params, headers=headers, timeout=10)
                    data = resp.json()
                    notices = data.get('data', {}).get('list', [])
                    
                    for notice in notices:
                        title = notice.get('title')
                        url_part = notice.get('art_code')
                        if "招股说明书" in title:
                            print(f"✅ Found candidate on LAST page: {title}")
                            print(f"   URL: https://pdf.dfcfw.com/pdf/H2_{url_part}_1.pdf")
                            found = True

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    targets = ["603171", "300929", "301003"]
    for t in targets:
        debug_eastmoney_download(t)
