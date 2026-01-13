import requests
import logging
import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CNINFO_SEARCH_URL = "http://www.cninfo.com.cn/new/information/topSearch/query"

def search_stock_cninfo_debug(keyword):
    """
    Debug version of search_stock_cninfo
    """
    if not keyword or len(keyword) < 2:
        return None
        
    try:
        payload = {
            'keyWord': keyword,
            'maxNum': 5
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'http://www.cninfo.com.cn/new/index'
        }
        
        logger.info(f"Querying for '{keyword}'...")
        response = requests.post(CNINFO_SEARCH_URL, data=payload, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, list) and len(data) > 0:
                logger.info(f"  -> Found {len(data)} results.")
                for item in data:
                    logger.info(f"     Match: {item.get('zwjc')} ({item.get('code')}) - OrgId: {item.get('orgId')}")
                return data[0]
            else:
                logger.info("  -> No results found.")
        else:
            logger.error(f"  -> HTTP Error: {response.status_code}")
            
    except Exception as e:
        logger.error(f"Error searching Cninfo for '{keyword}': {e}")
        
    return None

def main():
    failing_companies = [
        "中船重工汉光科技股份有限公司", # previously failed
        "中星技术股份有限公司",         # previously failed
        # Adding more variations to test
    ]

    print("=== Starting Debug Enrichment Round 2 ===")
    
    for company in failing_companies:
        print(f"\nTarget: {company}")
        
        stripped = company.replace("股份有限公司", "").replace("有限责任公司", "")
        
        # Strategy 4: Try picking the "brand" name manually to see if it works
        # "中船汉光" for "中船重工汉光科技"
        # "中星技术" might be delisted or not public? Or maybe "中星"
        
        experimental_keywords = []
        if "中船重工汉光科技" in stripped:
             experimental_keywords.append("中船汉光")
             experimental_keywords.append("汉光科技")
        
        if "中星技术" in stripped:
             experimental_keywords.append("中星")
             
        for kw in experimental_keywords:
            print(f"--- Strategy 4: Manual Keyword '{kw}' ---")
            result = search_stock_cninfo_debug(kw)
            if result:
                print("SUCCESS")
                break
        else:
             print("FAILED experimental strategies.")

if __name__ == "__main__":
    main()
