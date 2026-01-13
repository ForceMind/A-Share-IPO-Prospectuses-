import logging
import sys
import json
from src.downloader import Downloader

# Configure logging to show info
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Force utf-8 for stdout/stderr to avoid encoding errors in terminal
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

def debug_stock(downloader, code):
    print(f"\n--- Debugging {code} ---")
    
    # 1. Get Org ID
    print(f"1. Fetching Org ID for {code}...")
    url = 'http://www.cninfo.com.cn/new/information/topSearch/query'
    params = {'keyWord': code}
    
    org_id = None
    name = None
    
    try:
        response = downloader.session.post(url, data=params, timeout=10)
        data = response.json()
        
        for item in data:
            if item.get('code') == code:
                org_id = item.get('orgId')
                name = item.get('zwjc')
                print(f"   Found match: orgId={org_id}, name={name}")
                break
        
        if not org_id:
            print("   Org ID not found via code match.")
            return
    except Exception as e:
        print(f"   Error fetching Org ID: {e}")
        return

    # 2. Search Prospectus using standard logic
    print(f"2. Searching Prospectus for {code} ({name}) with orgId {org_id} (Standard Logic)...")
    results = downloader.search_prospectus(code, name, org_id)
    print(f"   Standard Logic Found {len(results)} candidates.")
    for idx, res in enumerate(results):
        print(f"   [{idx}] {res.get('announcementTitle')} - {res.get('adjunctUrl')}")

    if results:
        return

    # 3. Broad Search Debugging
    print(f"3. Performing Deep Debugging for {code} ({name})...")
    
    # Common params
    base_params = {
        'pageNum': 1,
        'pageSize': 50, # Increase page size
        'tabName': 'fulltext',
        'isHLtitle': 'true',
    }

    test_cases = [
        {
            "name": "Code Only, Sort ASC (Find Oldest)",
            "params": {
                'stock': f'{code},{org_id}',
                'searchkey': '',
                'column': 'szse' if code.startswith(('0', '3')) else 'sse',
                'category': '',
                'sortName': 'pubdate',
                'sortType': 'asc' # Oldest first
            }
        },
         {
            "name": "Name + '公开发行' (No Category)",
            "params": {
                'stock': '',
                'searchkey': f'{name} 公开发行',
                'column': 'szse' if code.startswith(('0', '3')) else 'sse',
                'category': '' 
            }
        },
        {
             "name": "Name + '上市公告书' (Backup)",
             "params": {
                'stock': '',
                'searchkey': f'{name} 上市公告书',
                'column': 'szse' if code.startswith(('0', '3')) else 'sse',
                'category': '' 
            }
        }
    ]

    url = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'

    for case in test_cases:
        print(f"   --- Test Case: {case['name']} ---")
        p = base_params.copy()
        p.update(case['params'])
        
        try:
            response = downloader.session.post(url, data=p, timeout=10)
            data = response.json()
            announcements = data.get('announcements')
            
            if announcements:
                print(f"     ✅ Found {len(announcements)} results!")
                found_count = 0
                for ann in announcements:
                    title = ann.get('announcementTitle')
                    # Print first 5 always
                    if found_count < 5:
                        print(f"       - {ann.get('secName')} | {title} | {ann.get('adjunctUrl')}")
                    
                    # Also print if it looks like a prospectus but wasn't in first 5
                    if found_count >= 5 and ("招股" in title or "发行" in title):
                         print(f"       (MATCH) {title} | {ann.get('adjunctUrl')}")
                    
                    found_count += 1

            else:
                print(f"     ❌ No results. (Total: {data.get('totalAnnouncement')})")
                
        except Exception as e:
            print(f"     ❌ Error: {e}")

if __name__ == "__main__":
    downloader = Downloader()
    
    # List provided by user
    targets = [
        "603171", # 税友股份
        "300929", # 华骐环保
        "301037"  # 保立佳
    ]
    
    for code in targets:
        debug_stock(downloader, code)
