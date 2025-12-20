import requests
import math
import time
import json
import sys

# Force utf-8 for stdout
sys.stdout.reconfigure(encoding='utf-8')

def debug_manual_mimic(code, org_id):
    print(f"\n--- Manually Mimicking Browser for {code} ---")
    url = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
    
    # 1. Get Total Count first
    params = {
        'pageNum': 1,
        'pageSize': 30,
        'column': 'szse',
        'tabName': 'fulltext',
        'plate': '',
        'stock': f'{code},{org_id}',
        'searchkey': '',
        'secid': '',
        'category': '',
        'trade': '',
        'seDate': '',
        'sortName': '',
        'sortType': '',
        'isHLtitle': 'true'
    }
    
    if code.startswith('6'):
        params['column'] = 'sse'
    
    try:
        print("1. Fetching page 1 to get total count...")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'http://www.cninfo.com.cn/new/disclosure/stock'
        }
        resp = requests.post(url, data=params, headers=headers)
        data = resp.json()
        
        total_count = data.get('totalRecordNum')
        print(f"   Total Records: {total_count}")
        
        if not total_count:
            print("   ❌ No records found.")
            return

        total_pages = math.ceil(int(total_count) / 30)
        print(f"   Total Pages (size 30): {total_pages}")
        
        # 2. Fetch Last Page directly
        print(f"2. Fetching Last Page: {total_pages}")
        params['pageNum'] = total_pages
        time.sleep(1)
        resp = requests.post(url, data=params, headers=headers)
        data = resp.json()
        announcements = data.get('announcements', [])
        
        print(f"   Found {len(announcements)} items on last page.")
        found = False
        for ann in announcements:
            title = ann['announcementTitle']
            if "招股" in title:
                print(f"   ✅ MATCH FOUND on Last Page: {title} | {ann['announcementId']}")
                found = True
        
        if not found and total_pages > 1:
             # 3. Fetch Second to Last Page
             print(f"3. Fetching 2nd to Last Page: {total_pages - 1}")
             params['pageNum'] = total_pages - 1
             time.sleep(1)
             resp = requests.post(url, data=params, headers=headers)
             data = resp.json()
             announcements = data.get('announcements', [])
             for ann in announcements:
                title = ann['announcementTitle']
                if "招股" in title:
                    print(f"   ✅ MATCH FOUND on 2nd Last Page: {title} | {ann['announcementId']}")
                    found = True

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    targets = [
        ("301003", "9900041596"),
        ("603759", "9900041834"),
        ("301042", "9900023025"),
        ("301005", "nssc1000561")
    ]
    
    for code, org in targets:
        debug_manual_mimic(code, org)
