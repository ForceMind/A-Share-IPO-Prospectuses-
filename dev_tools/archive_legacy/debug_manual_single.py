import requests
import math
import time
import json
import sys

# Force utf-8 for stdout
sys.stdout.reconfigure(encoding='utf-8')

def debug_manual_mimic_single(code, org_id):
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
        
        # Check last 15 pages
        start_check = max(1, total_pages - 15)
        
        print(f"Checking from {total_pages} down to {start_check}")
        
        found = False
        for p in range(total_pages, start_check - 1, -1):
            print(f"Checking Page: {p}...")
            params['pageNum'] = p
            time.sleep(1)
            resp = requests.post(url, data=params, headers=headers)
            data = resp.json()
            announcements = data.get('announcements', [])
            
            for ann in announcements:
                title = ann['announcementTitle']
                if "招股" in title:
                    print(f"   ✅ MATCH FOUND on Page {p}: {title} | {ann['announcementId']}")
                    found = True
            
            # Print first 3 titles of last page just to see
            if p == total_pages and announcements:
                print("   (Debug) First 3 items on last page:")
                for a in announcements[:3]:
                    print(f"     - {a['announcementTitle']} ({a['announcementTime']})") # check time

        # What if we search by keyword without page number but with sort?
        print("Searching with keyword '招股说明书' and sort ASC...")
        params['pageNum'] = 1
        params['searchkey'] = '招股说明书'
        params['sortName'] = 'pubdate'
        params['sortType'] = 'asc'
        
        time.sleep(1)
        resp = requests.post(url, data=params, headers=headers)
        data = resp.json()
        announcements = data.get('announcements', [])
        
        if announcements:
            print(f"Keyword search found {len(announcements)} results.")
            for a in announcements[:3]:
                print(f"  - {a['announcementTitle']} ({a['announcementTime']})")
        else:
            print(f"Keyword search found 0 results. (NoneType? {announcements is None})")
                
        # What if we search '首次公开发行'
        print("Searching with keyword '首次公开发行'...")
        params['searchkey'] = '首次公开发行'
        time.sleep(1)
        resp = requests.post(url, data=params, headers=headers)
        data = resp.json()
        announcements = data.get('announcements', [])
        
        if announcements:
            print(f"Keyword search found {len(announcements)} results.")
            for a in announcements[:3]:
                print(f"  - {a['announcementTitle']}")
        else:
            print(f"Keyword search found 0 results. (NoneType? {announcements is None})")

    except Exception as e:
        print(f"Error: {e}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    targets = [
        ("601598", "gshk0000598")
    ]
    
    for code, org in targets:
        debug_manual_mimic_single(code, org)
