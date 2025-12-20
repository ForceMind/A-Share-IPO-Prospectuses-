import requests
import json
import time
import sys

# Force utf-8 for stdout
sys.stdout.reconfigure(encoding='utf-8')

def debug_specific_announcement():
    print("Analyzing User Provided Links to understand why search failed...")
    
    url = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Origin': 'http://www.cninfo.com.cn',
        'Referer': 'http://www.cninfo.com.cn/new/disclosure/stock',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
    }

    test_cases = [
        {
            "code": "603171",
            "orgId": "9900041900",
            "target_id": "1210255157",
            "date": "2021-06-17",
            "column": "sse"
        },
        {
            "code": "300929",
            "orgId": "9900024912",
            "target_id": "1209096564",
            "date": "2021-01-14",
            "column": "szse"
        }
    ]

    for case in test_cases:
        print(f"\n--- Analyzing {case['code']} ---")
        
        # 1. Broad Search around that date
        params = {
            'pageNum': 1,
            'pageSize': 30,
            'column': case['column'],
            'tabName': 'fulltext',
            'plate': '',
            'stock': f"{case['code']},{case['orgId']}",
            'searchkey': '', 
            'secid': '',
            'category': '',
            'trade': '',
            'seDate': f"{case['date']}~{case['date']}", # Exact date search
            'sortName': '',
            'sortType': '',
            'isHLtitle': 'true'
        }
        
        try:
            print(f"1. Searching specific date {case['date']}...")
            resp = requests.post(url, data=params, headers=headers)
            data = resp.json()
            announcements = data.get('announcements', [])
            
            found = False
            for ann in announcements:
                if str(ann['announcementId']) == case['target_id']:
                    print("   ✅ Found target by date search!")
                    print(f"      Title: {ann['announcementTitle']}")
                    print(f"      TypeName: {ann['announcementTypeName']}")
                    print(f"      ColumnId: {ann['columnId']}") # This is the Category!
                    found = True
                    break
            
            if not found:
                print("   ❌ NOT found by exact date search.")
                print(f"   Total announcements on this date: {len(announcements)}")

        except Exception as e:
            print(f"   Error: {e}")

        # 2. Search with keyword '招股说明书' globally for that stock
        params['seDate'] = ''
        params['searchkey'] = '招股说明书'
        
        try:
            print(f"2. Searching with keyword '招股说明书'...")
            resp = requests.post(url, data=params, headers=headers)
            data = resp.json()
            announcements = data.get('announcements', [])
            
            found = False
            if announcements:
                for ann in announcements:
                    if str(ann['announcementId']) == case['target_id']:
                        print("   ✅ Found target by keyword search!")
                        print(f"      Title: {ann['announcementTitle']}")
                        found = True
                        break
            
            if not found:
                print("   ❌ NOT found by keyword search.")
                print(f"   Total found: {data.get('totalAnnouncement')}")
                if announcements:
                    print("   First 3 results:")
                    for ann in announcements[:3]:
                        print(f"      - {ann['announcementTitle']}")

        except Exception as e:
             print(f"   Error: {e}")

if __name__ == "__main__":
    debug_specific_announcement()
