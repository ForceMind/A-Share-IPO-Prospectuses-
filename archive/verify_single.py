import logging
import sys
from src.downloader import Downloader

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Force utf-8 for stdout/stderr
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

def test_downloader_single(downloader, code):
    print(f"\n--- Testing Single Case: {code} ---")
    
    # 1. Get Org ID
    org_id = downloader.get_org_id(code)
    
    name = "Unknown"
    # Helper to get name
    try:
        url = 'http://www.cninfo.com.cn/new/information/topSearch/query'
        params = {'keyWord': code}
        resp = downloader.session.post(url, data=params, timeout=10)
        data = resp.json()
        for item in data:
            if item.get('code') == code:
                name = item.get('zwjc')
                break
    except:
        pass
        
    print(f"Name: {name}")

    if not org_id:
        print(f"❌ Failed to get Org ID for {code}")
        return

    # 2. Search Prospectus
    print(f"Searching prospectus...")
    candidates = downloader.search_prospectus(code, name, org_id)
    
    if candidates:
        print(f"✅ Found {len(candidates)} candidates!")
        for c in candidates:
            print(f"   - {c['announcementTitle']} | {c['adjunctUrl']}")
    else:
        print("❌ No candidates found.")

if __name__ == "__main__":
    downloader = Downloader()
    
    targets = ["601598"]
    
    for code in targets:
        test_downloader_single(downloader, code)
