import logging
import sys
import os
from src.downloader import Downloader

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Force utf-8 for stdout/stderr
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

def test_downloader_logic(downloader, code):
    print(f"\n--- Testing Downloader Logic for {code} ---")
    
    # 1. Get Org ID
    org_id = downloader.get_org_id(code)
    if not org_id:
        print(f"❌ Failed to get Org ID for {code}")
        return

    # Infer name (hacky way since get_org_id doesn't return name, we rely on debug info or just pass empty if needed)
    # But wait, search_prospectus needs name for fallback strategies.
    # We can fetch it again or hardcode for this test.
    
    # Let's quickly get name
    name = "Unknown"
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
    
    print(f"Name identified as: {name}")

    # 2. Search Prospectus
    print(f"Searching prospectus for {code} ({name})...")
    candidates = downloader.search_prospectus(code, name, org_id)
    
    if candidates:
        print(f"✅ Found {len(candidates)} candidates!")
        for c in candidates:
            print(f"   - {c['announcementTitle']} | {c['adjunctUrl']}")
            
        # Try downloading the first one to a temp location to verify it works
        target = candidates[0]
        adjunct_url = target['adjunctUrl']
        download_url = "http://static.cninfo.com.cn/" + adjunct_url
        filename = f"test_{code}.pdf"
        
        print(f"Attempting to download {filename} from {download_url}...")
        if downloader.download_file(download_url, filename):
             print(f"✅ Download successful: {filename}")
             # Clean up
             if os.path.exists(filename):
                 os.remove(filename)
                 print("Cleaned up test file.")
        else:
             print(f"❌ Download failed.")

    else:
        print("❌ No candidates found even with new logic.")

if __name__ == "__main__":
    downloader = Downloader()
    
    targets = ["603171", "300929", "301037"]
    
    for code in targets:
        test_downloader_logic(downloader, code)
