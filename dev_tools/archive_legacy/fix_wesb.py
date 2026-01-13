
from src.downloader import Downloader
import logging
import os

def fix_wesb_download():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    
    # 688718 唯赛勃
    target_code = '688718'
    target_name = '唯赛勃'
    
    pdf_dir = r"e:\Privy\A-Share-IPO-Prospectuses\data\pdfs"
    filename = f"{target_code}_{target_name}.pdf"
    filepath = os.path.join(pdf_dir, filename)
    
    # 1. Delete if exists (it was Appendix last time)
    if os.path.exists(filepath):
        print(f"Deleting potentially wrong file: {filepath}")
        os.remove(filepath)
    else:
        # Check partial match
        for f in os.listdir(pdf_dir):
            if f.startswith(target_code):
                filepath = os.path.join(pdf_dir, f)
                print(f"Deleting partial match: {filepath}")
                os.remove(filepath)
                break

    # 2. Re-download
    downloader = Downloader()
    print(f"Starting re-download for {target_code}...")
    
    downloader.process_stock(target_code, target_name)

if __name__ == "__main__":
    fix_wesb_download()
