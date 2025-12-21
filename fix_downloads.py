
from src.downloader import Downloader
import logging
import os

def fix_wrong_files():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    
    # List of known wrong files to delete and re-download
    targets = [
        ('605011', '杭州热电'), 
        ('002962', '五方光电'), 
        ('300935', '盈建科'), 
        ('601187', '国泰环保')
    ]
    
    # 1. Delete existing wrong files
    pdf_dir = r"e:\Privy\A-Share-IPO-Prospectuses\data\pdfs"
    for code, name in targets:
        filename = f"{code}_{name}.pdf"
        filepath = os.path.join(pdf_dir, filename)
        
        # Try finding by partial match if exact name differs
        if not os.path.exists(filepath):
            for f in os.listdir(pdf_dir):
                if f.startswith(code):
                    filepath = os.path.join(pdf_dir, f)
                    break
        
        if os.path.exists(filepath):
            print(f"Deleting wrong file: {filepath}")
            os.remove(filepath)
        else:
            print(f"File not found to delete: {code}")

    # 2. Re-download specifically these codes
    downloader = Downloader()
    print("Starting re-download...")
    
    # Pass explicit list to force download even if they were marked as existing (though we just deleted them)
    # The updated Downloader.run() supports force_codes but process_stock is enough here
    for code, name in targets:
        downloader.process_stock(code, name)

if __name__ == "__main__":
    fix_wrong_files()
