import os
import logging
import sys
# Set up logging BEFORE importing other modules to catch everything
# Suppress pdfminer debug logs which are causing terminal issues
logging.getLogger("pdfminer").setLevel(logging.WARNING)

from src.extractor import ProspectusExtractor

# Set up main logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def run_debug_extraction():
    # Define the first 5 PDFs to debug
    pdf_files = [
        "301011_华立科技.pdf",
        "301012_扬电科技.pdf",
        "301013_利和兴.pdf",
        "301015_百洋医药.pdf",
        "301016_雷尔伟.pdf"
    ]
    
    # Path to PDF directory (relative to workspace root)
    pdf_dir = os.path.join("data", "pdfs")
    
    extractor = ProspectusExtractor()
    
    for pdf_file in pdf_files:
        print(f"\n{'='*50}")
        print(f"DEBUGGING: {pdf_file}")
        print(f"{'='*50}\n")
        
        pdf_path = os.path.join(pdf_dir, pdf_file)
        if not os.path.exists(pdf_path):
            print(f"Error: File not found at {pdf_path}")
            continue
            
        results = extractor.extract(pdf_path)
        
        print(f"\n--- EXTRACTION RESULTS for {pdf_file} ---")
        if not results:
            print("No results found.")
        else:
            for i, res in enumerate(results):
                print(f"\nItem {i+1}:")
                for k, v in res.items():
                    if k == 'context':
                        # Print only snippet of context to keep it readable
                        print(f"  {k}: {repr(v[:200])}...")
                    else:
                        print(f"  {k}: {v}")

if __name__ == "__main__":
    run_debug_extraction()
