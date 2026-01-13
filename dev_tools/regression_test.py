import os
import logging
import sys
import random
# Set up logging BEFORE importing other modules
logging.getLogger("pdfminer").setLevel(logging.WARNING)

from src.extractor import ProspectusExtractor

# Set up main logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def run_regression_test():
    # Path to PDF directory
    pdf_dir = os.path.join("data", "pdfs")
    
    # Get all PDF files
    all_pdfs = [f for f in os.listdir(pdf_dir) if f.endswith('.pdf')]
    
    # Select a random sample of 10 additional PDFs + the original 5
    original_failures = [
        "301011_华立科技.pdf",
        "301012_扬电科技.pdf",
        "301013_利和兴.pdf",
        "301015_百洋医药.pdf",
        "301016_雷尔伟.pdf"
    ]
    
    # Filter out original failures from the pool
    pool = [p for p in all_pdfs if p not in original_failures]
    
    # Sample 10 random PDFs if available, else take all
    sample_pdfs = random.sample(pool, min(10, len(pool)))
    
    test_set = original_failures + sample_pdfs
    
    extractor = ProspectusExtractor()
    
    print(f"Running regression test on {len(test_set)} PDFs...")
    
    results_summary = []
    
    for pdf_file in test_set:
        print(f"\nProcessing: {pdf_file}")
        pdf_path = os.path.join(pdf_dir, pdf_file)
        
        try:
            results = extractor.extract(pdf_path)
            
            # Simple validation: Check if we have insane numbers or future years
            has_issues = False
            issue_desc = []
            
            for res in results:
                if 'year' in res and str(res['year']).isdigit():
                    if int(res['year']) > 2024:
                        has_issues = True
                        issue_desc.append(f"Future Year {res['year']}")
                
                if 'amount' in res:
                    # Arbitrary large number check (> 5 billion RMB might be suspicious for these small caps, but possible)
                    if res['amount'] > 500000: # 50亿
                         issue_desc.append(f"Huge Amount {res['amount']}")
            
            status = "PASS" if not has_issues else f"WARN: {', '.join(issue_desc)}"
            results_summary.append({
                "file": pdf_file,
                "count": len(results),
                "status": status,
                "data": results
            })
            
        except Exception as e:
            results_summary.append({
                "file": pdf_file,
                "status": f"ERROR: {str(e)}",
                "count": 0,
                "data": []
            })

    print("\n" + "="*60)
    print("REGRESSION TEST SUMMARY")
    print("="*60)
    print(f"{'File':<30} | {'Count':<5} | {'Status'}")
    print("-" * 60)
    
    for item in results_summary:
        print(f"{item['file'][:30]:<30} | {item['count']:<5} | {item['status']}")
        if item['count'] > 0:
            # Print brief data for verification
            years = [str(x.get('year', 'N/A')) for x in item['data'] if 'note' not in x]
            if years:
                print(f"   -> Years found: {', '.join(years)}")
            else:
                 print(f"   -> No valid data extracted (notes only)")

if __name__ == "__main__":
    run_regression_test()
