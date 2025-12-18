import os
import logging
import sys

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
from extractor import ProspectusExtractor

def test_file(filename):
    pdf_path = os.path.join('data', 'pdfs', filename)
    if not os.path.exists(pdf_path):
        print(f"Skipping {filename} (not found)")
        return

    print(f"\nTesting {filename}...")
    extractor = ProspectusExtractor()
    results = extractor.extract(pdf_path)
    
    print("-" * 30)
    print(f"Results for {filename}:")
    for r in results:
        print(r)
    print("-" * 30)

if __name__ == '__main__':
    # Test a few files we know exist
    files = [
        '301291_明阳电气.pdf',
        '001239_永达股份.pdf',
        '300904_威力传动.pdf' 
    ]
    
    for f in files:
        test_file(f)
