import pdfplumber
import os
import sys

# Update path to a valid PDF
pdf_path = os.path.join('data', 'pdfs', '001239_永达股份.pdf')

def scan_for_dividends():
    if not os.path.exists(pdf_path):
        print(f"File not found: {pdf_path}")
        return

    print(f"Scanning {pdf_path} for dividend tables...")
    
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            try:
                text = page.extract_text()
                if not text:
                    continue
                
                # Loose check
                if ('分红' in text or '利润分配' in text) and ('万元' in text or '元' in text):
                    
                    tables = page.extract_tables()
                    if tables:
                         # Check if table contains years
                        for t_idx, table in enumerate(tables):
                            table_str = str(table)
                            if '202' in table_str or '201' in table_str:
                                print(f"\n{'='*30} Page {i+1} {'='*30}")
                                print("Text snippet:", text[:200].replace('\n', ' '))
                        
                                print(f"\nTable {t_idx+1}:")
                                for row in table:
                                    clean_row = [str(c).replace('\n', '').strip()[:30] if c else '' for c in row]
                                    print(clean_row)
            except Exception as e:
                print(f"Error on page {i+1}: {e}")

if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8')
    scan_for_dividends()
