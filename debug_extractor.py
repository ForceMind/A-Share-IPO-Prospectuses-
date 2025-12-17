import pdfplumber
import os

pdf_path = os.path.join('data', 'pdfs', '688008_澜起科技.pdf')
pages_to_debug = [332, 333, 334] # 0-indexed: 332 is page 333

with pdfplumber.open(pdf_path) as pdf:
    for p_idx in pages_to_debug:
        if p_idx < len(pdf.pages):
            page = pdf.pages[p_idx]
            text = page.extract_text()
            print(f"--- Page {p_idx+1} Text ---")
            print(text)
            print("-" * 30)
            
            tables = page.extract_tables()
            print(f"--- Page {p_idx+1} Tables ---")
            for table in tables:
                for row in table:
                    print(row)
            print("=" * 30)
