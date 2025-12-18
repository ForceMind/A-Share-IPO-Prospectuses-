import pdfplumber
import os

pdf_path = os.path.join('data', 'pdfs', '301291_明阳电气.pdf')
output_file = 'debug_text.txt'

try:
    with pdfplumber.open(pdf_path) as pdf:
        # Try a few pages
        pages_to_check = [212] # Based on previous output
        
        with open(output_file, 'w', encoding='utf-8') as f:
            for p_idx in pages_to_check:
                if p_idx < len(pdf.pages):
                    page = pdf.pages[p_idx]
                    text = page.extract_text()
                    f.write(f"--- Page {p_idx+1} ---\n")
                    f.write(text if text else "No text found")
                    f.write("\n\n")
                    
                    tables = page.extract_tables()
                    for table in tables:
                        f.write("Table:\n")
                        for row in table:
                            row_str = [str(cell).replace('\n', '') if cell else '' for cell in row]
                            f.write(str(row_str) + "\n")
    print(f"Text extracted to {output_file}")

except Exception as e:
    print(f"Error: {e}")
