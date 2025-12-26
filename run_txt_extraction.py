import os
import csv
from src.txt_extractor import TxtExtractor

def main():
    base_dir = "data/TXT"
    output_file = os.path.join(base_dir, "extracted_dividends.csv")
    extractor = TxtExtractor()
    
    results = []
    
    print(f"Scanning directory: {base_dir}")
    
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".txt") and "extracted_dividends" not in file:
                file_path = os.path.join(root, file)
                print(f"Processing {file}...")
                
                try:
                    data = extractor.extract_from_file(file_path)
                    if data and data['dividends']:
                        for div in data['dividends']:
                            results.append({
                                "company_name": data['company_name'],
                                "filename": data['filename'],
                                "year": div['year'],
                                "amount": div['amount_text'],
                                "unit": div['unit'],
                                "raw_context": div['raw_text']
                            })
                except Exception as e:
                    print(f"Failed to process {file}: {e}")

    # Save to CSV
    if results:
        keys = results[0].keys()
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(results)
        print(f"Extraction complete. Results saved to {output_file}")
    else:
        print("No dividend information found.")

if __name__ == "__main__":
    main()
