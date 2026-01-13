import pandas as pd
import os

file_path = 'data/TXT/extracted_dividends.xlsx'

if os.path.exists(file_path):
    try:
        # Read all sheets
        xl = pd.ExcelFile(file_path)
        print(f"Sheets found: {xl.sheet_names}")
        
        for sheet in xl.sheet_names:
            df = pd.read_excel(file_path, sheet_name=sheet)
            print(f"\n--- Sheet: {sheet} ---")
            print(df.head())
            print("-" * 20)
            
            # Check for 'Unknown' or missing codes/names
            # Assuming columns might be 'company_name', 'stock_code' based on previous context
            if 'company_name' in df.columns:
                unknown_names = df[df['company_name'].astype(str).str.contains('Unknown', case=False, na=True)]
                print(f"Rows with 'Unknown' company_name: {len(unknown_names)}")
                
            if 'stock_code' in df.columns:
                unknown_codes = df[df['stock_code'].astype(str).str.contains('Unknown', case=False, na=True) | df['stock_code'].isna()]
                print(f"Rows with 'Unknown' or NaN stock_code: {len(unknown_codes)}")

    except Exception as e:
        print(f"Error reading excel: {e}")
else:
    print(f"File not found: {file_path}")
