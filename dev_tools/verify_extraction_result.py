import pandas as pd
import os
import sys

def verify_excel():
    excel_path = "data/TXT/extracted_dividends.xlsx"
    if not os.path.exists(excel_path):
        print(f"Error: {excel_path} not found.")
        return

    print(f"Verifying {excel_path}...")
    
    try:
        xls = pd.ExcelFile(excel_path)
        
        # 1. Verify Sheet Names
        expected_sheets = ['Stock List', 'Dividends']
        if not all(sheet in xls.sheet_names for sheet in expected_sheets):
             print(f"Error: Missing sheets. Found: {xls.sheet_names}, Expected: {expected_sheets}")
             return

        # 2. Verify Stock List Columns
        df_stock = pd.read_excel(excel_path, sheet_name='Stock List')
        expected_cols_stock = ['Stock Name', 'Stock Code', 'Board', 'Industry', 'IPO Date', 'Full Company Name', 'Source File']
        
        missing_cols = [c for c in expected_cols_stock if c not in df_stock.columns]
        if missing_cols:
             print(f"Error: Stock List sheet missing columns: {missing_cols}")
        else:
             print("Stock List columns verified.")

        # 3. Verify Dividends Columns
        df_div = pd.read_excel(excel_path, sheet_name='Dividends')
        expected_cols_div = ['Stock Name', 'Stock Code', 'Dividend Year', 'Dividend Amount', 'Context Source', 'Source File']
        
        missing_cols_div = [c for c in expected_cols_div if c not in df_div.columns]
        if missing_cols_div:
             print(f"Error: Dividends sheet missing columns: {missing_cols_div}")
        else:
             print("Dividends columns verified.")
             
        # 4. Check for Empty Basic Info
        # We check if Stock Name or Stock Code is 'Unknown' or NaN in Stock List
        empty_mask = (df_stock['Stock Name'] == 'Unknown') | (df_stock['Stock Code'] == 'Unknown') | (df_stock['Stock Name'].isna()) | (df_stock['Stock Code'].isna())
        empty_count = empty_mask.sum()
        
        if empty_count > 0:
            print(f"Warning: Found {empty_count} records with empty/unknown basic info in Stock List.")
            print("Files with missing info:")
            print(df_stock[empty_mask][['Source File', 'Stock Name', 'Stock Code', 'Full Company Name']].to_string())
        else:
            print("No empty basic info found in Stock List.")

        # 5. Check Dividends Empty Info
        if not df_div.empty:
            empty_div_mask = (df_div['Stock Name'] == 'Unknown') | (df_div['Stock Code'] == 'Unknown')
            empty_div_count = empty_div_mask.sum()
            if empty_div_count > 0:
                print(f"Warning: Found {empty_div_count} dividend records with unknown stock info.")
            else:
                 print("All dividend records have stock info.")
        
    except Exception as e:
        print(f"Verification failed: {e}")

if __name__ == "__main__":
    verify_excel()
