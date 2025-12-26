import pandas as pd
import os

base_dir = "data/TXT"
output_file = os.path.join(base_dir, "debug_excel_output.xlsx")

print(f"Testing Excel write to {output_file}")

try:
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Sheet 1: Dividends (Empty case)
        print("Writing Dividends sheet...")
        pd.DataFrame(columns=['No Dividends Found']).to_excel(writer, sheet_name='Dividends', index=False)

        # Sheet 2: Stock Info (Empty case)
        print("Writing StockInfo sheet...")
        pd.DataFrame(columns=['No Stock Info']).to_excel(writer, sheet_name='StockInfo', index=False)

    print("Write finished.")
    
    # Verify
    xl = pd.ExcelFile(output_file, engine='openpyxl')
    print(f"Sheets found: {xl.sheet_names}")

except Exception as e:
    print(f"Error: {e}")
