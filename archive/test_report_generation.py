import pandas as pd
import os
import sys

# Ensure src is in the python path
sys.path.append(os.path.join(os.getcwd(), 'src'))

try:
    from src.main import generate_report
    from src.config import DATA_DIR, OUTPUT_DIR
except ImportError:
    # Fallback for when running from root
    from main import generate_report
    from config import DATA_DIR, OUTPUT_DIR

# Create a dummy stock_list.csv for testing
stock_list_path = os.path.join(DATA_DIR, 'test_stock_list.csv')
df = pd.DataFrame([
    {'code': '000001', 'name': '平安银行', 'listing_date': '2019-01-01', 'industry': '银行'},
    {'code': '000002', 'name': '万科A', 'listing_date': '2019-01-01', 'industry': '房地产'}
])
df.to_csv(stock_list_path, index=False)

# Run generate_report
generate_report(stock_list_path)

# Verify the output
report_path = os.path.join(OUTPUT_DIR, 'status_report.csv')
if os.path.exists(report_path):
    report_df = pd.read_csv(report_path)
    print("Report generated successfully.")
    print(report_df.head())
    if 'industry' in report_df.columns:
        print("Industry column exists in the report.")
    else:
        print("Industry column is MISSING in the report.")
else:
    print("Report file not found.")

# Clean up
if os.path.exists(stock_list_path):
    os.remove(stock_list_path)
