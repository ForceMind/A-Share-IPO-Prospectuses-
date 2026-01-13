import pandas as pd

def check_unknowns(file_path):
    print(f"Checking {file_path}...")
    try:
        df = pd.read_excel(file_path, sheet_name='Stock List')
        
        total = len(df)
        unknown_code = df[df['Stock Code'].astype(str).str.contains('Unknown', na=False)]
        unknown_name = df[df['Stock Name'].astype(str).str.contains('Unknown', na=False)]
        
        # Check specific companies that were failing
        failing_companies = [
            "上海艾融软件股份有限公司",
            "上海凯鑫分离技术股份有限公司",
            "中船重工汉光科技股份有限公司",
            "上海海融食品科技股份有限公司",
            "中荣印刷集团股份有限公司",
            "中辰电缆股份有限公司",
            "中星技术股份有限公司"
        ]
        
        print(f"\nTotal rows: {total}")
        print(f"Rows with 'Unknown' Stock Code: {len(unknown_code)}")
        print(f"Rows with 'Unknown' Stock Name: {len(unknown_name)}")
        
        print("\nVerifying originally failing companies:")
        for company in failing_companies:
            # Find row where full name matches or is contained
            # Assuming 'Full Company Name' column exists and is populated
            matches = df[df['Full Company Name'].astype(str).str.contains(company, na=False)]
            
            if matches.empty:
                 # Try to match by filename if full name not reliable
                 pass
            else:
                for _, row in matches.iterrows():
                    code = row['Stock Code']
                    name = row['Stock Name']
                    print(f"  {company}: Code={code}, Name={name}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_unknowns('data/TXT/extracted_dividends.xlsx')
