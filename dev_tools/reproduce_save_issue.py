if __name__ == '__main__':
    from src.txt_process_manager import TxtProcessManager
    import os
    import pandas as pd
    import multiprocessing

    multiprocessing.freeze_support()

    # Setup
    manager = TxtProcessManager()
    base_dir = "data/TXT"
    output_file = os.path.join(base_dir, "repro_dividends.xlsx")

    # Ensure directory exists
    os.makedirs(base_dir, exist_ok=True)

    # Test 1: Empty lists
    print("Test 1: Saving empty lists...")
    manager._save_to_excel([], [], base_dir=base_dir)

    # Check result
    target_file = os.path.join(base_dir, "extracted_dividends.xlsx")
    if os.path.exists(target_file):
        try:
            xl = pd.ExcelFile(target_file, engine='openpyxl')
            print(f"Sheets after empty save: {xl.sheet_names}")
        except Exception as e:
            print(f"Error reading empty save: {e}")
    else:
        print("File not created.")

    # Test 2: Some data
    print("\nTest 2: Saving some data...")
    dividends = [{'company_name': 'Test', 'stock_code': '000001', 'amount': '10', 'unit': '元'}]
    stock_infos = [{'company_name': 'Test', 'stock_code': '000001'}]

    manager._save_to_excel(dividends, stock_infos, base_dir=base_dir)

    if os.path.exists(target_file):
        try:
            xl = pd.ExcelFile(target_file, engine='openpyxl')
            print(f"Sheets after data save: {xl.sheet_names}")
        except Exception as e:
            print(f"Error reading data save: {e}")
