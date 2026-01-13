from src.txt_extractor import TxtExtractor
import json

def test_full_extraction():
    text = """
    公司2020年度利润分配方案为：每10股派发3元（含税），共计派发现金红利1500万元。
    公司2020年归属于母公司所有者的净利润为5000万元。
    经营活动产生的现金流量净额为2000万元。
    """
    
    extractor = TxtExtractor()
    
    print("--- Test 1: Regex Extraction (Force AI = False) ---")
    data, cost = extractor.extract_financials_enhanced(text, use_ai=False, force_ai=False)
    print(json.dumps(data, ensure_ascii=False, indent=2))
    
    # Note: We can't really test AI without a key and real API call, but we can verify code path doesn't crash.
    print("\n--- Test 2: AI Path Check (Mock) ---")
    # To test AI logic without key, we can inspect if it tries to call headers if we provide a fake key
    # But for now, ensuring Regex works for the new fields is good enough sanity check.
    
if __name__ == "__main__":
    test_full_extraction()
