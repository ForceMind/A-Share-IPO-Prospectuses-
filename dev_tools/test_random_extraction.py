import os
import random
import logging
import sys
import pandas as pd

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.txt_extractor import TxtExtractor
from src.config import DATA_DIR

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_random_extraction(num_files=2):
    txt_dir = os.path.join(DATA_DIR, "TXT")
    if not os.path.exists(txt_dir):
        print(f"Error: {txt_dir} does not exist.")
        return

    # 1. Collect all TXT files
    all_files = []
    for root, dirs, files in os.walk(txt_dir):
        for file in files:
            if file.endswith(".txt") and "extracted_dividends" not in file:
                all_files.append(os.path.join(root, file))

    if not all_files:
        print("No TXT files found.")
        return

    # 2. Select random files
    selected_files = random.sample(all_files, min(num_files, len(all_files)))
    
    print(f"Selected {len(selected_files)} files for testing:\n")
    
    extractor = TxtExtractor()
    
    # Ask for API Key
    print("\n" + "="*50)
    # Automatically check env var for CI/Automated testing
    api_key = os.environ.get("DEEPSEEK_API_KEY", "")
    if not api_key:
        print("[INFO] DEEPSEEK_API_KEY not found in environment.")
        # Try to read from a local config file if exists (optional)
        try:
            if os.path.exists("api_key.txt"):
                with open("api_key.txt", "r") as f:
                    api_key = f.read().strip()
                    print("[INFO] Loaded API Key from api_key.txt")
        except:
            pass
    
    if api_key:
        print("检测到 DeepSeek API Key。已启用 AI 增强提取 (AI Enhanced Extraction)。")
    else:
        print("未检测到 DeepSeek API Key。仅使用正则提取 (Regex Only)。")

    # Mock AI option for testing flow without real key
    use_mock_ai = False
    if not api_key:
        try:
            ans = input("是否启用模拟 AI (Mock AI) 以测试完整流程? (y/n): ").strip().lower()
            if ans == 'y':
                use_mock_ai = True
                print("已启用模拟 AI 模式。")
        except EOFError:
            pass

    total_cost = 0.0
    all_results = []
    
    for i, file_path in enumerate(selected_files):
        print(f"[{i+1}/{len(selected_files)}] 处理中: {os.path.basename(file_path)}")
        
        try:
            should_force_ai = True if (api_key or use_mock_ai) else False
            
            # Inject Mock if needed
            if use_mock_ai:
                # Monkey patch the extractor instance for this test
                def mock_extract(text, key):
                     # Simulate successful AI response
                     return [
                         {'year': '2022', 'amount': '1234.56', 'unit': '万元', 'metric': 'dividend', 'amount_text': '1234.56', 'is_ai': True, 'ai_cost': 0.0001, 'raw_text': text},
                         {'year': '2022', 'amount': '5000.00', 'unit': '万元', 'metric': 'net_profit', 'amount_text': '5000.00', 'is_ai': True, 'ai_cost': 0.0001, 'raw_text': text},
                         {'year': '2022', 'amount': '6000.00', 'unit': '万元', 'metric': 'operating_cash_flow', 'amount_text': '6000.00', 'is_ai': True, 'ai_cost': 0.0001, 'raw_text': text}
                     ], 0.0001, "Mock Prompt: " + text[:20] + "...", "{\"mock_json\": \"response\"}"
                
                extractor._extract_with_ai = mock_extract

            result = extractor.extract_from_file(
                file_path, 
                api_key="mock_key" if use_mock_ai else api_key, 
                cost_limit=10.0, 
                current_cost=total_cost,
                force_ai=should_force_ai
            )

            if result:
                # 'dividends' key holds all financial data now in the new logic
                financial_data = result.get('dividends', [])
                cost = result.get('cost', 0.0)
                total_cost += cost
                
                if financial_data:
                    print(f"  发现 {len(financial_data)} 条记录:")
                    for item in financial_data:
                        is_ai = "AI" if item.get('is_ai') else "Regex"
                        amount = item.get('amount_text', 'N/A')
                        unit = item.get('unit', '')
                        year = item.get('year', 'N/A')
                        metric = item.get('metric', 'dividend') # Default to dividend
                        
                        # Translate metric for display
                        metric_map = {'dividend': '分红', 'net_profit': '净利润', 'operating_cash_flow': '经营现金流'}
                        metric_display = metric_map.get(metric, metric)

                        print(f"    - [{metric_display}] 年份: {year}, 金额: {amount} {unit} [{is_ai}]")
                        if item.get('is_ai'):
                            print(f"      AI 成本: ¥{item.get('ai_cost', 0.0):.4f}")
                            # Clean AI Response (remove braces)
                            ai_response_raw = item.get('ai_response', '')
                            ai_response_clean = str(ai_response_raw).replace('{', '').replace('}', '')
                            
                            # Log first few chars
                            print(f"      AI 提示词(前50): {str(item.get('ai_prompt', ''))[:50]}...")
                            print(f"      AI 响应(清洗后): {ai_response_clean[:50]}...")
                        else:
                            ai_response_clean = ""

                        # Add to results list
                        all_results.append({
                            "文件": os.path.basename(file_path),
                            "公司": result.get('company_name', ''),
                            "年份": year,
                            "指标": metric_display,
                            "金额": amount,
                            "单位": unit,
                            "来源": is_ai,
                            "AI成本": item.get('ai_cost', 0.0),
                            "原文": item.get('raw_text', ''),
                            "AI提示词": item.get('ai_prompt', ''),
                            "AI响应内容": ai_response_clean
                        })
                else:
                    print("  未找到财务数据。")
                    # Add a record indicating no data found
                    all_results.append({
                        "文件": os.path.basename(file_path),
                        "公司": result.get('company_name', ''),
                        "年份": "N/A",
                        "指标": "N/A",
                        "金额": "N/A",
                        "单位": "",
                        "来源": "",
                        "AI成本": 0.0,
                        "原文": "",
                        "AI提示词": "",
                        "AI响应内容": ""
                    })
            else:
                 print("  提取失败 (返回空)")

        except Exception as e:
            print(f"  处理出错: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "="*50)
    print(f"测试完成. 总成本: ¥{total_cost:.4f}")
    
    if all_results:
        df = pd.DataFrame(all_results)
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(DATA_DIR, f"test_extraction_results_zh_{timestamp}.xlsx")
        df.to_excel(out_path, index=False)
        print(f"结果已保存至: {out_path}")
    print("="*50)

import time
from src.txt_process_manager import get_txt_manager

def test_manager_pipeline(limit=3):
    print("="*60)
    print(f"Testing TXT Process Manager (Limit={limit})")
    print("="*60)
    
    manager = get_txt_manager()
    
    # 1. Check/Set API Key
    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        print("[INFO] No API Key in env. Skipping AI input for automated test.")
        # print("[INPUT] Please enter DeepSeek API Key (Press Enter to skip):")
        # try:
        #     val = input().strip()
        #     if val:
        #         os.environ["DEEPSEEK_API_KEY"] = val
        #         print("[INFO] API Key set.")
        # except:
        #     pass
            
    # 2. Start
    print(f"[ACTION] Starting tasks...")
    manager.set_concurrency(2) # Safe concurrency
    manager.start_tasks(limit=limit)
    
    # 3. Monitor
    try:
        while True:
            status = manager.get_status()
            logs = manager.get_logs()
            
            for log in logs:
                print(f"[LOG] {log}")
                
            if not status['is_running'] and status['start_time']:
                # Allow a moment for final logs
                time.sleep(1)
                more_logs = manager.get_logs()
                for log in more_logs:
                    print(f"[LOG] {log}")
                break
                
            print(f"\r[STATUS] Running... Completed: {status['completed_tasks']}, Failed: {status['failed_tasks']}, Cost: {status['total_ai_cost']:.4f}", end="")
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n[STOP] Stopping tasks...")
        manager.stop_tasks()
        
    print("\n" + "="*60)
    print("Test Finished.")
    print(f"Total Cost: ¥{manager.status.get('total_ai_cost', 0):.4f}")
    print("Check data/TXT/extracted_dividends.xlsx for results.")

if __name__ == "__main__":
    # Prevent Windows Fork Bomb
    import multiprocessing
    multiprocessing.freeze_support()
    
    test_manager_pipeline(limit=3)
