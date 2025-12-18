import os
import sys
import subprocess
import time

def print_header():
    print("=" * 60)
    print(" A股招股说明书现金分红一键抓取工具 (并行加速版)")
    print(" A-Share IPO Prospectus Dividend Extractor")
    print("=" * 60)
    print("\n本脚本将自动执行以下步骤：")
    print("1. 检查并安装依赖 (Check Dependencies)")
    print("2. 获取股票列表 (Fetch Stock List)")
    print("3. 并行下载与解析 PDF (Download & Extract)")
    print("4. 生成状态报告 (Generate Report)")
    print("\n[提示] 您可以随时按 Ctrl+C 停止脚本，进度会自动保存。")
    print("-" * 60)

def run_command(command, description):
    print(f"\n[INFO] 正在{description}...")
    try:
        # Use shell=True to support commands like 'pip' on Windows
        result = subprocess.run(command, shell=True, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] {description}失败: {e}")
        return False
    except KeyboardInterrupt:
        print("\n[INFO] 用户取消。")
        sys.exit(0)

def main():
    # Force UTF-8 output for Windows Console
    if sys.platform.startswith('win'):
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except:
            pass

    print_header()

    # 1. Install Dependencies
    # We assume python is available since this script is running
    pip_cmd = f"{sys.executable} -m pip install -r requirements.txt -q"
    if not run_command(pip_cmd, "安装/更新依赖"):
        input("按回车键退出...")
        sys.exit(1)

    # 2. Get Stock List
    # Run get_stock_list.py
    stock_list_script = os.path.join("src", "get_stock_list.py")
    if not os.path.exists("data"):
        os.makedirs("data")
        
    # Always run or only if missing? User feedback implies "One click", so maybe check if exists?
    # But user might want updates. Let's run it, it skips if valid usually or overwrites.
    # The script currently overwrites.
    get_stock_cmd = f"{sys.executable} {stock_list_script}"
    if not run_command(get_stock_cmd, "获取最新股票列表"):
        pass # Continue even if list update fails, maybe old list works?

    # 3. Run Pipeline
    main_script = os.path.join("src", "main.py")
    pipeline_cmd = f"{sys.executable} {main_script} --action all --parallel"
    
    print(f"\n[INFO] 正在启动并行采集流程... (Starting Pipeline)")
    try:
        subprocess.run(pipeline_cmd, shell=True)
    except KeyboardInterrupt:
        pass

    print("\n" + "="*60)
    print(" 任务结束 (Finished)")
    print(" 结果文件: data/output/dividends_summary.xlsx")
    print(" 状态报告: data/output/status_report.csv")
    print("="*60)
    input("\n按回车键退出... (Press Enter to exit)")

if __name__ == "__main__":
    main()
