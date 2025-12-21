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
    print("3. 清理无效文件 (Audit & Clean)")
    print("4. 并行下载与解析 PDF (Download & Extract)")
    print("5. 生成状态报告 (Generate Report)")
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

def kill_process_on_port(port):
    """
    Kills the process listening on the specified port (Windows only).
    """
    if not sys.platform.startswith('win'):
        return

    try:
        # Find PID listening on the port
        cmd = f'netstat -ano | findstr :{port}'
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if not result.stdout:
            return

        lines = result.stdout.strip().split('\n')
        pids_to_kill = set()
        
        for line in lines:
            parts = line.split()
            # Standard format: Proto Local Address Foreign Address State PID
            # Example: TCP 0.0.0.0:8001 0.0.0.0:0 LISTENING 1234
            if len(parts) >= 5 and f':{port}' in parts[1] and parts[3] == 'LISTENING':
                pids_to_kill.add(parts[-1])
        
        for pid in pids_to_kill:
            # Skip if the PID is the current process (unlikely for port 8001 but good practice)
            if str(os.getpid()) == pid:
                continue
                
            print(f"[INFO] 发现端口 {port} 被占用 (PID: {pid})，正在终止...")
            subprocess.run(f'taskkill /F /PID {pid}', shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(1) # Give it a moment to release the port
            
    except Exception as e:
        print(f"[WARN] 无法释放端口 {port}: {e}")

def main():
    # Force UTF-8 output for Windows Console
    if sys.platform.startswith('win'):
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except:
            pass

    print_header()

    # Log Process ID for launcher
    print(f"[INFO] Launcher PID: {os.getpid()}")

    # 1. Install Dependencies
    # We assume python is available since this script is running
    pip_cmd = f'"{sys.executable}" -m pip install -r requirements.txt -q'
    # Skip pip check if run recently or just warn
    # if not run_command(pip_cmd, "安装/更新依赖"):
    #    input("按回车键退出...")
    #    sys.exit(1)
    
    # Optional dependency check (Warn only)
    try:
        subprocess.run(pip_cmd, shell=True, check=True)
    except:
         print("[WARN] 依赖安装可能未完全成功，尝试继续...")

    # 2. Get Stock List
    # Run get_stock_list.py
    stock_list_script = os.path.join("src", "get_stock_list.py")
    if not os.path.exists("data"):
        os.makedirs("data")
        
    stock_list_path = os.path.join("data", "stock_list.csv")
    
    # Check if stock list exists and is recent (e.g., less than 24 hours old)
    should_fetch = True
    if os.path.exists(stock_list_path):
        try:
            # Check file size to ensure it's not empty
            if os.path.getsize(stock_list_path) > 1024:
                # Check modification time
                mtime = os.path.getmtime(stock_list_path)
                if time.time() - mtime < 86400: # 24 hours
                    print(f"\n[INFO] 股票列表已存在且较新，跳过重新获取。 (Using existing stock list)")
                    should_fetch = False
        except:
            pass
            
    if should_fetch:
        get_stock_cmd = f'"{sys.executable}" {stock_list_script}'
        if not run_command(get_stock_cmd, "获取最新股票列表"):
            pass # Continue even if list update fails, maybe old list works?


    # 3. Audit & Clean (Optional but recommended)
    # Automatically clean invalid/small files before processing
    # Commented out because user complained about waiting time.
    # It is now integrated into the Web UI as an optional "Check Files" button.
    # audit_script = os.path.join("src", "audit_and_clean.py")
    # audit_cmd = f'"{sys.executable}" {audit_script}'
    # print(f"\n[INFO] 正在检查文件完整性... (Auditing files)")
    # if not run_command(audit_cmd, "清理无效PDF文件"):
    #      print(f"[WARN] 清理步骤失败，将继续执行...")

    # 4. Run Pipeline or Web Dashboard
    main_script = os.path.join("src", "main.py")
    web_script = os.path.join("src", "web_server.py")
    
    if "--web" in sys.argv:
        # Kill existing process on port 8001 to prevent conflicts
        kill_process_on_port(8001)

        print(f"\n[INFO] 正在启动 Web 仪表盘... (Starting Dashboard)")
        print(f"[INFO] 请在浏览器中打开: http://127.0.0.1:8001")
        
        # Try to open browser automatically
        import webbrowser
        threading_timer = None
        try:
            from threading import Timer
            def open_browser():
                webbrowser.open("http://127.0.0.1:8001")
            Timer(1.5, open_browser).start()
        except:
            pass
            
        web_cmd = f'"{sys.executable}" {web_script}'
        subprocess.run(web_cmd, shell=True)
    else:
        pipeline_cmd = f'"{sys.executable}" {main_script} --action all --parallel'
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
