import sys
import os
import subprocess
import time
import requests
import signal
import json

# Add project root to path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(PROJECT_ROOT)

WEB_SERVER_SCRIPT = os.path.join(PROJECT_ROOT, "src", "web_server.py")
PYTHON_EXE = sys.executable

def test_web_flow():
    print(f"[TEST] Starting Web Server: {WEB_SERVER_SCRIPT}")
    
    # Start the server
    # We use a new process group so we can ensure we kill it later
    process = subprocess.Popen(
        [PYTHON_EXE, WEB_SERVER_SCRIPT],
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    API_BASE = "http://127.0.0.1:3000"
    
    try:
        # Wait for server to come up
        print("[TEST] Waiting for server port 3000...")
        retries = 30
        server_up = False
        while retries > 0:
            try:
                resp = requests.get(f"{API_BASE}/api/txt/status", timeout=1)
                if resp.status_code == 200:
                    server_up = True
                    break
            except requests.exceptions.ConnectionError:
                pass
            time.sleep(1)
            retries -= 1
            
        if not server_up:
            print("[ERROR] Server failed to start in 30 seconds.")
            # Print stderr to see why
            outs, errs = process.communicate(timeout=1)
            print("--- Server STDERR ---")
            print(errs)
            return
            
        print("[TEST] Server is UP!")
        
        # Trigger Extraction for 3 files
        print("[TEST] Sending POST /api/txt/start?limit=3")
        resp = requests.post(f"{API_BASE}/api/txt/start", params={"limit": 3})
        print(f"[TEST] Start Response: {resp.json()}")
        
        if resp.status_code != 200:
            print(f"[ERROR] Failed to start task: {resp.text}")
            return
            
        # Poll Status
        print("[TEST] Polling status...")
        while True:
            resp = requests.get(f"{API_BASE}/api/txt/status")
            status_data = resp.json()
            is_running = status_data.get("is_running", False)
            logs = status_data.get("recent_logs", [])
            completed = status_data.get("completed_tasks", 0)
            
            print(f"[STATUS] Running: {is_running}, Completed: {completed}")
            if logs:
                print("--- Server Logs ---")
                for log in logs:
                    print(log)
                print("-------------------")
            
            if not is_running and completed >= 3:
                print("[SUCCESS] Task finished with 3 items processed.")
                # Optional: Check logs for "finished"
                break
                
            if not is_running and completed < 3:
                # It might have just started and is_running is False? 
                # Or it finished instantly? Or failed?
                # Give it a grace period if it's 0
                if completed == 0:
                   # check if it actually failed?
                   time.sleep(1)
                   continue
                print("[WARNING] Task finished but count < 3? (Maybe file count < 3)")
                break
                
            time.sleep(2)
            
        # Success verification
        
    except KeyboardInterrupt:
        print("\n[TEST] Interrupted by user.")
    except Exception as e:
        print(f"[ERROR] Exception: {e}")
    finally:
        print("[TEST] Stopping server...")
        try:
            process.terminate()
            outs, errs = process.communicate(timeout=5)
            print("--- Server STDOUT ---")
            if outs: print(outs[:2000] + "\n...(truncated)" if len(outs)>2000 else outs)
            print("--- Server STDERR ---")
            if errs: print(errs)
        except Exception as e:
            print(f"Error during shutdown: {e}")
            try: process.kill() 
            except: pass
        print("[TEST] Server stopped.")

if __name__ == "__main__":
    # Ensure requests is installed or handled (standard venv has it)
    try:
        import requests
    except ImportError:
        print("Installing requests...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
        import requests
        
    test_web_flow()
