from src.task_manager import TaskManager
import logging
import threading
import time

def verify_pipeline():
    tm = TaskManager()
    
    # 模拟任务
    def stop_later():
        time.sleep(5)
        tm.stop_tasks()
        
    threading.Thread(target=stop_later).start()
    
    # 启动任务 - 仅解析，因为下载可能太慢
    # 但为了复现"下载完成后没有开始解析"，我们应该尝试一个小范围的 all
    print("Starting pipeline action='all' limit=1...")
    tm.start_tasks(action="all", limit=1)
    
    while tm.status["is_running"]:
        time.sleep(1)
        print(f"Status: {tm.get_status()}")
        
    print("Pipeline finished.")
    print(f"Final Status: {tm.get_status()}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    verify_pipeline()
