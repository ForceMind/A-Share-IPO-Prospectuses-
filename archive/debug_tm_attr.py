import sys
import os

sys.path.append(os.getcwd())

from src.task_manager import TaskManager, get_task_manager

def check_task_manager():
    tm = TaskManager()
    print(f"TaskManager instance: {tm}")
    print(f"Has _run_extraction: {hasattr(tm, '_run_extraction')}")
    print(f"Has _run_download_phase: {hasattr(tm, '_run_download_phase')}")
    
    if hasattr(tm, '_run_extraction'):
        print("_run_extraction is available.")
    else:
        print("_run_extraction is MISSING.")

if __name__ == "__main__":
    check_task_manager()
