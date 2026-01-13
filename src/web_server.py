import logging
import sys
import os

# Add the project root to sys.path to allow absolute imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, Request, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn
import os
import asyncio
from src.task_manager import get_task_manager
from src.txt_process_manager import get_txt_manager

# Remove global instantiation to prevent multiprocessing recursive bomb
# task_manager = get_task_manager()

app = FastAPI(title="IPO Prospectus Dashboard")

# Setup templates
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/txt_dashboard", response_class=HTMLResponse)
async def txt_dashboard(request: Request):
    return templates.TemplateResponse("txt_dashboard.html", {"request": request})

@app.get("/api/status")
async def get_status():
    return get_task_manager().get_status()

@app.get("/api/txt/status")
async def get_txt_status():
    return get_txt_manager().get_status()

@app.post("/api/start")
async def start_tasks(action: str = "all", limit: int = None):
    get_task_manager().start_tasks(action=action, limit=limit)
    return {"status": "started"}

@app.post("/api/txt/start")
async def start_txt_tasks(limit: int = None):
    get_txt_manager().start_tasks(limit=limit)
    return {"status": "started"}

@app.post("/api/stop")
async def stop_tasks():
    get_task_manager().stop_tasks()
    return {"status": "stopping"}

@app.post("/api/txt/stop")
async def stop_txt_tasks():
    get_txt_manager().stop_tasks()
    return {"status": "stopping"}

@app.post("/api/verify")
async def verify_data():
    get_task_manager().start_tasks(action="verify")
    return {"status": "verification_started"}

@app.post("/api/config")
async def update_config(download_concurrency: int = None, extract_concurrency: int = None):
    get_task_manager().set_concurrency(download=download_concurrency, extract=extract_concurrency)
    return {
        "status": "updated", 
        "download_concurrency": get_task_manager().status.get("download_concurrency"),
        "extract_concurrency": get_task_manager().status.get("extract_concurrency")
    }

@app.post("/api/txt/config")
async def update_txt_config(concurrency: int = None, cost_limit: float = None, force_ai: bool = None):
    if concurrency:
        get_txt_manager().set_concurrency(concurrency)
    if cost_limit is not None:
        get_txt_manager().set_cost_limit(cost_limit)
    if force_ai is not None:
        get_txt_manager().set_force_ai(force_ai)
    return {
        "status": "updated",
        "concurrency": get_txt_manager().status.get("concurrency"),
        "ai_cost_limit": get_txt_manager().status.get("ai_cost_limit"),
        "force_ai": get_txt_manager().status.get("force_ai")
    }

@app.websocket("/ws/logs")
async def websocket_logs(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            # Pull from both managers
            logs_pdf = get_task_manager().get_logs()
            logs_txt = get_txt_manager().get_logs()
            
            all_logs = logs_pdf + logs_txt
            
            if all_logs:
                for log in all_logs:
                    await websocket.send_text(log)
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"WebSocket error: {e}")

@app.websocket("/ws/txt/logs")
async def websocket_txt_logs(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            logs = get_txt_manager().get_logs()
            if logs:
                for log in logs:
                    await websocket.send_text(log)
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"WebSocket error: {e}")

def run_server(host="127.0.0.1", port=8001):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - [PID:%(process)d] - %(levelname)s - %(message)s')
    uvicorn.run(app, host=host, port=port)

if __name__ == "__main__":
    run_server()
