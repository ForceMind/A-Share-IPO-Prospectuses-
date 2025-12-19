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

# Remove global instantiation to prevent multiprocessing recursive bomb
# task_manager = get_task_manager()

app = FastAPI(title="IPO Prospectus Dashboard")

# Setup templates
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/status")
async def get_status():
    return get_task_manager().get_status()

@app.post("/api/start")
async def start_tasks(action: str = "all", limit: int = None):
    get_task_manager().start_tasks(action=action, limit=limit)
    return {"status": "started"}

@app.post("/api/stop")
async def stop_tasks():
    get_task_manager().stop_tasks()
    return {"status": "stopping"}

@app.post("/api/config")
async def update_config(concurrency: int):
    get_task_manager().set_concurrency(concurrency)
    return {"status": "updated", "concurrency": concurrency}

@app.websocket("/ws/logs")
async def websocket_logs(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            logs = get_task_manager().get_logs()
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
