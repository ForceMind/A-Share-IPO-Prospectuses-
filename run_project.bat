@echo off
cd /d "%~dp0"
if not exist venv (
    echo [INFO] Creating virtual environment...
    python -m venv venv
)
call venv\Scripts\activate
python src/launcher.py
pause
