@echo off
chcp 65001
setlocal

echo ========================================================
echo       A-Share IPO Prospectus Dividend Extractor
echo ========================================================
echo.

set /p API_KEY="Enter DeepSeek API Key (Leave empty to use Regex only): "

if not "%API_KEY%"=="" (
    set DEEPSEEK_API_KEY=%API_KEY%
    echo DeepSeek API Key set.
) else (
    echo No API Key provided. Using Regex mode.
)

echo.
echo Starting IPO Prospectus Analysis Dashboard...
start http://127.0.0.1:8001/txt_dashboard
python src/web_server.py
pause
