@echo off
chcp 65001 > nul
echo ========================================================
echo A股招股说明书现金分红一键抓取工具 (并行加速版)
echo A-Share IPO Prospectus Dividend Extractor (Parallel Mode)
echo ========================================================
echo.
echo 本脚本将自动执行以下步骤：
echo 1. 生成或更新股票列表 (Stock List)
echo 2. 并行下载招股说明书 PDF 并同时进行解析 (Download & Extract)
echo 3. 支持断点续传：自动跳过已下载和已解析的文件
echo 4. 生成最终状态报告 (Status Report)
echo.

if not exist venv (
    echo [INFO] 正在创建虚拟环境 (Creating virtual environment)...
    python -m venv venv
)

echo [INFO] 正在激活虚拟环境 (Activating virtual environment)...
call venv\Scripts\activate

echo [INFO] 正在安装/更新依赖 (Installing dependencies)...
pip install -r requirements.txt -q

if not exist data (
    mkdir data
)

if not exist data\stock_list.csv (
    echo [INFO] 正在获取股票列表 (Fetching stock list)...
    python src/get_stock_list.py
)

echo.
echo [INFO] 正在启动并行采集流程... (Starting Pipeline)
echo [INFO] 提示：您可以随时按 Ctrl+C 停止脚本，进度会自动保存。
echo [INFO] Tip: You can press Ctrl+C to stop anytime. Progress is saved.
echo.

python src/main.py --action all --parallel

echo.
echo [INFO] 任务完成！(Done!)
echo [INFO] 结果文件 (Results): data/output/dividends_summary.xlsx
echo [INFO] 状态报告 (Report):  data/output/status_report.csv
pause
