@echo off
chcp 65001 >nul
echo ========================================================
echo       A股招股书分红数据自动化提取工具 - 一键运行
echo ========================================================

echo [1/3] 正在检查并安装依赖...
python -m pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo 依赖安装失败，请检查网络或 Python 环境。
    pause
    exit /b
)

echo.
echo [2/3] 正在获取 A 股上市公司列表 (2019-2023)...
python src/get_stock_list.py
if %errorlevel% neq 0 (
    echo 获取列表失败。
    pause
    exit /b
)

echo.
echo [3/3] 开始下载招股书并提取分红数据...
echo 注意：此过程可能耗时较长，因为需要下载大量 PDF 文件。
echo 日志文件位于 logs/pipeline.log
echo.
echo 如果您只想运行测试（只处理前 3 个），请按 Ctrl+C 终止，然后运行: python src/main.py --limit 3
echo.

python src/main.py --action all

echo.
echo ========================================================
echo                   任务完成！
echo 结果已保存至: data/output/dividends_summary.xlsx
echo ========================================================
pause
