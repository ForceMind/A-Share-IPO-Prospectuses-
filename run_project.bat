@echo off
cd /d "%~dp0"
chcp 65001

echo ========================================================
echo       A股招股书财务数据提取工具 (Web版)
echo ========================================================
echo.

REM Check Python
python --version >nul 2>&1
if %errorlevel% neq 0 goto NO_PYTHON

REM Check Venv
if exist venv goto ACTIVATE_VENV

echo [信息] 正在创建虚拟环境...
python -m venv venv
if %errorlevel% neq 0 goto VENV_FAIL

:ACTIVATE_VENV
call venv\Scripts\activate

REM Check Dependencies
if exist venv\Lib\site-packages\installed_flag goto START_SERVER

echo [信息] 正在安装依赖...
pip install -r requirements.txt
if %errorlevel% neq 0 goto INSTALL_FAIL
type nul > venv\Lib\site-packages\installed_flag

:START_SERVER
echo.
echo [信息] 正在启动 launcher (引导程序)...
python src/launcher.py --web
if %errorlevel% neq 0 goto SERVER_CRASH
goto END

:NO_PYTHON
echo [错误] 未检测到 Python，或未添加到 PATH。
pause
exit /b 1

:VENV_FAIL
echo [错误] 创建虚拟环境失败。
pause
exit /b 1

:INSTALL_FAIL
echo [错误] 依赖安装失败。
pause
exit /b 1

:SERVER_CRASH
echo [错误] Web服务器异常退出。
pause
exit /b 1

:END
pause
