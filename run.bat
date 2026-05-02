@echo off
cd /d %~dp0

where python >nul 2>nul
if %errorlevel% neq 0 (
  echo Python not found. Please install Python 3.12 and run again.
  pause
  exit /b 1
)

for /f %%i in ('python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"') do set PYTHON_VERSION=%%i
if not "%PYTHON_VERSION%"=="3.12" (
  echo Python 3.12 is required for this project. Found Python %PYTHON_VERSION%.
  pause
  exit /b 1
)

if not exist .venv (
  python -m venv .venv
)

call .venv\Scripts\activate
pip install -r requirements.txt
if "%TASK_SOURCE_ROOT%"=="" set TASK_SOURCE_ROOT=%cd%\data
streamlit run app.py
pause
