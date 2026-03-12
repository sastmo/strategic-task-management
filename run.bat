@echo off
cd /d %~dp0

where python >nul 2>nul
if %errorlevel% neq 0 (
  echo Python not found. Please install Python 3.11+ and run again.
  pause
  exit /b 1
)

if not exist .venv (
  python -m venv .venv
)

call .venv\Scripts\activate
pip install -r requirements.txt
streamlit run app.py
pause