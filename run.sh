#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

if command -v python3 >/dev/null 2>&1; then
  PYTHON=python3
elif command -v python >/dev/null 2>&1; then
  PYTHON=python
else
  echo "Python not found. Please install Python 3.11+ and run again."
  exit 1
fi

if [ ! -d ".venv" ]; then
  "$PYTHON" -m venv .venv
fi

source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py