#!/usr/bin/env bash
set -euxo pipefail

echo "===== STARTUP DEBUG ====="
python --version
echo "PORT=${PORT:-NOT_SET}"
echo "PWD=$(pwd)"
echo "Files:"
ls -la

echo "===== PYTHON COMPILE CHECK ====="
python -m py_compile app.py

echo "===== FLASK APP IMPORT CHECK ====="
python - <<'PY'
import sys
import traceback

try:
    from app import app
    print("APP IMPORT OK")
    print(app.url_map)
except Exception:
    print("APP IMPORT FAILED")
    traceback.print_exc()
    sys.exit(1)
PY

echo "===== STARTING GUNICORN ====="
exec gunicorn app:app \
  --bind "0.0.0.0:${PORT:-10000}" \
  --workers 1 \
  --threads 2 \
  --timeout 120 \
  --log-level debug \
  --access-logfile - \
  --error-logfile -
