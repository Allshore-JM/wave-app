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

echo "===== STEP IMPORT FLASK ONLY ====="
python - <<'PY'
print("before flask import", flush=True)
import flask
print("after flask import", flush=True)
PY

echo "===== STEP IMPORT APP MODULE ====="
python - <<'PY'
import sys
import traceback
import faulthandler

faulthandler.enable()

print("before from app import app", flush=True)

try:
    from app import app
    print("after from app import app", flush=True)
    print("APP IMPORT OK", flush=True)
    print(app.url_map, flush=True)
except BaseException:
    print("APP IMPORT FAILED", flush=True)
    traceback.print_exc()
    sys.exit(1)
PY

echo "===== STARTING GUNICORN ====="
exec gunicorn app:app \
  --bind "0.0.0.0:${PORT:-10000}" \
  --workers 1 \
  --threads 4 \
  --timeout 120 \
  --log-level info \
  --access-logfile - \
  --error-logfile -
