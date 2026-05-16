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
python -m py_compile wsgi.py
python -m py_compile gfs_grid.py || echo "gfs_grid.py compile failed (continuing)"

echo "===== STEP IMPORT FLASK ONLY ====="
python - <<'PY'
print("before flask import", flush=True)
import flask
print("after flask import", flush=True)
PY

echo "===== STEP IMPORT WSGI MODULE ====="
python - <<'PY'
import sys
import traceback
import faulthandler

faulthandler.enable()

print("before from wsgi import app", flush=True)

try:
    from wsgi import app
    print("after from wsgi import app", flush=True)
    print("WSGI IMPORT OK", flush=True)
    print(app.url_map, flush=True)
except BaseException:
    print("WSGI IMPORT FAILED", flush=True)
    traceback.print_exc()
    sys.exit(1)
PY

echo "===== STARTING GUNICORN ====="
exec gunicorn wsgi:app \
  --bind "0.0.0.0:${PORT:-10000}" \
  --workers 1 \
  --threads 2 \
  --timeout 120 \
  --log-level debug \
  --access-logfile - \
  --error-logfile -
