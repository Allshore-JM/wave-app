"""
WSGI entry point.

Imports the Flask `app` from app.py and attaches the GFS global grid overlay
routes from gfs_grid.py without modifying app.py itself.

Switching gunicorn from `app:app` to `wsgi:app` is the single deploy change
required. If gfs_grid fails to import (e.g. cfgrib not installed), the rest
of the app still serves — the GFS endpoints will report 503 with details
on /api/gfs/status.
"""

from app import app  # noqa: F401  re-exported as the WSGI callable

try:
    import gfs_grid
    gfs_grid.register_routes(app)
    print("[wsgi] GFS overlay routes registered on /api/gfs/*")
except Exception as exc:  # pragma: no cover - env dependent
    print(f"[wsgi] gfs_grid routes NOT registered: {exc!r}")


if __name__ == "__main__":
    # Local dev convenience: `python wsgi.py` is equivalent to `python app.py`
    # but with the GFS routes attached.
    app.run(debug=True)
