"""
WSGI entry point for the test deployment (wave-app-clean.onrender.com).

This deploy hosts the buoy-app Phase 1 review build (map mouse-hang fix);
it does NOT include the GFS overlay routes. We re-export the Flask ``app``
from app.py so the test service starts whether its Render start command
targets ``app:app`` (via start.sh) or ``wsgi:app``.
"""

from app import app  # noqa: F401  re-exported as the WSGI callable


if __name__ == "__main__":
    app.run(debug=True)
