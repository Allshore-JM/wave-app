"""Port-ready wave-app blueprint for the unlinked reef page (hardened per R3 review).

To install in wave-app (Allshore-JM/wave-app), on a `feat/reef-page` branch off origin/test:
  1. copy this file, `web/reef.html`, and `reef_data/<spot>/*` to the wave-app repo root
  2. add ONE line to app.py:
        from bigwave_reef import bp as reef_bp; app.register_blueprint(reef_bp)
  3. set env REEF_KEY on the Render service (page fails CLOSED -> 404 if unset)
  4. deploy to `test` first; access at /reef/himalayas?k=<REEF_KEY>

Security posture (R3): access-key gated on BOTH routes (fail-closed 404), spot + filename
exact-match whitelists, resolved-path containment check, noindex on every response,
Cache-Control on data. Additive: touches no existing routes (grep app.py for '/reef' first).
"""
from __future__ import annotations

import hmac
import os
from pathlib import Path

from flask import Blueprint, abort, request, send_file

bp = Blueprint("reef", __name__)

_HERE = Path(__file__).resolve().parent
REEF_ROOT = (_HERE / "reef_data").resolve()      # reef_data/<spot>/<artifact>
REEF_HTML = _HERE / "web" / "reef.html"

ALLOWED = {"meta.json", "zones.json", "contours.geojson", "profiles.json",
           "hillshade.png", "report.html"}
SPOTS = {"himalayas"}
_MIME = {".geojson": "application/geo+json", ".json": "application/json",
         ".png": "image/png", ".html": "text/html"}


def _key_ok() -> bool:
    """REEF_KEY gates access. Unset -> FAIL CLOSED (404), never expose on misconfig."""
    want = os.environ.get("REEF_KEY")
    if not want:
        return False
    return hmac.compare_digest(request.args.get("k", ""), want)


def _noindex(resp):
    resp.headers["X-Robots-Tag"] = "noindex, nofollow"
    return resp


@bp.route("/reef/<spot>")
def reef_page(spot):
    if not _key_ok() or spot not in SPOTS:
        abort(404)
    resp = send_file(REEF_HTML)
    resp.headers["Cache-Control"] = "no-store"
    return _noindex(resp)


@bp.route("/reef/<spot>/data/<name>")
def reef_data(spot, name):
    if not _key_ok() or spot not in SPOTS or name not in ALLOWED:
        abort(404)
    target = (REEF_ROOT / spot / name).resolve()
    if not target.is_file() or REEF_ROOT not in target.parents:
        abort(404)
    resp = send_file(target, mimetype=_MIME.get(target.suffix))
    resp.headers["Cache-Control"] = "private, max-age=3600"   # auth-gated: never shared-cacheable
    return _noindex(resp)
