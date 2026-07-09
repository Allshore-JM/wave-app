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
import json
import os
from pathlib import Path

from flask import Blueprint, abort, jsonify, request, send_file

# Live conditions module -- imported defensively so a failure here can NEVER take down the
# (already-in-prod) static reef page. A missing/broken module -> /live returns no_data.
try:
    from reef_live import live_response as _live_response
except Exception as _live_import_err:  # noqa: BLE001
    import logging
    logging.getLogger(__name__).warning("reef_live unavailable, /live will return no_data: %r",
                                         _live_import_err)
    _live_response = None

bp = Blueprint("reef", __name__)

_HERE = Path(__file__).resolve().parent
# Env-overridable so the local preview server can point at bigwave/artifacts + web/reef.html
# and import THIS blueprint (one _key_ok, one SPOTS whitelist, one containment check) rather
# than re-implementing the routes. Defaults are the wave-app repo-root layout, unchanged.
REEF_ROOT = Path(os.environ.get("REEF_DATA_ROOT", _HERE / "reef_data")).resolve()
REEF_HTML = Path(os.environ.get("REEF_HTML", _HERE / "web" / "reef.html"))
_META_CACHE: dict = {}

ALLOWED = {"meta.json", "zones.json", "contours.geojson", "profiles.json",
           "hillshade.png", "report.html"}
SPOTS = {"himalayas", "laniakea"}
_MIME = {".geojson": "application/geo+json", ".json": "application/json",
         ".png": "image/png", ".html": "text/html"}


def _key_ok() -> bool:
    """REEF_KEY gates access. Unset -> FAIL CLOSED (404), never expose on misconfig. Compare as
    BYTES: hmac.compare_digest raises TypeError on a non-ASCII str, which would 500 (a recon
    oracle) on an attacker-supplied ?k=<unicode>. Bytes compare fails closed instead."""
    want = os.environ.get("REEF_KEY")
    if not want:
        return False
    return hmac.compare_digest(request.args.get("k", "").encode("utf-8", "ignore"),
                               want.encode("utf-8", "ignore"))


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


def _load_meta(spot: str) -> dict | None:
    if spot not in _META_CACHE:
        p = (REEF_ROOT / spot / "meta.json").resolve()
        if not p.is_file() or REEF_ROOT not in p.parents:
            _META_CACHE[spot] = None
        else:
            try:
                _META_CACHE[spot] = json.loads(p.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                _META_CACHE[spot] = None
    return _META_CACHE[spot]


def _live_no_data(spot: str, why: str) -> dict:
    return {"schema_version": 1, "spot_id": spot, "state": "no_data", "bin": None, "notes": [why]}


@bp.route("/reef/<spot>/live")
def reef_live_route(spot):
    """Live condition bin (or an honest degraded state). ALWAYS 200 (404 only for a bad key /
    unknown spot); never 5xx -- a malformed meta or a module fault degrades to no_data, it does
    not error. no-store, private + Surrogate-Control -- MUST NOT be edge-cached (age would freeze)."""
    if not _key_ok() or spot not in SPOTS:
        abort(404)
    meta = _load_meta(spot)
    if _live_response is None or meta is None:
        payload = _live_no_data(spot, "live conditions unavailable (module or config missing)")
    else:
        try:
            payload = _live_response(spot, meta)
        except Exception:  # noqa: BLE001 -- never 5xx; the panel degrades honestly
            payload = _live_no_data(spot, "live conditions temporarily unavailable")
    resp = jsonify(payload)
    resp.headers["Cache-Control"] = "no-store, private"
    resp.headers["Surrogate-Control"] = "no-store"        # defense vs a CDN misconfig re-serving
    resp.headers["Pragma"] = "no-cache"
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
