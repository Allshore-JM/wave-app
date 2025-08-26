# app.py
# Flask service for Wave App: fetch & parse GFS "bulls" files and return rows for table rendering.
# Compatible with gunicorn: `gunicorn app:app`
#
# Key behaviors:
# - Finds the newest available bulls file under the current or most recent GFS cycle.
# - Parses header flexibly (new/old variants) and extracts up to 6 swell groups.
# - Uses forecast hour ("hr" column) to compute timestamps: cycle_utc + timedelta(hours=hr).
#   -> Guarantees monotonic +1 hour progression without month-end jumps.
# - Converts Hs meters -> feet; labels rows in local time if lat/lon are provided (TimezoneFinder).
#
# Endpoints:
#   GET /api/health
#   GET /api/bull/<station>?lat=..&lon=..
#   GET /api/info
#
# Optional: Keep your existing frontend; it can call /api/bull/<station>[?lat=&lon=] to fill the table.

from __future__ import annotations

import os
import re
import math
import json
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, request, send_from_directory, Response

# Optional timezone handling; if not available the service will fall back to UTC cleanly.
try:
    import pytz  # type: ignore
except Exception:  # pragma: no cover
    pytz = None

try:
    from timezonefinder import TimezoneFinder  # type: ignore
except Exception:  # pragma: no cover
    TimezoneFinder = None  # type: ignore

# ------------------------------------------------------------
# Config
# ------------------------------------------------------------
BASE_NOMADS = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"
CANDIDATE_PATHS = [
    # Most common/modern layout
    "gfs.{ymd}/{hh}/wave/gridded/bulls.t{hh}z/{station}.bull",
    # Fallback older layouts (kept just in case)
    "gfs.{ymd}/{hh}/wave/bulls.t{hh}z/{station}.bull",
    "gfs.{ymd}/{hh}/wave/bulls.t{hh}z/bulls.{station}.bull",
]
HTTP_TIMEOUT = (6, 20)  # (connect, read)
M_TO_FT = 3.28084

# Stations whose values are expected in meters (the vast majority). If you find a feed in feet,
# you can add station -> "ft" here or detect via header text.
DEFAULT_HEIGHT_UNITS = "m"  # assume meters and convert; safe default for WW3 / GFS waves.

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)sZ %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("wave-app")

app = Flask(__name__)

# ------------------------------------------------------------
# Utilities
# ------------------------------------------------------------

def _tz_from_latlon(lat: Optional[float], lon: Optional[float]) -> str:
    """Best-effort timezone lookup. Returns IANA TZ name or 'UTC'."""
    if lat is None or lon is None:
        return "UTC"
    if TimezoneFinder is None:
        return "UTC"
    try:
        tf = TimezoneFinder()
        tzname = tf.timezone_at(lat=lat, lng=lon)
        if not tzname:
            return "UTC"
        return tzname
    except Exception as e:  # pragma: no cover
        logger.warning("Timezone lookup failed: %s", e)
        return "UTC"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _ymd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def _hh(dt: datetime) -> str:
    return dt.strftime("%H")


def _candidate_cycles(start: datetime, days_back: int = 2) -> List[Tuple[str, str, datetime]]:
    """
    Produce a list of (ymd, hh, cycle_dt_utc) to try, newest-first.
    We check today and up to 'days_back' prior days, across 18/12/06/00 cycles.
    """
    cycles = ["18", "12", "06", "00"]
    out: List[Tuple[str, str, datetime]] = []
    base_date = start
    for d in range(days_back + 1):
        date0 = (base_date - timedelta(days=d)).date()
        for hh in cycles:
            dt = datetime.combine(date0, datetime.min.time(), tzinfo=timezone.utc).replace(hour=int(hh))
            # Only include cycles that are not in the future vs now
            if dt <= start:
                out.append((_ymd(dt), hh, dt))
    # Sort by datetime desc (newest first)
    out.sort(key=lambda t: t[2], reverse=True)
    return out


def _http_exists(url: str) -> bool:
    try:
        # HEAD can be blocked; GET with small read is more robust here.
        r = requests.get(url, timeout=HTTP_TIMEOUT, stream=True, headers={"User-Agent": "wave-app/1.0"})
        try:
            if r.status_code == 200:
                return True
            return False
        finally:
            r.close()
    except requests.RequestException:
        return False


def _find_latest_bull_url(station: str) -> Tuple[str, datetime]:
    """
    Determine the newest available bulls URL for a station.
    Returns (url, cycle_dt_utc).
    Raises RuntimeError if nothing found.
    """
    now = _now_utc()
    for ymd, hh, cycle_dt in _candidate_cycles(now, days_back=3):
        for templ in CANDIDATE_PATHS:
            rel = templ.format(ymd=ymd, hh=hh, station=station)
            url = f"{BASE_NOMADS}/{rel}"
            if _http_exists(url):
                logger.info("Using bulls file: %s", url)
                return url, cycle_dt
    raise RuntimeError("No bulls file found for station in the recent cycles.")


def _download_text(url: str) -> str:
    r = requests.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent": "wave-app/1.0"})
    r.raise_for_status()
    # NOMADS directory serves plain text; ensure str
    r.encoding = r.encoding or "utf-8"
    return r.text


# ------------------------------------------------------------
# Parsing
# ------------------------------------------------------------

_HR_COL_NAMES = {"hr", "fhr", "hour"}
_HS_NAMES = {"hs", "swh", "wvht", "hsig", "htsgw", "hm0"}
_TP_NAMES = {"tp", "tpp", "tps", "per", "pp1d", "tm1"}
_DIR_NAMES = {"dir", "dp", "dd", "wvdir", "peakdir", "mdc", "mdcd", "pcdir"}

def _normalize_token(tok: str) -> str:
    return re.sub(r"[^a-z0-9]", "", tok.lower())


def _find_header_and_columns(lines: List[str]) -> Tuple[int, Dict[str, int], List[Tuple[int, int, int]]]:
    """
    Identify:
      - header line index
      - indices for 'hr' (forecast hour) and possibly a combined Hs column ('wvht'/etc.)
      - swell group triples [(hs_idx, tp_idx, dir_idx), ...] up to 6 groups
    Returns (header_index, single_cols, swell_triples)
    """
    header_idx = -1
    header_tokens: List[str] = []

    # Find the header row: look for something starting with 'hr' (case-insensitive).
    for i, line in enumerate(lines[:120]):  # header is early
        toks = [t for t in re.split(r"\s+", line.strip()) if t]
        if not toks:
            continue
        t0 = _normalize_token(toks[0])
        if t0 in _HR_COL_NAMES:
            header_idx = i
            header_tokens = [_normalize_token(t) for t in toks]
            break

    if header_idx == -1:
        # Fallback: scan for any row that at least contains 'hr' somewhere.
        for i, line in enumerate(lines[:160]):
            toks = [t for t in re.split(r"\s+", line.strip()) if t]
            flat = [_normalize_token(t) for t in toks]
            if any(tok in _HR_COL_NAMES for tok in flat):
                header_idx = i
                header_tokens = flat
                break

    if header_idx == -1:
        raise ValueError("Could not locate header line with 'hr' column.")

    # Map single columns
    single_cols: Dict[str, int] = {}
    for j, tok in enumerate(header_tokens):
        if tok in _HR_COL_NAMES and "hr" not in single_cols:
            single_cols["hr"] = j
        if tok in _HS_NAMES and "combined_hs" not in single_cols:
            single_cols["combined_hs"] = j

    # Find repeated swell triples (… SwH SwP SwD …)
    swell_triples: List[Tuple[int, int, int]] = []
    j = 0
    lt = len(header_tokens)
    while j < lt:
        tok = header_tokens[j]
        if tok in _HS_NAMES and j + 2 < lt:
            t1 = header_tokens[j + 1]
            t2 = header_tokens[j + 2]
            if (t1 in _TP_NAMES) and (t2 in _DIR_NAMES):
                swell_triples.append((j, j + 1, j + 2))
                j += 3
                continue
        j += 1

    # Cap at 6 swell groups for safety/compatibility
    swell_triples = swell_triples[:6]
    return header_idx, single_cols, swell_triples


def _safe_float(tok: str) -> Optional[float]:
    try:
        # Some files hide floats as ints; casting to float handles both
        return float(tok)
    except Exception:
        return None


def _compute_times_from_hr(cycle_dt_utc: datetime, hr_val: int) -> datetime:
    # hr is forecast hour offset from model cycle in hours (0..)
    return cycle_dt_utc + timedelta(hours=int(hr_val))


def _build_rows(
    lines: List[str],
    header_idx: int,
    single_cols: Dict[str, int],
    swell_triples: List[Tuple[int, int, int]],
    cycle_dt_utc: datetime,
    local_tz_name: str,
    height_units_hint: str = DEFAULT_HEIGHT_UNITS,
) -> Tuple[List[str], List[List[Optional[float]]]]:
    """
    Return (columns, rows). Columns format:
      ["Date (TZ)", "Time (TZ)", "S1 Hs (ft)","S1 Tp (s)","S1 Dir (°)", ..., "Combined Hs (ft)"]
    """
    if not swell_triples:
        # Ensure at least one triple; even if header didn't advertise, we still create placeholders.
        swell_triples = []

    # Columns
    tz_display = local_tz_name if local_tz_name else "UTC"
    cols: List[str] = ["Date (" + tz_display + ")", "Time (" + tz_display + ")"]
    for gi in range(max(1, len(swell_triples))):
        cols.extend([f"S{gi+1} Hs (ft)", f"S{gi+1} Tp (s)", f"S{gi+1} Dir (°)"])
    cols.append("Combined Hs (ft)")

    # Timezone setup
    local_tz = None
    if pytz and local_tz_name:
        try:
            local_tz = pytz.timezone(local_tz_name)
        except Exception:
            local_tz = pytz.UTC if pytz else None

    rows: List[List[Optional[float]]] = []

    # Data starts after header line
    start_idx = header_idx + 1
    # Skip any blank / dashed lines after header
    while start_idx < len(lines):
        if lines[start_idx].strip() and not lines[start_idx].strip().startswith("#"):
            break
        start_idx += 1

    for line in lines[start_idx:]:
        raw = line.strip()
        if not raw:
            continue
        toks = [t for t in re.split(r"\s+", raw) if t]
        if not toks:
            continue

        # Try to read hr column
        hr_idx = single_cols.get("hr", 0)
        if hr_idx >= len(toks):
            # malformed row; skip
            continue

        hr_val = _safe_float(toks[hr_idx])
        if hr_val is None:
            # Non-numeric -> likely we've reached the footers
            continue

        # Forecast UTC timestamp from cycle + hr offset
        forecast_dt_utc = _compute_times_from_hr(cycle_dt_utc, int(hr_val))

        # Convert to local tz if available
        dt_local = forecast_dt_utc
        if local_tz and pytz:
            dt_local = forecast_dt_utc.astimezone(local_tz)

        # Display-friendly date/time (no seconds)
        # We intentionally keep month/day/year to be readable across month boundaries
        try:
            date_str_local = dt_local.strftime("%a, %b %d, %Y")
        except Exception:
            # locale-independent fallback
            date_str_local = dt_local.strftime("%Y-%m-%d")
        time_str_local = dt_local.strftime("%I:%M %p").lstrip("0")

        row: List[Optional[float]] = [date_str_local, time_str_local]

        # Extract swell groups
        if swell_triples:
            for (hs_i, tp_i, di_i) in swell_triples:
                hs_m = _safe_float(toks[hs_i]) if hs_i < len(toks) else None
                tp_s = _safe_float(toks[tp_i]) if tp_i < len(toks) else None
                dir_v = _safe_float(toks[di_i]) if di_i < len(toks) else None
                if hs_m is not None and height_units_hint.lower().startswith("m"):
                    hs_val_ft = hs_m * M_TO_FT
                else:
                    hs_val_ft = hs_m  # already feet or unknown
                row.extend([
                    None if hs_val_ft is None else round(hs_val_ft, 2),
                    None if tp_s is None else round(tp_s, 2),
                    None if dir_v is None else round(dir_v, 0),
                ])
        else:
            # If we didn't find explicit triples, keep one group of Nones to satisfy columns
            row.extend([None, None, None])

        # Combined Hs (if any)
        combined_hs_ft: Optional[float] = None
        comb_idx = single_cols.get("combined_hs", -1)
        if 0 <= comb_idx < len(toks):
            comb_val = _safe_float(toks[comb_idx])
            if comb_val is not None:
                if height_units_hint.lower().startswith("m"):
                    combined_hs_ft = comb_val * M_TO_FT
                else:
                    combined_hs_ft = comb_val
        row.append(None if combined_hs_ft is None else round(combined_hs_ft, 2))

        rows.append(row)

    return cols, rows


def parse_bull_text(text: str, cycle_dt_utc: datetime, local_tz_name: str) -> Tuple[List[str], List[List[Optional[float]]]]:
    lines = text.splitlines()
    header_idx, single_cols, swell_triples = _find_header_and_columns(lines)
    cols, rows = _build_rows(
        lines=lines,
        header_idx=header_idx,
        single_cols=single_cols,
        swell_triples=swell_triples,
        cycle_dt_utc=cycle_dt_utc,
        local_tz_name=local_tz_name,
        height_units_hint=DEFAULT_HEIGHT_UNITS,
    )
    return cols, rows


# ------------------------------------------------------------
# Flask routes
# ------------------------------------------------------------

@app.get("/api/health")
def api_health() -> Response:
    return jsonify({"ok": True, "ts": _now_utc().isoformat()})


@app.get("/api/info")
def api_info() -> Response:
    return jsonify({
        "service": "wave-app-backend",
        "source": "GFS bulls.tHHz",
        "base": BASE_NOMADS,
        "notes": "Use /api/bull/<station>?lat=<lat>&lon=<lon> to fetch the latest table. Times are computed from forecast hour.",
    })


@app.get("/api/bull/<station>")
def api_bull(station: str) -> Response:
    """
    Fetch and parse the latest bulls file for a station.
      Optional query params:
        lat, lon -> to label rows in the station's local timezone
    """
    station = station.strip()
    if not station:
        return jsonify({"ok": False, "error": "Missing station id."}), 400

    # Optional lat/lon for timezone
    lat_q = request.args.get("lat")
    lon_q = request.args.get("lon")
    lat = float(lat_q) if lat_q is not None and lat_q != "" else None
    lon = float(lon_q) if lon_q is not None and lon_q != "" else None
    tz_name = _tz_from_latlon(lat, lon)

    try:
        url, cycle_dt_utc = _find_latest_bull_url(station)
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e), "station": station}), 404
    except Exception as e:
        logger.exception("Failed discovering bulls file.")
        return jsonify({"ok": False, "error": "Discovery failed.", "details": str(e)}), 500

    try:
        text = _download_text(url)
    except Exception as e:
        logger.exception("Failed downloading bulls file.")
        return jsonify({"ok": False, "error": "Download failed.", "details": str(e), "url": url}), 502

    try:
        columns, rows = parse_bull_text(text, cycle_dt_utc=cycle_dt_utc, local_tz_name=tz_name)
    except Exception as e:
        logger.exception("Parsing failed.")
        # Provide part of the first lines to help debug
        head = "\n".join(text.splitlines()[:40])
        return jsonify({
            "ok": False,
            "error": "Parsing failed.",
            "details": str(e),
            "url": url,
            "cycle_utc": cycle_dt_utc.isoformat(),
            "sample": head
        }), 500

    return jsonify({
        "ok": True,
        "station": station,
        "url": url,
        "cycle_utc": cycle_dt_utc.isoformat(),
        "timezone": tz_name,
        "columns": columns,
        "rows": rows
    })


# Optional root: serve an index.html if one exists next to the app, else a simple message.
@app.get("/")
def root() -> Response:
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        index_path = os.path.join(here, "index.html")
        if os.path.exists(index_path):
            return send_from_directory(here, "index.html")
    except Exception:
        pass
    return Response(
        "Wave App backend is running. Use /api/bull/<station>?lat=<lat>&lon=<lon> to get table data.",
        content_type="text/plain",
    )


# ------------------------------------------------------------
# Local dev
# ------------------------------------------------------------
if __name__ == "__main__":
    # For local testing: `python app.py`
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)
