"""Tide at the spot, in the DEM's LMSL datum, from NOAA CO-OPS.

Station 1612668 (Haleiwa) is a SUBORDINATE station of Honolulu 1612340: it has NO harmonic
series, so `datum=MSL` and 6-min intervals both error. But NOAA already publishes the reduced
hi/lo PREDICTIONS at `datum=MLLW&interval=hilo` (offsets already applied: HW -62 min, LW -125
min, height x0.8). We consume those and cosine-interpolate between extremes (measured vs NOAA's
own 6-min harmonic at Honolulu: mean 0.7 cm, p95 2.0 cm -- well inside a 40 cm tide bin).

Datum tie: the CUDEM DEM is LMSL. CO-OPS surveyed datums give MSL - MLLW = 0.2835 m at 1612668,
so tide_lmsl = h_mllw - 0.2835. NOAA's PREDICTION offsets imply a ~0.8x range (a smaller tidal
range than the surveyed datums), leaving a ~+-0.09 m systematic ambiguity we cannot resolve from
the API -- published, not hidden. Total 1-sigma ~0.10 m ~= 27% of a tide bin.

Tide is a bin AXIS: if it is unavailable the bin cannot be resolved (state no_data). NEVER
substitute 0.0 (that lands in the MID bin) and NEVER silently proxy Honolulu (62-125 min phase
error). Vendored VERBATIM into the wave-app repo root.
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import requests

COOPS = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
MSL_MINUS_MLLW_M = 0.2835          # CO-OPS surveyed datums, 1612668, epoch 1983-2001
TIDE_UNCERTAINTY_M = 0.10          # RSS of interpolation + datum-tie + amplitude ambiguity
REF_STATION = "1612340"            # Honolulu, the reference station (offsets already applied)


def fetch_tide_hilo(session: requests.Session, station: str, now_epoch: int,
                    timeout=(3.05, 6.0)) -> list[tuple]:
    """+-36 h of hi/lo predictions (MLLW) around `now_epoch`. Returns [(epoch, h_mllw_m, type)]
    sorted by time. Raises on a CO-OPS error body or empty response."""
    now = datetime.fromtimestamp(now_epoch, timezone.utc)
    params = {"product": "predictions", "application": "bigwave-reef", "datum": "MLLW",
              "interval": "hilo", "station": station, "time_zone": "gmt", "units": "metric",
              "format": "json",
              "begin_date": (now - timedelta(hours=36)).strftime("%Y%m%d %H:%M"),
              "end_date": (now + timedelta(hours=36)).strftime("%Y%m%d %H:%M")}
    j = session.get(COOPS, params=params, timeout=timeout).json()
    if "error" in j:
        raise RuntimeError(f"CO-OPS {station}: {j['error'].get('message', j['error'])}")
    preds = j.get("predictions", [])
    if not preds:
        raise RuntimeError(f"CO-OPS {station}: empty predictions")
    out = []
    for p in preds:
        try:   # skip a malformed row rather than fail the whole fetch
            t = datetime.strptime(p["t"], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            v = float(p["v"])
        except (KeyError, ValueError, TypeError):
            continue
        out.append((int(t.timestamp()), v, p.get("type", "")))
    out.sort()
    return out


_MAX_EXTREME_GAP_S = 11 * 3600   # consecutive tidal extremes are ~6 h apart; a bigger gap or a
#                                  same-type pair (H->H / L->L) means a MISSING extreme -> refuse


def tide_lmsl_at(hilo: list[tuple], epoch: int, msl_minus_mllw: float = MSL_MINUS_MLLW_M):
    """Cosine-interpolate the MLLW hi/lo series at `epoch`, convert to LMSL. Returns
    (tide_lmsl_m, h_mllw_m), or (None, None) if `epoch` is not bracketed by two ALTERNATING
    extremes within a plausible gap -- interpolating across a missing extreme gives a wrong
    tide (measured 0.13 m > the stated 0.10 m uncertainty), so we refuse -> honest no_data."""
    for (t1, h1, ty1), (t2, h2, ty2) in zip(hilo, hilo[1:]):
        if t1 <= epoch <= t2 and t2 > t1:
            if (t2 - t1) > _MAX_EXTREME_GAP_S or (ty1 and ty2 and ty1 == ty2):
                return None, None      # missing extreme / malformed series
            frac = (epoch - t1) / (t2 - t1)
            h = h1 + (h2 - h1) * (1.0 - math.cos(math.pi * frac)) / 2.0   # harmonic between extremes
            return round(h - msl_minus_mllw, 3), round(h, 3)
    return None, None


def tide_lmsl(session: requests.Session, station: str, now_epoch: int, timeout=(3.05, 6.0)) -> dict:
    """Full tide reading at `now_epoch`: {tide_m (LMSL), h_mllw_m, uncertainty_m, station, ...}.
    Raises on failure -- the caller renders state no_data (tide is a bin axis)."""
    hilo = fetch_tide_hilo(session, station, now_epoch, timeout)
    tide_m, h_mllw = tide_lmsl_at(hilo, now_epoch)
    if tide_m is None:
        raise RuntimeError(f"CO-OPS {station}: now not bracketed by hi/lo extremes")
    return {"tide_m": tide_m, "h_mllw_m": h_mllw, "datum": "LMSL",
            "uncertainty_m": TIDE_UNCERTAINTY_M, "station": station,
            "reference_station": REF_STATION, "msl_minus_mllw_m": MSL_MINUS_MLLW_M,
            "method": "cosine_between_extremes@MLLW_hilo"}
