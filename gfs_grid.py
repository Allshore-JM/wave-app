"""
GFS grid data fetcher for global map overlays.

Fetches subsetted GRIB2 data from NOAA NOMADS gribfilter, decodes via cfgrib,
downsamples to a manageable resolution, and returns JSON payloads suitable
for rendering as Leaflet canvas overlays (wind animation, wave height, swell
period).

Three data layers are exposed:
    - wind   : 10 m U/V components from gfs_0p25 (atmospheric model)
    - waves  : Significant wave height (HTSGW) from gfswave
    - period : Primary peak wave period (PERPW) from gfswave

All endpoints return JSON. The wind payload follows the
"wind-js / leaflet-velocity" convention so the frontend can drive the
particle layer directly. Scalar layers (wave height, period) return a flat
grid that the frontend renders as a coloured raster.

This module is designed to be safe to import even if cfgrib/xarray are not
installed. In that case the routes will respond with a 503 explaining the
limitation, instead of crashing the whole Flask app on startup.
"""

from __future__ import annotations

import gzip
import io
import logging
import os
import tempfile
import threading
import time
from datetime import datetime, timedelta
from typing import Any

import requests

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------
# Optional GRIB stack
# ----------------------------------------------------------------------------

try:
    import cfgrib  # type: ignore
    import numpy as np  # type: ignore
    import xarray as xr  # type: ignore
    _GRIB_OK = True
    _GRIB_IMPORT_ERROR: str | None = None
except Exception as exc:  # pragma: no cover - env dependent
    cfgrib = None  # type: ignore
    np = None  # type: ignore
    xr = None  # type: ignore
    _GRIB_OK = False
    _GRIB_IMPORT_ERROR = repr(exc)


# ----------------------------------------------------------------------------
# Constants and configuration
# ----------------------------------------------------------------------------

GFS_ATMOS_FILTER = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
GFS_WAVE_FILTER = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfswave.pl"

# Default downsample is 1 deg; the frontend explicitly requests 0.25 deg native
# resolution via the ?resolution= query argument for the Windy-style overlay.
DEFAULT_DOWNSAMPLE_DEG = 1.0
DEFAULT_FORECAST_HOUR = 0

CACHE_TTL_SECONDS = 90 * 60

# Cap how large a frontend-facing payload can be.
# Native 0.25 deg global = 1,440 x 721 = 1,038,240 cells.
# Bumped from 200,000 -> 1,200,000 so the frontend can request the full-
# resolution grid for crisp wave-height contours.
MAX_CELLS = 1_200_000

NOMADS_TIMEOUT = 60

# ----------------------------------------------------------------------------
# In-memory cache
# ----------------------------------------------------------------------------

_CACHE: dict[str, dict[str, Any]] = {}
_CACHE_LOCK = threading.Lock()


def _cache_get(key: str) -> Any | None:
    with _CACHE_LOCK:
        entry = _CACHE.get(key)
        if not entry:
            return None
        if (time.time() - entry["ts"]) > CACHE_TTL_SECONDS:
            _CACHE.pop(key, None)
            return None
        return entry["value"]


def _cache_set(key: str, value: Any) -> None:
    with _CACHE_LOCK:
        _CACHE[key] = {"ts": time.time(), "value": value}


# ----------------------------------------------------------------------------
# Run detection cache (separate from data cache, shorter TTL)
# ----------------------------------------------------------------------------

_RUN_CACHE_TTL_SECONDS = 10 * 60  # 10 minutes

_RUN_CACHE: dict[str, dict[str, Any]] = {}
_RUN_CACHE_LOCK = threading.Lock()


def _run_cache_get(key: str) -> Any | None:
    with _RUN_CACHE_LOCK:
        entry = _RUN_CACHE.get(key)
        if not entry:
            return None
        if (time.time() - entry["ts"]) > _RUN_CACHE_TTL_SECONDS:
            _RUN_CACHE.pop(key, None)
            return None
        return entry["value"]


def _run_cache_set(key: str, value: Any) -> None:
    with _RUN_CACHE_LOCK:
        _RUN_CACHE[key] = {"ts": time.time(), "value": value}


# ----------------------------------------------------------------------------
# Run detection (smart probe ordering)
# ----------------------------------------------------------------------------

def _smart_run_order(now: datetime) -> list[tuple[str, str]]:
    """Return (yyyymmdd, hh) pairs in most-likely-available order.

    GFS runs are published roughly 5.5 hours after the cycle start.  Given
    the current UTC hour we can predict which cycle is most likely online
    and try it first, falling back to earlier cycles / yesterday.

    Publication estimates:
        00z -> ~05:30 UTC    06z -> ~11:30 UTC
        12z -> ~17:30 UTC    18z -> ~23:30 UTC
    """
    cycles = [18, 12, 6, 0]
    pub_delay_hours = 5.5

    result: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    # Walk backwards from the most recent cycle that should be published.
    for delta_day in (0, 1):
        check = now - timedelta(days=delta_day)
        yyyymmdd = check.strftime("%Y%m%d")
        for cycle_hour in cycles:
            # Estimate when this cycle becomes available.
            pub_time = check.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(hours=cycle_hour + pub_delay_hours)
            if pub_time <= now:
                pair = (yyyymmdd, f"{cycle_hour:02d}")
                if pair not in seen:
                    seen.add(pair)
                    result.append(pair)

    # Ensure we always have fallback candidates (at least yesterday's cycles)
    # even if the time math produces an empty list.
    if not result:
        for delta_day in (0, 1):
            check = now - timedelta(days=delta_day)
            yyyymmdd = check.strftime("%Y%m%d")
            for cycle_hour in cycles:
                pair = (yyyymmdd, f"{cycle_hour:02d}")
                if pair not in seen:
                    seen.add(pair)
                    result.append(pair)

    return result


def _latest_atmos_run() -> tuple[str, str] | tuple[None, None]:
    cached = _run_cache_get("run:atmos")
    if cached is not None:
        return cached

    now = datetime.utcnow()
    for yyyymmdd, hh in _smart_run_order(now):
        test_url = (
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/"
            f"gfs.{yyyymmdd}/{hh}/atmos/gfs.t{hh}z.pgrb2.0p25.f000"
        )
        try:
            resp = requests.head(test_url, timeout=10, allow_redirects=True)
            if resp.status_code == 200:
                result = (yyyymmdd, hh)
                _run_cache_set("run:atmos", result)
                return result
        except Exception:
            continue
    return None, None


def _latest_wave_run() -> tuple[str, str] | tuple[None, None]:
    cached = _run_cache_get("run:wave")
    if cached is not None:
        return cached

    now = datetime.utcnow()
    for yyyymmdd, hh in _smart_run_order(now):
        test_url = (
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/"
            f"gfs.{yyyymmdd}/{hh}/wave/gridded/"
            f"gfswave.t{hh}z.global.0p25.f000.grib2"
        )
        try:
            resp = requests.head(test_url, timeout=10, allow_redirects=True)
            if resp.status_code == 200:
                result = (yyyymmdd, hh)
                _run_cache_set("run:wave", result)
                return result
        except Exception:
            continue
    return None, None


# ----------------------------------------------------------------------------
# Gribfilter URL builders
# ----------------------------------------------------------------------------

def _build_atmos_wind_url(yyyymmdd: str, hh: str, fhr: int) -> str:
    file_name = f"gfs.t{hh}z.pgrb2.0p25.f{fhr:03d}"
    params = {
        "dir": f"/gfs.{yyyymmdd}/{hh}/atmos",
        "file": file_name,
        "var_UGRD": "on",
        "var_VGRD": "on",
        "lev_10_m_above_ground": "on",
    }
    return _build_url(GFS_ATMOS_FILTER, params)


def _build_wave_url(yyyymmdd: str, hh: str, fhr: int, variables: list[str]) -> str:
    file_name = f"gfswave.t{hh}z.global.0p25.f{fhr:03d}.grib2"
    params: dict[str, str] = {
        "dir": f"/gfs.{yyyymmdd}/{hh}/wave/gridded",
        "file": file_name,
        "lev_surface": "on",
    }
    for v in variables:
        params[f"var_{v}"] = "on"
    return _build_url(GFS_WAVE_FILTER, params)


def _build_url(base: str, params: dict[str, str]) -> str:
    from urllib.parse import urlencode
    return f"{base}?{urlencode(params)}"


# ----------------------------------------------------------------------------
# GRIB fetch + decode
# ----------------------------------------------------------------------------

def _fetch_grib(url: str) -> bytes:
    resp = requests.get(url, timeout=NOMADS_TIMEOUT)
    resp.raise_for_status()
    body = resp.content
    if not body or len(body) < 200:
        raise RuntimeError(
            f"NOMADS returned a suspiciously small response ({len(body)} bytes). "
            "The requested file or variable may not be available yet."
        )
    return body


def _open_grib_bytes(grib_bytes: bytes) -> "list[xr.Dataset]":
    tmp = tempfile.NamedTemporaryFile(
        prefix="gfs_", suffix=".grib2", delete=False
    )
    try:
        tmp.write(grib_bytes)
        tmp.flush()
        tmp.close()
        datasets = cfgrib.open_datasets(tmp.name, indexpath="")
        if not datasets:
            raise RuntimeError("cfgrib.open_datasets returned no datasets")
        for ds in datasets:
            ds.load()
        return datasets
    finally:
        try:
            os.unlink(tmp.name)
        except Exception:
            pass


# ----------------------------------------------------------------------------
# Downsampling
# ----------------------------------------------------------------------------

def _stride_for_resolution(da_shape: tuple[int, int], target_deg: float, native_deg: float = 0.25) -> int:
    stride = max(1, int(round(target_deg / native_deg)))
    return stride


def _downsample(da: "xr.DataArray", stride: int) -> "xr.DataArray":
    lat_name = "latitude" if "latitude" in da.dims else "lat"
    lon_name = "longitude" if "longitude" in da.dims else "lon"
    return da.isel({lat_name: slice(None, None, stride), lon_name: slice(None, None, stride)})


# ----------------------------------------------------------------------------
# Payload builders
# ----------------------------------------------------------------------------

def _build_wind_payload(u_da: "xr.DataArray", v_da: "xr.DataArray", *,
                        yyyymmdd: str, hh: str, fhr: int) -> list[dict[str, Any]]:
    lat_name = "latitude" if "latitude" in u_da.dims else "lat"
    lon_name = "longitude" if "longitude" in u_da.dims else "lon"

    lats = u_da[lat_name].values.astype(float)
    lons = u_da[lon_name].values.astype(float)

    if lats[0] < lats[-1]:
        u_da = u_da.reindex({lat_name: lats[::-1]})
        v_da = v_da.reindex({lat_name: lats[::-1]})
        lats = u_da[lat_name].values.astype(float)

    if lons[0] > lons[-1]:
        u_da = u_da.reindex({lon_name: lons[::-1]})
        v_da = v_da.reindex({lon_name: lons[::-1]})
        lons = u_da[lon_name].values.astype(float)

    u_vals = u_da.values.astype("float32")
    v_vals = v_da.values.astype("float32")

    ny, nx = u_vals.shape
    if nx * ny > MAX_CELLS:
        raise RuntimeError(
            f"Wind payload would be {nx*ny} cells, exceeds MAX_CELLS={MAX_CELLS}"
        )

    dx = float(abs(lons[1] - lons[0])) if nx > 1 else 1.0
    dy = float(abs(lats[0] - lats[1])) if ny > 1 else 1.0
    lo1 = float(lons[0])
    la1 = float(lats[0])
    lo2 = float(lons[-1])
    la2 = float(lats[-1])

    u_flat = np.where(np.isnan(u_vals), 0.0, u_vals).ravel().tolist()
    v_flat = np.where(np.isnan(v_vals), 0.0, v_vals).ravel().tolist()

    ref_time = f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}T{hh}:00:00Z"
    valid_dt = datetime.strptime(f"{yyyymmdd}{hh}", "%Y%m%d%H") + timedelta(hours=fhr)
    valid_time = valid_dt.strftime("%Y-%m-%dT%H:00:00Z")

    base_header = {
        "parameterCategory": 2,
        "lo1": lo1, "la1": la1, "lo2": lo2, "la2": la2,
        "nx": int(nx), "ny": int(ny),
        "dx": dx, "dy": dy,
        "refTime": ref_time, "forecastTime": int(fhr), "validTime": valid_time,
    }

    u_record = {
        "header": {**base_header, "parameterNumber": 2,
                   "parameterNumberName": "eastward_wind", "parameterUnit": "m s-1"},
        "data": u_flat,
    }
    v_record = {
        "header": {**base_header, "parameterNumber": 3,
                   "parameterNumberName": "northward_wind", "parameterUnit": "m s-1"},
        "data": v_flat,
    }
    return [u_record, v_record]


def _build_scalar_payload(da: "xr.DataArray", *, name: str, units: str,
                          yyyymmdd: str, hh: str, fhr: int) -> dict[str, Any]:
    lat_name = "latitude" if "latitude" in da.dims else "lat"
    lon_name = "longitude" if "longitude" in da.dims else "lon"

    lats = da[lat_name].values.astype(float)
    lons = da[lon_name].values.astype(float)

    if lats[0] < lats[-1]:
        da = da.reindex({lat_name: lats[::-1]})
        lats = da[lat_name].values.astype(float)
    if lons[0] > lons[-1]:
        da = da.reindex({lon_name: lons[::-1]})
        lons = da[lon_name].values.astype(float)

    vals = da.values.astype("float32")
    ny, nx = vals.shape

    if nx * ny > MAX_CELLS:
        raise RuntimeError(
            f"Scalar payload would be {nx*ny} cells, exceeds MAX_CELLS={MAX_CELLS}"
        )

    finite = vals[np.isfinite(vals)]
    vmin = float(finite.min()) if finite.size else 0.0
    vmax = float(finite.max()) if finite.size else 0.0

    cleaned: list[float | None] = []
    for row in vals:
        for v in row:
            if np.isnan(v):
                cleaned.append(None)
            else:
                cleaned.append(round(float(v), 2))

    ref_time = f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}T{hh}:00:00Z"
    valid_dt = datetime.strptime(f"{yyyymmdd}{hh}", "%Y%m%d%H") + timedelta(hours=fhr)
    valid_time = valid_dt.strftime("%Y-%m-%dT%H:00:00Z")

    return {
        "name": name, "units": units,
        "refTime": ref_time, "forecastTime": int(fhr), "validTime": valid_time,
        "header": {
            "lo1": float(lons[0]), "la1": float(lats[0]),
            "lo2": float(lons[-1]), "la2": float(lats[-1]),
            "nx": int(nx), "ny": int(ny),
            "dx": float(abs(lons[1] - lons[0])) if nx > 1 else 1.0,
            "dy": float(abs(lats[0] - lats[1])) if ny > 1 else 1.0,
            "min": vmin, "max": vmax,
        },
        "data": cleaned,
    }


# ----------------------------------------------------------------------------
# Public fetchers
# ----------------------------------------------------------------------------

def _require_grib_stack() -> None:
    if not _GRIB_OK:
        raise RuntimeError(
            "GRIB stack unavailable. Install cfgrib + xarray + eccodes. "
            f"Import error: {_GRIB_IMPORT_ERROR}"
        )


def get_wind_layer(fhr: int = DEFAULT_FORECAST_HOUR,
                   resolution_deg: float = DEFAULT_DOWNSAMPLE_DEG) -> dict[str, Any]:
    _require_grib_stack()

    cache_key = f"wind:{fhr}:{resolution_deg}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    yyyymmdd, hh = _latest_atmos_run()
    if not yyyymmdd:
        raise RuntimeError("No recent GFS atmos run found on NOMADS.")

    url = _build_atmos_wind_url(yyyymmdd, hh, fhr)
    logger.info("Fetching GFS wind GRIB: %s", url)
    grib_bytes = _fetch_grib(url)

    datasets = _open_grib_bytes(grib_bytes)
    u_da = v_da = None
    for ds in datasets:
        try:
            u_var = next((v for v in ("u10", "10u") if v in ds.data_vars), None)
            v_var = next((v for v in ("v10", "10v") if v in ds.data_vars), None)
            if u_var and v_var:
                u_da = ds[u_var]
                v_da = ds[v_var]
                break
        finally:
            try:
                ds.close()
            except Exception:
                pass

    if u_da is None or v_da is None:
        all_vars = [list(ds.data_vars) for ds in datasets]
        raise RuntimeError(
            f"GFS wind GRIB did not contain expected U/V variables. "
            f"All datasets vars: {all_vars}"
        )

    try:
        stride = _stride_for_resolution(u_da.shape, resolution_deg, native_deg=0.25)
        u_da = _downsample(u_da, stride)
        v_da = _downsample(v_da, stride)
        records = _build_wind_payload(u_da, v_da, yyyymmdd=yyyymmdd, hh=hh, fhr=fhr)
    except Exception:
        raise

    payload = {
        "layer": "wind",
        "model": "gfs_0p25",
        "run": {"date": yyyymmdd, "hour": hh},
        "records": records,
    }
    _cache_set(cache_key, payload)
    return payload


def get_wave_height_layer(fhr: int = DEFAULT_FORECAST_HOUR,
                          resolution_deg: float = DEFAULT_DOWNSAMPLE_DEG) -> dict[str, Any]:
    return _get_wave_scalar_layer("HTSGW", "htsgw", "m", fhr, resolution_deg, "waves")


def get_swell_period_layer(fhr: int = DEFAULT_FORECAST_HOUR,
                           resolution_deg: float = DEFAULT_DOWNSAMPLE_DEG) -> dict[str, Any]:
    return _get_wave_scalar_layer("PERPW", "perpw", "s", fhr, resolution_deg, "period")


def _get_wave_scalar_layer(grib_var: str, name: str, units: str,
                           fhr: int, resolution_deg: float,
                           layer_label: str) -> dict[str, Any]:
    _require_grib_stack()

    cache_key = f"{layer_label}:{fhr}:{resolution_deg}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    yyyymmdd, hh = _latest_wave_run()
    if not yyyymmdd:
        raise RuntimeError("No recent GFS-Wave run found on NOMADS.")

    url = _build_wave_url(yyyymmdd, hh, fhr, [grib_var])
    logger.info("Fetching GFS-Wave GRIB (%s): %s", grib_var, url)
    grib_bytes = _fetch_grib(url)

    datasets = _open_grib_bytes(grib_bytes)
    candidates = {
        "HTSGW": ("swh", "htsgw", "Significant_height_of_combined_wind_waves_and_swell_surface"),
        "PERPW": ("perpw", "pp1d", "Primary_wave_mean_period_surface"),
    }.get(grib_var, ())

    da = None
    all_vars: list[list[str]] = []
    for ds in datasets:
        try:
            ds_vars = list(ds.data_vars)
            all_vars.append(ds_vars)
            if da is not None:
                continue
            for c in candidates:
                if c in ds.data_vars:
                    da = ds[c]
                    logger.info("Found %s as '%s' in dataset with vars %s", grib_var, c, ds_vars)
                    break
            if da is None and len(ds_vars) == 1:
                da = ds[ds_vars[0]]
                logger.info("Fallback: using only var '%s' for %s", ds_vars[0], grib_var)
        finally:
            if da is None:
                try:
                    ds.close()
                except Exception:
                    pass

    if da is None:
        raise RuntimeError(
            f"Could not identify {grib_var} in wave GRIB. "
            f"All dataset vars: {all_vars}. Candidates tried: {candidates}"
        )

    try:
        stride = _stride_for_resolution(da.shape, resolution_deg, native_deg=0.25)
        da = _downsample(da, stride)
        payload_grid = _build_scalar_payload(
            da, name=name, units=units, yyyymmdd=yyyymmdd, hh=hh, fhr=fhr,
        )
    except Exception:
        raise

    payload = {
        "layer": layer_label,
        "model": "gfswave_0p25",
        "run": {"date": yyyymmdd, "hour": hh},
        "grid": payload_grid,
    }
    _cache_set(cache_key, payload)
    return payload


def get_debug_info(fhr: int = 0) -> dict[str, Any]:
    _require_grib_stack()

    yyyymmdd, hh = _latest_wave_run()
    if not yyyymmdd:
        return {"error": "No recent wave run found on NOMADS"}

    url = _build_wave_url(yyyymmdd, hh, fhr, ["HTSGW"])
    grib_bytes = _fetch_grib(url)

    tmp = tempfile.NamedTemporaryFile(prefix="gfs_dbg_", suffix=".grib2", delete=False)
    result: dict[str, Any] = {
        "run": {"date": yyyymmdd, "hour": hh},
        "fhr": fhr, "url": url, "grib_size_bytes": len(grib_bytes),
    }
    try:
        tmp.write(grib_bytes)
        tmp.flush()
        tmp.close()
        try:
            dsets = cfgrib.open_datasets(tmp.name, indexpath="")
            ds_summaries = []
            for i, ds in enumerate(dsets):
                ds.load()
                vars_info = {}
                for vname in ds.data_vars:
                    da = ds[vname]
                    vals = da.values.ravel()
                    finite = vals[np.isfinite(vals)]
                    vars_info[vname] = {
                        "dims": list(da.dims),
                        "shape": list(da.shape),
                        "attrs": {k: str(v) for k, v in da.attrs.items()},
                        "n_total": int(vals.size),
                        "n_finite": int(finite.size),
                        "n_nan": int(vals.size - finite.size),
                        "min": round(float(finite.min()), 3) if finite.size else None,
                        "max": round(float(finite.max()), 3) if finite.size else None,
                        "sample_first5": [
                            (round(float(v), 3) if np.isfinite(v) else None)
                            for v in vals[:5]
                        ],
                    }
                ds_summaries.append({
                    "dataset_index": i, "data_vars": vars_info,
                    "coords": list(ds.coords.keys()),
                })
                ds.close()
            result["cfgrib_open_datasets"] = ds_summaries
        except Exception as e:
            result["cfgrib_open_datasets_error"] = str(e)
    finally:
        try:
            os.unlink(tmp.name)
        except Exception:
            pass
    return result


def get_status() -> dict[str, Any]:
    info: dict[str, Any] = {
        "grib_stack_available": _GRIB_OK,
        "grib_import_error": _GRIB_IMPORT_ERROR,
    }
    if _GRIB_OK:
        atmos = _latest_atmos_run()
        wave = _latest_wave_run()
        info["latest_atmos_run"] = {"date": atmos[0], "hour": atmos[1]} if atmos[0] else None
        info["latest_wave_run"] = {"date": wave[0], "hour": wave[1]} if wave[0] else None
    info["cache_keys"] = list(_CACHE.keys())
    info["cache_ttl_seconds"] = CACHE_TTL_SECONDS
    info["run_cache_keys"] = list(_RUN_CACHE.keys())
    info["run_cache_ttl_seconds"] = _RUN_CACHE_TTL_SECONDS
    return info


# ----------------------------------------------------------------------------
# Flask route registration
# ----------------------------------------------------------------------------

def register_routes(app) -> None:
    from flask import jsonify, request

    @app.after_request
    def _gzip_response(response):
        """Gzip JSON responses over 1 KB when the client supports it."""
        if (
            response.status_code == 200
            and "gzip" in request.headers.get("Accept-Encoding", "")
            and response.content_type
            and "application/json" in response.content_type
            and response.content_length
            and response.content_length > 1024
        ):
            data = response.get_data()
            compressed = gzip.compress(data, compresslevel=6)
            response.set_data(compressed)
            response.headers["Content-Encoding"] = "gzip"
            response.headers["Content-Length"] = len(compressed)
            response.headers["Vary"] = "Accept-Encoding"
        return response

    def _parse_query_args():
        try:
            fhr = int(request.args.get("fhr", "0"))
        except Exception:
            fhr = 0
        fhr = max(0, min(fhr, 384))
        try:
            resolution_deg = float(request.args.get("resolution", "1.0"))
        except Exception:
            resolution_deg = 1.0
        resolution_deg = max(0.25, min(resolution_deg, 5.0))
        return fhr, resolution_deg

    @app.route("/api/gfs/debug")
    def api_gfs_debug():
        if not _GRIB_OK:
            return jsonify({"error": "GRIB stack unavailable",
                            "detail": _GRIB_IMPORT_ERROR}), 503
        fhr, _ = _parse_query_args()
        try:
            return jsonify(get_debug_info(fhr=fhr))
        except Exception as exc:
            logger.exception("Debug endpoint failed")
            return jsonify({"error": str(exc)}), 500

    @app.route("/api/gfs/status")
    def api_gfs_status():
        try:
            return jsonify({"available": _GRIB_OK, **get_status()})
        except Exception as exc:
            return jsonify({"available": _GRIB_OK, "error": str(exc)}), 500

    @app.route("/api/gfs/wind")
    def api_gfs_wind():
        if not _GRIB_OK:
            return jsonify({"error": "GRIB stack unavailable",
                            "detail": _GRIB_IMPORT_ERROR}), 503
        fhr, resolution = _parse_query_args()
        try:
            return jsonify(get_wind_layer(fhr=fhr, resolution_deg=resolution))
        except Exception as exc:
            logger.exception("Wind layer failed")
            return jsonify({"error": str(exc), "layer": "wind", "fhr": fhr}), 500

    @app.route("/api/gfs/waves")
    def api_gfs_waves():
        if not _GRIB_OK:
            return jsonify({"error": "GRIB stack unavailable",
                            "detail": _GRIB_IMPORT_ERROR}), 503
        fhr, resolution = _parse_query_args()
        try:
            return jsonify(get_wave_height_layer(fhr=fhr, resolution_deg=resolution))
        except Exception as exc:
            logger.exception("Wave height layer failed")
            return jsonify({"error": str(exc), "layer": "waves", "fhr": fhr}), 500

    @app.route("/api/gfs/period")
    def api_gfs_period():
        if not _GRIB_OK:
            return jsonify({"error": "GRIB stack unavailable",
                            "detail": _GRIB_IMPORT_ERROR}), 503
        fhr, resolution = _parse_query_args()
        try:
            return jsonify(get_swell_period_layer(fhr=fhr, resolution_deg=resolution))
        except Exception as exc:
            logger.exception("Swell period layer failed")
            return jsonify({"error": str(exc), "layer": "period", "fhr": fhr}), 500
