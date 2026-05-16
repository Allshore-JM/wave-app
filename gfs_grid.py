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
# cfgrib + xarray + numpy are heavy and may not be installed in every
# environment yet. We import them lazily and fall back to a clear error so
# the rest of the app keeps running.

try:
    import numpy as np  # type: ignore
    import xarray as xr  # type: ignore
    _GRIB_OK = True
    _GRIB_IMPORT_ERROR: str | None = None
except Exception as exc:  # pragma: no cover - env dependent
    np = None  # type: ignore
    xr = None  # type: ignore
    _GRIB_OK = False
    _GRIB_IMPORT_ERROR = repr(exc)


# ----------------------------------------------------------------------------
# Constants and configuration
# ----------------------------------------------------------------------------

# NOMADS gribfilter endpoints
GFS_ATMOS_FILTER = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
GFS_WAVE_FILTER = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfswave.pl"

# Native resolutions on NOMADS:
#   gfs_0p25  : 0.25 deg atmospheric grid (1440 x 721)
#   gfswave   : 0.25 deg wave grid (global file is gfswave.tCCz.global.0p25.fFFF.grib2)
#
# 0.25 deg over the whole globe would be ~1 million cells per layer. That's
# too much to ship to a browser. We downsample to a coarser display grid.
#
# 1.0 deg → 360 x 181 = 65,160 cells. ~250 KB JSON, fast and plenty for the
# Windy-style global view. Higher zooms can be added later via region-specific
# subsets if we need more detail.
DEFAULT_DOWNSAMPLE_DEG = 1.0

# Each forecast cycle is run 4x/day at 00, 06, 12, 18 UTC. The wave model
# publishes hourly steps out to ~120 h, then 3-hourly. The atmos GFS publishes
# hourly to f120 in gfs_0p25_1hr or 3-hourly in gfs_0p25. We default to f000
# (analysis) for the first version and add forecast stepping later.
DEFAULT_FORECAST_HOUR = 0

# How long to cache decoded layers in memory (seconds). GFS publishes every
# 6 hours, so 90 minutes is comfortably under that.
CACHE_TTL_SECONDS = 90 * 60

# Cap how large a frontend-facing payload can be. Even at 1 degree we are well
# under this, but a defensive guard avoids OOMing a render dyno if someone
# requests a finer grid.
MAX_CELLS = 200_000

# Network timeouts
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
# Run detection
# ----------------------------------------------------------------------------

def _latest_atmos_run() -> tuple[str, str] | tuple[None, None]:
    """Find the most recent GFS atmos run that has f000 published.

    Returns (yyyymmdd, hh) or (None, None) if nothing is available.
    """
    now = datetime.utcnow()
    for delta_day in (0, 1):
        check = now - timedelta(days=delta_day)
        yyyymmdd = check.strftime("%Y%m%d")
        for hh in ("18", "12", "06", "00"):
            test_url = (
                "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/"
                f"gfs.{yyyymmdd}/{hh}/atmos/gfs.t{hh}z.pgrb2.0p25.f000"
            )
            try:
                resp = requests.head(test_url, timeout=10, allow_redirects=True)
                if resp.status_code == 200:
                    return yyyymmdd, hh
            except Exception:
                continue
    return None, None


def _latest_wave_run() -> tuple[str, str] | tuple[None, None]:
    """Find the most recent GFS-Wave run that has the global f000 file published.

    Returns (yyyymmdd, hh) or (None, None) if nothing is available.
    """
    now = datetime.utcnow()
    for delta_day in (0, 1):
        check = now - timedelta(days=delta_day)
        yyyymmdd = check.strftime("%Y%m%d")
        for hh in ("18", "12", "06", "00"):
            test_url = (
                "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/"
                f"gfs.{yyyymmdd}/{hh}/wave/gridded/"
                f"gfswave.t{hh}z.global.0p25.f000.grib2"
            )
            try:
                resp = requests.head(test_url, timeout=10, allow_redirects=True)
                if resp.status_code == 200:
                    return yyyymmdd, hh
            except Exception:
                continue
    return None, None


# ----------------------------------------------------------------------------
# Gribfilter URL builders
# ----------------------------------------------------------------------------

def _build_atmos_wind_url(yyyymmdd: str, hh: str, fhr: int) -> str:
    """Subsetted URL for 10 m U/V wind at a single forecast hour."""
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
    """Subsetted URL for one or more wave variables at a single forecast hour.

    variables is a list like ["HTSGW"] or ["PERPW"].
    """
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
    """Download GRIB2 bytes from NOMADS. Returns raw bytes or raises."""
    resp = requests.get(url, timeout=NOMADS_TIMEOUT)
    resp.raise_for_status()
    body = resp.content
    if not body or len(body) < 200:
        # NOMADS sometimes returns an HTML error page with 200 OK
        raise RuntimeError(
            f"NOMADS returned a suspiciously small response ({len(body)} bytes). "
            "The requested file or variable may not be available yet."
        )
    return body


def _open_grib_bytes(grib_bytes: bytes) -> "xr.Dataset":
    """Write bytes to a temp file and open with cfgrib via xarray.

    cfgrib needs a file path. We write to a temp file in /tmp and clean up.
    """
    tmp = tempfile.NamedTemporaryFile(
        prefix="gfs_", suffix=".grib2", delete=False
    )
    try:
        tmp.write(grib_bytes)
        tmp.flush()
        tmp.close()
        ds = xr.open_dataset(
            tmp.name,
            engine="cfgrib",
            backend_kwargs={"indexpath": ""},  # skip writing a .idx sidecar
        )
        return ds
    finally:
        try:
            os.unlink(tmp.name)
        except Exception:
            pass


# ----------------------------------------------------------------------------
# Downsampling
# ----------------------------------------------------------------------------

def _stride_for_resolution(da_shape: tuple[int, int], target_deg: float, native_deg: float = 0.25) -> int:
    """Compute an integer stride that gets us close to target_deg resolution."""
    stride = max(1, int(round(target_deg / native_deg)))
    return stride


def _downsample(da: "xr.DataArray", stride: int) -> "xr.DataArray":
    """Coarsen by simple striding. Uses .isel which is fast and preserves coords."""
    # The grid has dims (latitude, longitude). Stride both.
    lat_name = "latitude" if "latitude" in da.dims else "lat"
    lon_name = "longitude" if "longitude" in da.dims else "lon"
    return da.isel({lat_name: slice(None, None, stride), lon_name: slice(None, None, stride)})


# ----------------------------------------------------------------------------
# Payload builders
# ----------------------------------------------------------------------------

def _build_wind_payload(u_da: "xr.DataArray", v_da: "xr.DataArray", *,
                        yyyymmdd: str, hh: str, fhr: int) -> list[dict[str, Any]]:
    """Build the wind-js / leaflet-velocity JSON payload.

    The format expects two records (one for U, one for V), each with a
    header describing the grid and a flat data array of values row-major
    starting at the top-left (north-west) corner.
    """
    # Coordinate names
    lat_name = "latitude" if "latitude" in u_da.dims else "lat"
    lon_name = "longitude" if "longitude" in u_da.dims else "lon"

    lats = u_da[lat_name].values.astype(float)
    lons = u_da[lon_name].values.astype(float)

    # The wind-js format expects:
    #   lo1 = west edge longitude (degrees, 0..360 or -180..180; we use 0..360)
    #   la1 = north edge latitude
    #   dx, dy = grid spacing in degrees
    #   nx, ny = grid width/height
    #
    # NOAA GFS publishes longitudes 0..360 ascending and latitudes 90..-90
    # descending. We normalise to that layout: rows top-to-bottom = north-to-south.

    # Ensure latitudes are descending (north -> south)
    if lats[0] < lats[-1]:
        u_da = u_da.reindex({lat_name: lats[::-1]})
        v_da = v_da.reindex({lat_name: lats[::-1]})
        lats = u_da[lat_name].values.astype(float)

    # Ensure longitudes are ascending starting near 0
    # GFS already gives us 0..359.75; just normalise if needed.
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

    # Replace NaNs with 0.0 so the frontend doesn't have to handle them.
    u_flat = np.where(np.isnan(u_vals), 0.0, u_vals).ravel().tolist()
    v_flat = np.where(np.isnan(v_vals), 0.0, v_vals).ravel().tolist()

    # Reference time (model run) and valid time
    ref_time = f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}T{hh}:00:00Z"
    valid_dt = datetime.strptime(f"{yyyymmdd}{hh}", "%Y%m%d%H") + timedelta(hours=fhr)
    valid_time = valid_dt.strftime("%Y-%m-%dT%H:00:00Z")

    base_header = {
        "parameterCategory": 2,        # Momentum
        "lo1": lo1,
        "la1": la1,
        "lo2": lo2,
        "la2": la2,
        "nx": int(nx),
        "ny": int(ny),
        "dx": dx,
        "dy": dy,
        "refTime": ref_time,
        "forecastTime": int(fhr),
        "validTime": valid_time,
    }

    u_record = {
        "header": {
            **base_header,
            "parameterNumber": 2,
            "parameterNumberName": "eastward_wind",
            "parameterUnit": "m s-1",
        },
        "data": u_flat,
    }
    v_record = {
        "header": {
            **base_header,
            "parameterNumber": 3,
            "parameterNumberName": "northward_wind",
            "parameterUnit": "m s-1",
        },
        "data": v_flat,
    }
    return [u_record, v_record]


def _build_scalar_payload(da: "xr.DataArray", *, name: str, units: str,
                          yyyymmdd: str, hh: str, fhr: int) -> dict[str, Any]:
    """Build a scalar grid payload (one value per cell) for raster overlays."""
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

    # Compute statistics ignoring NaNs (land masked)
    finite = vals[np.isfinite(vals)]
    vmin = float(finite.min()) if finite.size else 0.0
    vmax = float(finite.max()) if finite.size else 0.0

    # Round to 2 decimals to slim down the JSON. Replace NaN with null
    # via JSON serialization (json.dumps handles None).
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
        "name": name,
        "units": units,
        "refTime": ref_time,
        "forecastTime": int(fhr),
        "validTime": valid_time,
        "header": {
            "lo1": float(lons[0]),
            "la1": float(lats[0]),
            "lo2": float(lons[-1]),
            "la2": float(lats[-1]),
            "nx": int(nx),
            "ny": int(ny),
            "dx": float(abs(lons[1] - lons[0])) if nx > 1 else 1.0,
            "dy": float(abs(lats[0] - lats[1])) if ny > 1 else 1.0,
            "min": vmin,
            "max": vmax,
        },
        "data": cleaned,
    }


# ----------------------------------------------------------------------------
# Public fetchers
# ----------------------------------------------------------------------------

def _require_grib_stack() -> None:
    if not _GRIB_OK:
        raise RuntimeError(
            "GRIB stack unavailable. "
            "Install cfgrib + xarray + eccodes. "
            f"Import error: {_GRIB_IMPORT_ERROR}"
        )


def get_wind_layer(fhr: int = DEFAULT_FORECAST_HOUR,
                   resolution_deg: float = DEFAULT_DOWNSAMPLE_DEG) -> dict[str, Any]:
    """Return the wind layer payload for the requested forecast hour."""
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

    ds = _open_grib_bytes(grib_bytes)
    try:
        # Variable names from cfgrib for GFS U/V at 10 m: u10, v10
        u_var = "u10" if "u10" in ds.data_vars else "10u"
        v_var = "v10" if "v10" in ds.data_vars else "10v"
        if u_var not in ds.data_vars or v_var not in ds.data_vars:
            raise RuntimeError(
                f"GFS wind GRIB did not contain expected U/V variables. "
                f"Got: {list(ds.data_vars)}"
            )
        u_da = ds[u_var]
        v_da = ds[v_var]

        stride = _stride_for_resolution(u_da.shape, resolution_deg, native_deg=0.25)
        u_da = _downsample(u_da, stride)
        v_da = _downsample(v_da, stride)

        records = _build_wind_payload(u_da, v_da, yyyymmdd=yyyymmdd, hh=hh, fhr=fhr)
    finally:
        try:
            ds.close()
        except Exception:
            pass

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
    """Return the significant wave height (HTSGW) layer."""
    return _get_wave_scalar_layer("HTSGW", "htsgw", "m", fhr, resolution_deg, "waves")


def get_swell_period_layer(fhr: int = DEFAULT_FORECAST_HOUR,
                           resolution_deg: float = DEFAULT_DOWNSAMPLE_DEG) -> dict[str, Any]:
    """Return the primary peak wave period (PERPW) layer."""
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

    ds = _open_grib_bytes(grib_bytes)
    try:
        # cfgrib short names for these GRIB vars:
        #   HTSGW -> 'swh' (significant wave height) in newer eccodes,
        #            sometimes 'htsgw' depending on tables.
        #   PERPW -> 'perpw' or 'pp1d' depending on tables.
        candidates = {
            "HTSGW": ("swh", "htsgw"),
            "PERPW": ("perpw", "pp1d"),
        }.get(grib_var, ())
        da = None
        for c in candidates:
            if c in ds.data_vars:
                da = ds[c]
                break
        if da is None:
            # Fall back to whatever single variable came back
            data_vars = list(ds.data_vars)
            if len(data_vars) == 1:
                da = ds[data_vars[0]]
            else:
                raise RuntimeError(
                    f"Could not identify {grib_var} in wave GRIB. "
                    f"Got data_vars: {data_vars}"
                )

        stride = _stride_for_resolution(da.shape, resolution_deg, native_deg=0.25)
        da = _downsample(da, stride)
        payload_grid = _build_scalar_payload(
            da, name=name, units=units,
            yyyymmdd=yyyymmdd, hh=hh, fhr=fhr,
        )
    finally:
        try:
            ds.close()
        except Exception:
            pass

    payload = {
        "layer": layer_label,
        "model": "gfswave_0p25",
        "run": {"date": yyyymmdd, "hour": hh},
        "grid": payload_grid,
    }
    _cache_set(cache_key, payload)
    return payload


# ----------------------------------------------------------------------------
# Lightweight status / probe endpoint
# ----------------------------------------------------------------------------

def get_status() -> dict[str, Any]:
    """Quickly report whether the GRIB stack is importable and what the latest
    GFS runs look like. Cheap, safe to hit without doing a full GRIB fetch."""
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
    return info


# ----------------------------------------------------------------------------
# Flask route registration
# ----------------------------------------------------------------------------
# Exposed as a single register_routes(app) helper so app.py only needs to
# import this module and call it. This keeps app.py changes minimal.

def register_routes(app) -> None:
    """Register /api/gfs/* endpoints on the provided Flask app."""
    from flask import jsonify, request

    def _parse_query_args():
        try:
            fhr = int(request.args.get("fhr", "0"))
        except Exception:
            fhr = 0
        # GFS-Wave publishes hourly to ~120 h, 3-hourly beyond. Cap at 384.
        fhr = max(0, min(fhr, 384))
        try:
            resolution_deg = float(request.args.get("resolution", "1.0"))
        except Exception:
            resolution_deg = 1.0
        # Clamp resolution: 0.25 = native, 5.0 = very coarse.
        resolution_deg = max(0.25, min(resolution_deg, 5.0))
        return fhr, resolution_deg

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
