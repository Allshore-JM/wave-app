"""EUMETNET OPERA composite for the radar job (plan section 85): the ODIM HDF5 reader and the projection.

Anonymous object storage, CC BY 4.0, property rights EUMETNET:
  https://s3.waw3-1.cloudferro.com/openradar-24h/YYYY/MM/DD/OPERA/COMP/OPERA@YYYYMMDDTHHMM@0@DBZH.h5
every 5 minutes, about 4 minutes late (at most 14 seen), kept 24 h; a label ends a 10-minute MAX window. /dataset1/data1/data:
4400 rows x 3800 columns, gain 1, offset 0, nodata -9999000 (no radar), undetect -8888000 (radar, no echo), real values down to
about -33 dBZ. Lambert azimuthal equal-area on WGS84, 1000 m. forward() is Snyder's ellipsoidal form (no pyproj in the job's
environment); every read checks the file's own corner attributes against it, so a changed grid fails loud.
"""
import math

import numpy as np

BUCKET_URL = "https://s3.waw3-1.cloudferro.com/openradar-24h"
ROWS, COLS = 4400, 3800
PIXEL_M = 1000.0
LAT0, LON0, X0, Y0 = 55.0, 10.0, 1950000.0, -2100000.0
A_M, F = 6378137.0, 1.0 / 298.257223563
E2 = F * (2.0 - F)
E = math.sqrt(E2)
PROJ_PARTS = ("+proj=laea", "+lat_0=55", "+lon_0=10", "+x_0=1950000", "+y_0=-2100000")
CORNER_TOL_M = 1500.0                   # the corner attributes hold a few decimals of a degree, and an edge or a centre
MAX_BYTES = 24_000_000                  # a real frame is 1.4 - 1.7 MB


def url_of(when):
    return f"{BUCKET_URL}/{when:%Y/%m/%d}/OPERA/COMP/OPERA@{when:%Y%m%dT%H%M}@0@DBZH.h5"


def _q(sin_phi):
    return (1.0 - E2) * (sin_phi / (1.0 - E2 * sin_phi ** 2)
                         - np.log((1.0 - E * sin_phi) / (1.0 + E * sin_phi)) / (2.0 * E))


_QP = float(_q(np.float64(1.0)))
_RQ = A_M * math.sqrt(_QP / 2.0)
_S0 = math.sin(math.radians(LAT0))
_BETA0 = math.asin(float(_q(np.float64(_S0))) / _QP)
_D = A_M * (math.cos(math.radians(LAT0)) / math.sqrt(1.0 - E2 * _S0 ** 2)) / (_RQ * math.cos(_BETA0))
_SB0, _CB0 = math.sin(_BETA0), math.cos(_BETA0)


def forward(lat, lon):
    """Degrees -> grid metres (x east from the left edge, y north, negative below the top edge)."""
    lat, lon = np.asarray(lat, dtype=np.float64), np.asarray(lon, dtype=np.float64)
    sb = np.clip(_q(np.sin(np.radians(lat))) / _QP, -1.0, 1.0)
    cb = np.sqrt(1.0 - sb * sb)
    dl = np.radians(lon - LON0)
    cdl = np.cos(dl)
    b = _RQ * np.sqrt(2.0 / (1.0 + _SB0 * sb + _CB0 * cb * cdl))
    return X0 + b * _D * cb * np.sin(dl), Y0 + (b / _D) * (_CB0 * sb - _SB0 * cb * cdl)


def source_index(lat, lon):
    """The source pixel holding each point (flat index rows x cols), -1 outside the grid. Pixels are 1000 m squares from the
    top-left corner (0, 0), the corner attributes being the grid's outer corners."""
    x, y = forward(lat, lon)
    col = np.floor(x / PIXEL_M).astype(np.int64)
    row = np.floor(-y / PIXEL_M).astype(np.int64)
    inside = (col >= 0) & (col < COLS) & (row >= 0) & (row < ROWS)
    return np.where(inside, row * COLS + col, -1)


def _text(v):
    return v.decode("ascii", "replace") if isinstance(v, bytes) else str(v)


def read(path):
    """An OPERA DBZH file -> {values: float32 (4400, 3800) dBZ, NaN = no radar, -inf = radar without an echo; time: the label
    'YYYYMMDDHHMM' from the top group; radars: count}. A file of any other layout is a ValueError."""
    import h5py
    try:
        f = h5py.File(path, "r")
    except OSError as exc:
        raise ValueError(f"not an HDF5 file: {exc}") from None
    with f:
        for name in ("what", "where", "dataset1/data1/data", "dataset1/data1/what", "dataset1/what"):
            if name not in f:
                raise ValueError(f"{name} not found")
        where, what = f["where"].attrs, f["dataset1/data1/what"].attrs
        proj = _text(where.get("projdef", ""))
        for part in PROJ_PARTS:
            if part not in proj:
                raise ValueError(f"projdef {proj!r} lacks {part}")
        qty = _text(what.get("quantity", f["dataset1/what"].attrs.get("quantity", "")))
        if qty != "DBZH":
            raise ValueError(f"quantity {qty!r} is not DBZH")
        ds = f["dataset1/data1/data"]
        if ds.shape != (ROWS, COLS):
            raise ValueError(f"data shape {ds.shape} is not {(ROWS, COLS)}")
        if int(where.get("xsize", 0)) != COLS or int(where.get("ysize", 0)) != ROWS \
                or abs(float(where.get("xscale", 0)) - PIXEL_M) > 1e-6 or abs(float(where.get("yscale", 0)) - PIXEL_M) > 1e-6:
            raise ValueError("the grid size or the pixel size has changed")
        check_corners(where)
        try:
            gain, offset = float(what.get("gain", 1.0)), float(what.get("offset", 0.0))
            nodata, undetect = float(what["nodata"]), float(what["undetect"])
        except KeyError as exc:
            raise ValueError(f"{exc} not found") from None
        raw = ds[()]
        top = f["what"].attrs
        stamp = _text(top.get("date", "")) + _text(top.get("time", ""))[:4]
        nodes = _text(f["how"].attrs.get("nodes", "")) if "how" in f else ""
    vals = (raw * gain + offset).astype(np.float32)
    vals[raw == undetect] = -np.inf
    vals[raw == nodata] = np.nan
    return {"values": vals, "time": stamp, "radars": len([n for n in nodes.split(",") if n.strip()])}


def check_corners(where):
    """The file's four corner attributes against forward(): a moved grid is a ValueError."""
    expect = {"UL": (0.0, 0.0), "UR": (COLS * PIXEL_M, 0.0), "LL": (0.0, -ROWS * PIXEL_M), "LR": (COLS * PIXEL_M, -ROWS * PIXEL_M)}
    for name, (ex, ey) in expect.items():
        try:
            x, y = forward(float(where[f"{name}_lat"]), float(where[f"{name}_lon"]))
        except KeyError:
            raise ValueError(f"corner {name} is missing") from None
        if abs(float(x) - ex) > CORNER_TOL_M or abs(float(y) - ey) > CORNER_TOL_M:
            raise ValueError(f"corner {name} projects to ({float(x):.0f}, {float(y):.0f}), not ({ex:.0f}, {ey:.0f})")
