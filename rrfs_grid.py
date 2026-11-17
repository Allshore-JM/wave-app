"""NOAA RRFS grid geometry, shared by the RRFS job (tools/model_frames/rrfs.py) and the site (plan section 99).

Pure numpy, no network, no eccodes. The four fine RRFS grids are PINNED here exactly as their GRIB2 headers gave them on
2026-11-17 (shapeOfTheEarth 6 = a sphere of radius 6,371,229 m; scanningMode 64 = row 0 is the SOUTH row, columns run
east). A changed NOAA grid must fail loud (check_geometry), never be read with stale numbers.

- forward(grid, lat, lon) -> (fi, fj): fractional column / row of a point (arrays welcome).
- cell(grid, lat, lon) -> (j, i) | None: the nearest node, None outside the grid or on its edge row / column.
- rotate(grid, u, v, lon) -> (u, v): RRFS winds are GRID-RELATIVE (uvRelativeToGrid = 1); this turns them to
  earth-relative (east, north). Mercator needs nothing. Lambert and polar stereographic turn by a = n * (lon - LoV),
  sign checked in plan section 99 step 1 against GFS over open ocean (CONUS median |diff| 6.3 deg rotated vs 30.6 with
  the opposite sign).
"""
import math

import numpy as np

R = 6371229.0   # shapeOfTheEarth 6

# Keys and values exactly as eccodes reads them (the check_geometry pin).
GRIDS = {
    "hi": {"gridType": "mercator", "Nx": 321, "Ny": 225, "lat1": 18.072699, "lon1": 198.474999, "LaD": 20.0,
           "Dx": 2500.0, "Dy": 2500.0},
    "pr": {"gridType": "mercator", "Nx": 544, "Ny": 310, "lat1": 15.0, "lon1": 284.5, "LaD": 20.0,
           "Dx": 2500.0, "Dy": 2500.0},
    "conus": {"gridType": "lambert", "Nx": 1799, "Ny": 1059, "lat1": 21.138123, "lon1": 237.280472, "LaD": 38.5,
              "LoV": 262.5, "Latin1": 38.5, "Latin2": 38.5, "Dx": 3000.0, "Dy": 3000.0},
    "ak": {"gridType": "polar_stereographic", "Nx": 1649, "Ny": 1105, "lat1": 40.53, "lon1": 181.429, "LaD": 60.0,
           "LoV": 210.0, "Dx": 2976.0, "Dy": 2976.0},
}
ORDER = ("hi", "pr", "conus", "ak")           # the first grid that holds a point serves it: CONUS and Alaska OVERLAP
                                              # off Washington and British Columbia (e.g. 48.4 N 124.7 W is in both); CONUS
                                              # wins there. Every user of these grids (tables, points, overlay) must use locate()
SCANNING_MODE = 64
SHAPE_OF_THE_EARTH = 6

# eccodes key -> our key, for check_geometry
_ECC_KEYS = {"gridType": "gridType", "Nx": "Nx", "Ny": "Ny", "latitudeOfFirstGridPointInDegrees": "lat1",
             "longitudeOfFirstGridPointInDegrees": "lon1", "LaDInDegrees": "LaD", "LoVInDegrees": "LoV",
             "Latin1InDegrees": "Latin1", "Latin2InDegrees": "Latin2", "DxInMetres": "Dx", "DyInMetres": "Dy"}
_ORIENTATION_KEY = "orientationOfTheGridInDegrees"       # polar stereo: eccodes names LoV this way


class GeometryError(ValueError):
    """A record's grid is not the pinned one (NOAA changed the grid, or the wrong file)."""


def _wrap180(x):
    return (np.asarray(x, dtype=float) + 180.0) % 360.0 - 180.0


def _lambert_n(g):
    return math.sin(math.radians(g["Latin1"]))


def _xy(name, lat, lon):
    """Projected metres (x east, y north) of lat/lon on the grid's plane, origin chosen per projection."""
    g = GRIDS[name]
    phi = np.radians(np.asarray(lat, dtype=float))
    lon = np.asarray(lon, dtype=float)
    t = g["gridType"]
    if t == "mercator":
        k = R * math.cos(math.radians(g["LaD"]))
        x = k * np.radians(_wrap180(lon - g["lon1"]))           # relative to the first column: no seam at 180
        y = k * np.log(np.tan(math.pi / 4 + phi / 2))
        return x, y
    if t == "lambert":
        n = _lambert_n(g)
        p1 = math.radians(g["Latin1"])
        F = math.cos(p1) * math.tan(math.pi / 4 + p1 / 2) ** n / n
        rho = R * F / np.tan(math.pi / 4 + phi / 2) ** n
        th = n * np.radians(_wrap180(lon - g["LoV"]))
        return rho * np.sin(th), -rho * np.cos(th)
    if t == "polar_stereographic":
        rho = R * (1 + math.sin(math.radians(g["LaD"]))) * np.tan(math.pi / 4 - phi / 2)
        th = np.radians(_wrap180(lon - g["LoV"]))
        return rho * np.sin(th), -rho * np.cos(th)
    raise GeometryError(f"unknown gridType {t}")


_ORIGIN = {}


def _origin(name):
    if name not in _ORIGIN:
        g = GRIDS[name]
        x0, y0 = _xy(name, g["lat1"], g["lon1"])
        _ORIGIN[name] = (float(x0), float(y0))
    return _ORIGIN[name]


def forward(name, lat, lon):
    """(fi, fj): fractional column and row (row 0 = south) of lat/lon on grid `name`."""
    g = GRIDS[name]
    x, y = _xy(name, lat, lon)
    x0, y0 = _origin(name)
    return (x - x0) / g["Dx"], (y - y0) / g["Dy"]


def cell(name, lat, lon):
    """(j, i) of the nearest node of grid `name`, or None outside it or on its outer row / column (a cell there has
    no neighbours on one side: treated as outside, so a point at the very edge falls back to GFS)."""
    g = GRIDS[name]
    if not (-90.0 < lat < 90.0) or not math.isfinite(lat) or not math.isfinite(lon):
        return None
    fi, fj = forward(name, lat, lon)
    i, j = int(round(float(fi))), int(round(float(fj)))
    if 0 < i < g["Nx"] - 1 and 0 < j < g["Ny"] - 1:
        return j, i
    return None


def locate(lat, lon):
    """(grid name, j, i) of the first fine grid holding lat/lon, or None."""
    for name in ORDER:
        c = cell(name, lat, lon)
        if c is not None:
            return name, c[0], c[1]
    return None


def rotation_angle(name, lon):
    """Radians to turn grid-relative winds to earth-relative at longitude `lon` (0 for Mercator)."""
    g = GRIDS[name]
    t = g["gridType"]
    if t == "mercator":
        return np.zeros_like(np.asarray(lon, dtype=float))
    n = _lambert_n(g) if t == "lambert" else 1.0
    return n * np.radians(_wrap180(np.asarray(lon, dtype=float) - g["LoV"]))


def rotate(name, u, v, lon):
    """Earth-relative (u east, v north) from grid-relative u, v at longitude lon (arrays of one shape welcome)."""
    a = rotation_angle(name, lon)
    c, s = np.cos(a), np.sin(a)
    u = np.asarray(u, dtype=float)
    v = np.asarray(v, dtype=float)
    return c * u + s * v, -s * u + c * v


def speed_dir(u, v):
    """Speed (m/s) and the direction the wind blows FROM (deg, 0-360) of earth-relative u, v."""
    u = np.asarray(u, dtype=float)
    v = np.asarray(v, dtype=float)
    return np.hypot(u, v), (np.degrees(np.arctan2(-u, -v)) + 360.0) % 360.0


def check_geometry(name, meta):
    """Raise GeometryError unless the eccodes keys in `meta` match the pin of grid `name` (and scanning / earth)."""
    g = GRIDS[name]
    bad = []
    if meta.get("scanningMode") != SCANNING_MODE:
        bad.append(f"scanningMode {meta.get('scanningMode')}")
    if meta.get("shapeOfTheEarth") != SHAPE_OF_THE_EARTH:
        bad.append(f"shapeOfTheEarth {meta.get('shapeOfTheEarth')}")
    seen = dict(meta)
    if g["gridType"] == "polar_stereographic" and "LoVInDegrees" not in seen and _ORIENTATION_KEY in seen:
        seen["LoVInDegrees"] = seen[_ORIENTATION_KEY]
    for ek, k in _ECC_KEYS.items():
        if k not in g:
            continue
        got = seen.get(ek)
        want = g[k]
        if isinstance(want, str) or isinstance(want, int):
            ok = got == want
        else:
            ok = got is not None and abs(float(got) - want) <= 1e-5
        if not ok:
            bad.append(f"{ek} {got!r} != {want!r}")
    if bad:
        raise GeometryError(f"RRFS grid {name}: " + "; ".join(bad))


def self_check(name, lats, lons, sample=1000, tol=0.05, seed=99):
    """Max |forward(node) - node index| over `sample` random nodes of eccodes' latitude / longitude arrays (row 0 =
    south, as eccodes gives them for scanningMode 64); GeometryError above `tol` cells."""
    lats = np.asarray(lats, dtype=float)
    lons = np.asarray(lons, dtype=float)
    ny, nx = lats.shape
    rng = np.random.default_rng(seed)
    js = rng.integers(0, ny, sample)
    is_ = rng.integers(0, nx, sample)
    fi, fj = forward(name, lats[js, is_], lons[js, is_])
    err = float(max(np.max(np.abs(fi - is_)), np.max(np.abs(fj - js))))
    if not err <= tol:
        raise GeometryError(f"RRFS grid {name}: projection self-check off by {err:.4f} cells (> {tol})")
    return err
