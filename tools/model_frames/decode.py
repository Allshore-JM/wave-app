"""GRIB2 record -> float32 grid on the site's fixed geometry.

Output geometry (every consumer relies on this; the manifest repeats it):
  shape (721, 1440); row 0 = +90 N, row 720 = -90 S (0.25 deg, pixel = grid point CENTRE);
  col 0 = -180.00 E ... col 1439 = +179.75 E (0.25 deg). Longitude is periodic: there is no
  duplicated dateline column (col 1440 would be col 0). Missing values -> NaN.
"""
import numpy as np

NI, NJ = 1440, 721

# Identity the job expects for each .idx key (eccodes shortName, typeOfLevel, level).
EXPECT = {
    "HTSGW:surface":          ("swh",   "surface",           1),
    "PERPW:surface":          ("perpw", "surface",           1),
    "DIRPW:surface":          ("dirpw", "surface",           1),   # primary (peak) wave direction, degrees true, FROM
    "UGRD:10 m above ground": ("10u",   "heightAboveGround", 10),
    "VGRD:10 m above ground": ("10v",   "heightAboveGround", 10),
    "PRMSL:mean sea level":   ("prmsl", "meanSea",           0),    # pressure reduced to MSL, Pa (not MSLET, the Eta reduction)
}


# ECMWF open data (plan section 43): the index names records by MARS param; the surface level is 0 (GFS: 1).
EXPECT_ECMWF = {
    "swh":  ("swh",  "surface",           0),   # significant height of combined wind waves and swell
    "pp1d": ("pp1d", "surface",           0),   # peak wave period (1/fp of the 1-D spectrum)
    "mwd":  ("mwd",  "surface",           0),   # MEAN wave direction, degrees true, FROM
    "10u":  ("10u",  "heightAboveGround", 10),
    "10v":  ("10v",  "heightAboveGround", 10),
    "msl":  ("msl",  "meanSea",           0),   # mean sea level pressure, Pa (plan section 46)
}
ECMWF_CIRCULAR = ("mwd",)                       # angles: never interpolated linearly
# The open data's grids: 0.25 deg today; ECMWF has said the tier moves to 0.1 deg (owner 2026-10-14: build on 0.25 deg,
# regrid a 0.1-deg field to the site grid if 0.25 deg is withdrawn). (Ni, Nj): increment in degrees.
ECMWF_GRIDS = {(NI, NJ): 0.25, (3600, 1801): 0.1}


def check_geometry(meta):
    """Raise unless meta describes exactly the 0.25 deg global lat/lon grid we assume."""
    if (meta["Ni"], meta["Nj"]) != (NI, NJ):
        raise ValueError(f"unexpected grid {meta['Ni']}x{meta['Nj']}")
    if abs(meta["latitudeOfFirstGridPointInDegrees"] - 90.0) > 1e-6 or meta["jScansPositively"]:
        raise ValueError("expected rows from +90 N southward")
    if abs(meta["latitudeOfLastGridPointInDegrees"] + 90.0) > 1e-6:
        raise ValueError("expected last row at -90 S")
    if abs(meta["longitudeOfFirstGridPointInDegrees"]) > 1e-6 or meta["iScansNegatively"]:
        raise ValueError("expected columns starting at 0 E, eastward")
    if meta["jPointsAreConsecutive"] or meta["alternativeRowScanning"]:
        raise ValueError("expected row-major, non-alternating scanning")
    if abs(meta["iDirectionIncrementInDegrees"] - 0.25) > 1e-6 or abs(meta["jDirectionIncrementInDegrees"] - 0.25) > 1e-6:
        raise ValueError("expected 0.25 deg spacing")


def check_geometry_ecmwf(meta):
    """Raise unless meta is an ECMWF open-data global regular lat/lon grid starting at +90 N and 180 E (= -180 E),
    eastward, row-major: exactly the site's column order, so no roll. Returns the increment (0.25 or 0.1 deg)."""
    dx = ECMWF_GRIDS.get((meta["Ni"], meta["Nj"]))
    if dx is None:
        raise ValueError(f"unexpected grid {meta['Ni']}x{meta['Nj']}")
    if meta.get("gridType", "regular_ll") != "regular_ll":
        raise ValueError(f"unexpected grid type {meta.get('gridType')}")
    if abs(meta["latitudeOfFirstGridPointInDegrees"] - 90.0) > 1e-6 or meta["jScansPositively"]:
        raise ValueError("expected rows from +90 N southward")
    if abs(meta["latitudeOfLastGridPointInDegrees"] + 90.0) > 1e-6:
        raise ValueError("expected last row at -90 S")
    if abs(abs(meta["longitudeOfFirstGridPointInDegrees"]) - 180.0) > 1e-6 or meta["iScansNegatively"]:
        raise ValueError("expected columns starting at 180 E (= -180 E), eastward")
    if meta["jPointsAreConsecutive"] or meta["alternativeRowScanning"]:
        raise ValueError("expected row-major, non-alternating scanning")
    if abs(meta["iDirectionIncrementInDegrees"] - dx) > 1e-6 or abs(meta["jDirectionIncrementInDegrees"] - dx) > 1e-6:
        raise ValueError(f"expected {dx} deg spacing")
    return dx


def regrid_to_site(grid, circular=False):
    """A 0.1-deg ECMWF field (1801 x 3600, +90..-90 N, -180..179.9 E) -> the site's 721 x 1440 grid. Site node
    (i, j) sits at source row 2.5 i, column 2.5 j (both grids start at +90 N / -180 E). Scalars: bilinear over the
    present neighbours (longitude periodic), NaN where they carry less than half the weight (as the client draws).
    Angles (circular): the nearest present source node, never an average of angles."""
    src = np.asarray(grid, dtype=np.float64)
    rows, cols = src.shape
    y = np.arange(NJ) * (rows - 1) / (NJ - 1)
    x = np.arange(NI) * cols / NI
    y0 = np.minimum(np.floor(y).astype(int), rows - 2)
    x0 = np.floor(x).astype(int) % cols
    fy, fx = (y - y0)[:, None], (x - np.floor(x))[None, :]
    x1 = (x0 + 1) % cols
    corners = [(y0, x0, (1 - fy) * (1 - fx)), (y0, x1, (1 - fy) * fx), (y0 + 1, x0, fy * (1 - fx)), (y0 + 1, x1, fy * fx)]
    if circular:
        best_w = np.full((NJ, NI), -1.0)
        out = np.full((NJ, NI), np.nan)
        for r, c, w in corners:
            v = src[np.ix_(r, c)]
            w = np.where(np.isnan(v), -1.0, np.broadcast_to(w, (NJ, NI)))
            take = w > best_w
            out = np.where(take, v, out)
            best_w = np.where(take, w, best_w)
        return out.astype(np.float32)
    acc = np.zeros((NJ, NI))
    wsum = np.zeros((NJ, NI))
    for r, c, w in corners:
        v = src[np.ix_(r, c)]
        ok = ~np.isnan(v)
        w = np.broadcast_to(w, (NJ, NI))
        acc += np.where(ok, v * w, 0.0)
        wsum += np.where(ok, w, 0.0)
    out = np.where(wsum >= 0.5 - 1e-9, acc / np.maximum(wsum, 1e-12), np.nan)
    return out.astype(np.float32)


def to_site_grid_ecmwf(values, meta, circular=False):
    """Pure: raw eccodes values of an ECMWF open-data message -> site geometry float32 (no roll: column 0 is
    already -180 E); a 0.1-deg message is regridded to 0.25 deg."""
    dx = check_geometry_ecmwf(meta)
    grid = np.asarray(values, dtype=np.float64).reshape(meta["Nj"], meta["Ni"])
    grid = np.where(grid == meta["missingValue"], np.nan, grid)
    if not np.all(np.isfinite(grid) | np.isnan(grid)):
        raise ValueError("non-finite values in field")
    if dx != 0.25:
        return regrid_to_site(grid, circular=circular)
    return grid.astype(np.float32)


def check_identity(meta, key, run_dt, step, expect=None):
    """Raise unless the decoded message IS the field/step/cycle the job asked for (G1 P1-4)."""
    short, level_type, level = (expect or EXPECT)[key]
    if meta["shortName"] != short or meta["typeOfLevel"] != level_type or int(meta["level"]) != level:
        raise ValueError(f"{key}: got {meta['shortName']}/{meta['typeOfLevel']}/{meta['level']}")
    if str(meta["stepRange"]) != str(step):
        raise ValueError(f"{key}: stepRange {meta['stepRange']} != {step}")
    if int(meta["dataDate"]) != int(run_dt.strftime("%Y%m%d")) or int(meta["dataTime"]) != run_dt.hour * 100:
        raise ValueError(f"{key}: cycle {meta['dataDate']}/{meta['dataTime']} != {run_dt:%Y%m%d/%H00}")


def to_site_grid(values, meta):
    """Pure: raw eccodes values (row-major, 0..359.75 E, +90..-90 N) -> site geometry float32."""
    check_geometry(meta)
    grid = np.asarray(values, dtype=np.float64).reshape(NJ, NI)
    grid = np.where(grid == meta["missingValue"], np.nan, grid)
    grid = np.roll(grid, -NI // 2, axis=1)         # 0..359.75 E -> -180..179.75 E
    if not np.all(np.isfinite(grid) | np.isnan(grid)):
        raise ValueError("non-finite values in field")
    return grid.astype(np.float32)


_KEYS = ("shortName", "name", "units", "typeOfLevel", "level", "stepRange", "dataDate", "dataTime",
         "Ni", "Nj", "latitudeOfFirstGridPointInDegrees", "latitudeOfLastGridPointInDegrees",
         "longitudeOfFirstGridPointInDegrees", "iDirectionIncrementInDegrees",
         "jDirectionIncrementInDegrees", "jScansPositively", "iScansNegatively",
         "jPointsAreConsecutive", "alternativeRowScanning", "packingType", "missingValue")


def _read(blob, keys):
    import eccodes
    h = eccodes.codes_new_from_message(blob)
    try:
        meta = {k: eccodes.codes_get(h, k) for k in keys}
        vals = eccodes.codes_get_values(h)
    finally:
        eccodes.codes_release(h)
    return meta, vals


def decode(blob, key=None, run_dt=None, step=None):
    """GRIB message bytes -> (grid, meta); with key/run_dt/step the identity is enforced too."""
    meta, vals = _read(blob, _KEYS)
    if key is not None:
        check_identity(meta, key, run_dt, step)
    return to_site_grid(vals, meta), meta


def decode_ecmwf(blob, param, run_dt, step):
    """An ECMWF open-data GRIB message -> (site grid, meta), its identity (param, level, step, cycle) enforced."""
    meta, vals = _read(blob, _KEYS + ("gridType",))
    check_identity(meta, param, run_dt, step, EXPECT_ECMWF)
    return to_site_grid_ecmwf(vals, meta, circular=param in ECMWF_CIRCULAR), meta


def hpa(p):
    """Pressure in Pa -> hPa (float32); NaN stays NaN."""
    return (np.asarray(p, dtype=np.float64) / 100.0).astype(np.float32)


def wind_speed(u, v):
    return np.sqrt(u.astype(np.float64) ** 2 + v.astype(np.float64) ** 2).astype(np.float32)


def wind_dir_from(u, v):
    """Meteorological wind direction: degrees true (clockwise from north) the wind blows FROM, in
    [0, 360). u, v are the earth-relative eastward / northward components (GFS 0.25 deg lat/lon:
    uvRelativeToGrid = 0). u > 0 (blowing east) -> 270; v > 0 (blowing north) -> 180; NaN propagates;
    an exact calm (u = v = 0, speed 0) reads 180."""
    d = (np.degrees(np.arctan2(-u.astype(np.float64), -v.astype(np.float64))) % 360.0).astype(np.float32)
    return np.where(d >= 360.0, np.float32(0.0), d)                   # 359.99999... rounds up to 360 in float32


# NOAA GLWU (plan section 50): the Great Lakes run's Lambert conformal grid (NDFD 2.5 km), regridded to a regular
# lat/lon grid around the lakes for the browser. Identity per .idx key (eccodes shortName, typeOfLevel, level).
EXPECT_GLWU = {
    "WIND:surface":  ("ws",    "surface", 1),     # 10 m wind speed of the NDFD forcing, m/s
    "WDIR:surface":  ("wdir",  "surface", 1),     # its direction, degrees true, FROM
    "HTSGW:surface": ("swh",   "surface", 1),
    "PERPW:surface": ("perpw", "surface", 1),     # 10-0-11 = 1/fp (true peak), the GFS-Wave encoding
    "DIRPW:surface": ("dirpw", "surface", 1),     # primary (peak) wave direction, degrees true, FROM
}
# The source grid, as published 2026-10-21 (every message is checked against it).
GLWU_SRC = {"gridType": "lambert", "Nx": 581, "Ny": 361, "latitudeOfFirstGridPointInDegrees": 41.530708,
            "longitudeOfFirstGridPointInDegrees": 267.363984, "LoVInDegrees": 265.0, "LaDInDegrees": 25.0,
            "Latin1InDegrees": 25.0, "Latin2InDegrees": 25.0, "DxInMetres": 2539.703, "DyInMetres": 2539.703,
            "iScansNegatively": 0, "jScansPositively": 1, "jPointsAreConsecutive": 0, "shapeOfTheEarth": 6}
EARTH_R6 = 6371229.0                            # shapeOfTheEarth 6: a sphere of this radius
# The browser's grid: centre-registered, 0.02 deg (2.2 km N-S, 1.6 km E-W at 45 N), around every GLWU water cell
# (41.38-49.01 N, 92.11-76.13 W) with a margin of 10+ nodes, so the (periodic) coastal fill never wraps.
GLWU_GRID = {"cols": 831, "rows": 401, "lon0": -92.4, "lat0": 49.2, "dlon": 0.02, "dlat": -0.02,
             "registration": "center", "lon_periodic": False}
_GLWU_KEYS = ("shortName", "name", "units", "typeOfLevel", "level", "stepRange", "dataDate", "dataTime",
              "missingValue") + tuple(GLWU_SRC)


def check_geometry_glwu(meta):
    """Raise unless meta is exactly the GLWU Lambert grid the job was built for (a changed grid fails loud)."""
    for k, want in GLWU_SRC.items():
        got = meta.get(k)
        if isinstance(want, float):
            if got is None or abs(float(got) - want) > 1e-5 * max(1.0, abs(want)):
                raise ValueError(f"GLWU grid: {k} = {got}, expected {want}")
        elif got != want:
            raise ValueError(f"GLWU grid: {k} = {got}, expected {want}")


def lambert_colrow(lat, lon, src=GLWU_SRC, radius=EARTH_R6):
    """Fractional (col, row) of points in the GLWU Lambert grid (spherical LCC, one standard parallel = LaD = Latin1 =
    Latin2; rows count NORTHWARD from the first point: scanning mode 64). Exact to 1e-11 cells against eccodes'
    own coordinates (2026-10-21)."""
    phi1 = np.radians(src["Latin1InDegrees"])
    n = np.sin(phi1)
    f = np.cos(phi1) * np.tan(np.pi / 4 + phi1 / 2) ** n / n

    def xy(la, lo):
        rho = radius * f / np.tan(np.pi / 4 + np.radians(np.asarray(la, dtype=np.float64)) / 2) ** n
        th = n * np.radians(((np.asarray(lo, dtype=np.float64) - src["LoVInDegrees"] + 180.0) % 360.0) - 180.0)
        return rho * np.sin(th), -rho * np.cos(th)

    x0, y0 = xy(src["latitudeOfFirstGridPointInDegrees"], src["longitudeOfFirstGridPointInDegrees"])
    x, y = xy(lat, lon)
    return (x - x0) / src["DxInMetres"], (y - y0) / src["DyInMetres"]


def glwu_index(grid=GLWU_GRID, src=GLWU_SRC):
    """(flat source index of the nearest GLWU cell for every node of the browser grid, bool mask of the nodes with one
    inside the source grid). Nearest = the cell whose centre is within half a cell in both directions."""
    lat = grid["lat0"] + grid["dlat"] * np.arange(grid["rows"])
    lon = grid["lon0"] + grid["dlon"] * np.arange(grid["cols"])
    col, row = lambert_colrow(lat[:, None], lon[None, :], src)
    c, r = np.rint(col).astype(np.int64), np.rint(row).astype(np.int64)
    inside = (c >= 0) & (c < src["Nx"]) & (r >= 0) & (r < src["Ny"])
    flat = np.where(inside, np.clip(r, 0, src["Ny"] - 1) * src["Nx"] + np.clip(c, 0, src["Nx"] - 1), 0)
    return flat, inside


def check_glwu_coords(lats, lons, src=GLWU_SRC):
    """The Lambert maths against eccodes' own latitudes / longitudes of a GLWU message (every build): each source node
    must project onto its own (col, row) within 1e-4 of a cell."""
    nx, ny = src["Nx"], src["Ny"]
    col, row = lambert_colrow(np.asarray(lats).reshape(ny, nx), np.asarray(lons).reshape(ny, nx), src)
    jj, ii = np.mgrid[0:ny, 0:nx]
    err = max(float(np.abs(col - ii).max()), float(np.abs(row - jj).max()))
    if not err < 1e-4:
        raise ValueError(f"GLWU Lambert projection off by {err:.3g} cells against eccodes' coordinates")
    return err


def decode_glwu(blob, key, run_dt, step, coords=False):
    """A GLWU GRIB message -> (flat float64 values with NaN for missing (land), meta[, (lats, lons)]): identity and the
    source grid enforced."""
    import eccodes
    h = eccodes.codes_new_from_message(blob)
    try:
        meta = {k: eccodes.codes_get(h, k) for k in _GLWU_KEYS}
        vals = np.asarray(eccodes.codes_get_values(h), dtype=np.float64)
        ll = (eccodes.codes_get_array(h, "latitudes"), eccodes.codes_get_array(h, "longitudes")) if coords else None
    finally:
        eccodes.codes_release(h)
    check_identity(meta, key, run_dt, step, EXPECT_GLWU)
    check_geometry_glwu(meta)
    if vals.size != GLWU_SRC["Nx"] * GLWU_SRC["Ny"]:
        raise ValueError(f"{key}: {vals.size} values for a {GLWU_SRC['Nx']}x{GLWU_SRC['Ny']} grid")
    vals = np.where(vals == meta["missingValue"], np.nan, vals)
    if not np.all(np.isfinite(vals) | np.isnan(vals)):
        raise ValueError(f"{key}: non-finite values")
    return (vals, meta, ll) if coords else (vals, meta)
