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
    "UGRD:10 m above ground": ("10u",   "heightAboveGround", 10),
    "VGRD:10 m above ground": ("10v",   "heightAboveGround", 10),
}


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


def check_identity(meta, key, run_dt, step):
    """Raise unless the decoded message IS the field/step/cycle the job asked for (G1 P1-4)."""
    short, level_type, level = EXPECT[key]
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


def decode(blob, key=None, run_dt=None, step=None):
    """GRIB message bytes -> (grid, meta); with key/run_dt/step the identity is enforced too."""
    import eccodes
    h = eccodes.codes_new_from_message(blob)
    try:
        meta = {k: eccodes.codes_get(h, k) for k in _KEYS}
        vals = eccodes.codes_get_values(h)
    finally:
        eccodes.codes_release(h)
    if key is not None:
        check_identity(meta, key, run_dt, step)
    return to_site_grid(vals, meta), meta


def wind_speed(u, v):
    return np.sqrt(u.astype(np.float64) ** 2 + v.astype(np.float64) ** 2).astype(np.float32)
