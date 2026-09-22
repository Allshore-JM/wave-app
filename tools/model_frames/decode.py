"""GRIB2 record -> float32 grid on the site's fixed geometry.

Output geometry (every consumer relies on this):
  shape (721, 1440); row 0 = +90 N, row 720 = -90 S (0.25 deg);
  col 0 = -180.00 E ... col 1439 = +179.75 E (0.25 deg). Missing values -> NaN.
"""
import numpy as np

NI, NJ = 1440, 721


def decode(blob):
    """Return (grid float32 [721,1440], meta dict). Raises if the geometry is not the expected one."""
    import eccodes
    h = eccodes.codes_new_from_message(blob)
    try:
        ni, nj = eccodes.codes_get(h, "Ni"), eccodes.codes_get(h, "Nj")
        meta = {k: eccodes.codes_get(h, k) for k in (
            "shortName", "name", "units", "typeOfLevel", "level", "stepRange", "dataDate", "dataTime",
            "latitudeOfFirstGridPointInDegrees", "longitudeOfFirstGridPointInDegrees",
            "iDirectionIncrementInDegrees", "jDirectionIncrementInDegrees", "jScansPositively",
            "packingType", "missingValue")}
        vals = eccodes.codes_get_values(h).astype(np.float64)
    finally:
        eccodes.codes_release(h)
    if (ni, nj) != (NI, NJ):
        raise ValueError(f"unexpected grid {ni}x{nj}")
    if abs(meta["latitudeOfFirstGridPointInDegrees"] - 90.0) > 1e-6 or meta["jScansPositively"]:
        raise ValueError("expected rows from +90 N southward")
    if abs(meta["longitudeOfFirstGridPointInDegrees"]) > 1e-6:
        raise ValueError("expected columns starting at 0 E")
    if abs(meta["iDirectionIncrementInDegrees"] - 0.25) > 1e-6 or abs(meta["jDirectionIncrementInDegrees"] - 0.25) > 1e-6:
        raise ValueError("expected 0.25 deg spacing")
    grid = vals.reshape(NJ, NI)
    grid = np.where(grid == meta["missingValue"], np.nan, grid)
    grid = np.roll(grid, -NI // 2, axis=1)         # 0..359.75 E -> -180..179.75 E
    return grid.astype(np.float32), meta


def wind_speed(u, v):
    return np.sqrt(u * u + v * v).astype(np.float32)
