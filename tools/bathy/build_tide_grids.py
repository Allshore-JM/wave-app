"""New Zealand's datum grids (plan section 111a): two small GeoTIFFs the bathymetry builder reads like the Lower 48's.

  nz_msl_nzvd2016.tif   mean sea level's height in NZVD2016 (m). The LINZ coastal lidar (NZVD2016 heights) subtracts it
                        (the builder's `geoid` rule): height above mean sea level = NZVD2016 height - this grid.
  nz_mhhw_msl.tif       mean higher high water's height above mean sea level (m): the water rule's threshold per pixel
                        (regions with mhhw "grid:nz").

MSL in NZVD2016 is interpolated (inverse distance squared over the nearest 4) from the 10 LINZ standard ports in
tools/bathy/nz_datum_stations.json (-0.03 .. -0.21 m: a constant would be up to 0.09 m off).

MHHW above MSL comes from the EOT20 global ocean tide model (DGFI-TUM, Hart-Davis et al. 2021, CC BY 4.0; SEANOE
doi:10.17882/79489), predicted hourly for 19 years (2003-2021, pyTMD) at every 1/8-degree cell within REACH of a region
that reads the grid: the mean of each UTC day's highest hour, less the mean, exactly as the gauges' observed planes were
taken (nz_datum_stations.json, gauges). EOT20 has no weather, and reads 0.00-0.11 m below the 12 gauges (median 0.03);
that difference, interpolated from the gauges the same way, is added (leave-one-out after it: median 0.019 m, max 0.08 m
on 2026-11-26). Every other cell of the grid takes its nearest computed cell's value, so the grid has no hole.

Laptop only (the EOT20 files are 2.3 GB; pip install pyTMD==3.1.0): the grids are committed and pinned by hash
(bathy_sources.DATUM_GRIDS), and the builder never needs the model.

  python tools/bathy/build_tide_grids.py --eot20 <dir holding EOT20/ocean_tides> [--check]
"""
import argparse
import json
import os
import sys
import time

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import bathy_sources as S       # noqa: E402
import build_datum_grids as G   # noqa: E402

STATIONS = os.path.join(HERE, "nz_datum_stations.json")
BOX = (-48.0, 166.5, -33.5, 179.5)    # S, W, N, E: every NZ region's box grown by its reach and a block's margin
STEP = 0.125                          # degrees (EOT20's own grid)
REACH = 0.25                          # degrees around a region's read box where the model is predicted
YEARS = 19
EPOCH = (2003, 1, 1, 0, 0, 0)
CHUNK = 100                           # cells per pyTMD call
FILES = {"msl": os.path.join(HERE, "nz_msl_nzvd2016.tif"), "mhhw": os.path.join(HERE, "nz_mhhw_msl.tif")}
SANE = {"msl": (-0.5, 0.5), "mhhw": (0.0, 2.5)}
LOW_MHHW = 0.2                        # cells below this are listed (amphidromes: real, but worth a look)


def grid_axes():
    s, w, n, e = BOX
    rows, cols = int(round((n - s) / STEP)), int(round((e - w) / STEP))
    return n - (np.arange(rows) + 0.5) * STEP, w + (np.arange(cols) + 0.5) * STEP


def wanted(lat, lon):
    """bool (rows, cols): the cells within REACH of the read box of a region whose mhhw is "grid:nz"."""
    glat, glon = np.meshgrid(lat, lon, indexing="ij")
    m = np.zeros(glat.shape, bool)
    for r in S.REGIONS.values():
        if r.get("mhhw") != "grid:nz":
            continue
        w, s, e, n = r["box"]
        g = r.get("reach", 0) + REACH
        m |= (glon >= w - g) & (glon <= e + g) & (glat >= s - g) & (glat <= n + g)
    return m


def daily_planes(z):
    """(MHHW - MSL) of each row of hourly heights z (cells, hours) from hour 0 UTC: the mean of each day's highest hour,
    less the mean."""
    n = (z.shape[1] // 24) * 24
    d = z[:, :n].reshape(z.shape[0], -1, 24)
    return d.max(axis=2).mean(axis=1) - z.mean(axis=1)


def eot20_mhhw(lat, lon, model_dir, log=print):
    """MHHW - MSL of EOT20 at each point (1-d arrays), 19 years hourly, CHUNK points per call."""
    import pyTMD.compute
    hours = np.arange(0, YEARS * 365.25 * 24, 1.0)
    out = np.empty(lat.size)
    t0 = time.time()
    for i in range(0, lat.size, CHUNK):
        z = pyTMD.compute.tide_elevations(lon[i:i + CHUNK], lat[i:i + CHUNK], hours * 3600.0, directory=model_dir,
                                          model="EOT20", epoch=EPOCH, type="time series", standard="UTC",
                                          extrapolate=True, cutoff=np.inf)
        z = np.ma.filled(np.asarray(z, dtype=np.float64), np.nan).reshape(-1, hours.size)
        if not np.isfinite(z).all():
            raise ValueError("EOT20 gave no value at %d of the points %d-%d" % ((~np.isfinite(z).all(1)).sum(), i,
                                                                                  i + CHUNK))
        out[i:i + CHUNK] = daily_planes(z)
        log("  EOT20 %d / %d cells (%.0f s)" % (min(i + CHUNK, lat.size), lat.size, time.time() - t0))
    return out


def nearest_fill(values, have):
    """Every cell without a value takes its nearest computed cell's value (grid cells, not km)."""
    from scipy import ndimage
    _, (iy, ix) = ndimage.distance_transform_edt(~have, return_indices=True)
    return values[iy, ix]


def loo_report(name, slat, slon, sval):
    err = G.loo(slat, slon, sval)
    print("%s: %d stations, leave-one-out |error| median %.3f m, max %.3f m" % (name, sval.size, np.median(err),
                                                                               err.max()), flush=True)
    return err


def write(path, field, values):
    import rasterio
    from rasterio.transform import from_origin
    s, w, n, e = BOX
    profile = dict(driver="GTiff", width=values.shape[1], height=values.shape[0], count=1, dtype="float32",
                   crs="EPSG:4326", transform=from_origin(w, n, STEP, STEP), nodata=float("nan"),
                   compress="deflate", predictor=3, tiled=False)
    with rasterio.open(path, "w", **profile) as d:
        d.write(np.round(values, 3).astype(np.float32), 1)
        d.update_tags(field=field, source="tools/bathy/build_tide_grids.py (plan section 111a)")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--eot20", required=True, help="directory holding EOT20/ocean_tides/*_ocean_eot20.nc")
    ap.add_argument("--check", action="store_true", help="the gauges' numbers only; nothing written")
    ap.add_argument("--cache", help="an .npz keeping the model's cells (reused when it matches the grid)")
    a = ap.parse_args(argv)
    st = json.load(open(STATIONS, encoding="utf-8"))
    lat, lon = grid_axes()
    glat, glon = np.meshgrid(lat, lon, indexing="ij")

    # mean sea level in NZVD2016, from the ports
    p = st["ports"]
    plat = np.array([v["lat"] for v in p.values()])
    plon = np.array([v["lon"] for v in p.values()])
    pval = np.array([v["msl_nzvd2016"] for v in p.values()])
    loo_report("MSL in NZVD2016 (ports)", plat, plon, pval)

    # MHHW: EOT20 at the gauges first (their difference corrects the model)
    g = st["gauges"]
    names = list(g)
    glat_s = np.array([g[k]["lat"] for k in names])
    glon_s = np.array([g[k]["lon"] for k in names])
    obs = np.array([g[k]["mhhw_msl"] for k in names])
    mod = eot20_mhhw(glat_s, glon_s, a.eot20)
    diff = obs - mod
    for k, o, m_, d in zip(names, obs, mod, diff):
        print("  %-28s gauge %.3f  EOT20 %.3f  gauge - EOT20 %+.3f" % (g[k]["name"], o, m_, d), flush=True)
    print("gauge - EOT20: median %+.3f, |max| %.3f" % (np.median(diff), np.abs(diff).max()), flush=True)
    err = loo_report("MHHW after the gauge correction (gauge - EOT20)", glat_s, glon_s, diff)
    if a.check:
        return 0

    msl = G.idw(glat, glon, plat, plon, pval).reshape(glat.shape)
    want = wanted(lat, lon)
    model = np.full(glat.shape, np.nan)
    print("EOT20 at %d of %d grid cells" % (want.sum(), want.size), flush=True)
    cached = None
    if a.cache and os.path.exists(a.cache):
        with np.load(a.cache) as f:
            if f["want"].shape == want.shape and (f["want"] == want).all():
                cached = f["values"]
    if cached is None:
        cached = eot20_mhhw(glat[want], glon[want], a.eot20)
        if a.cache:
            np.savez(a.cache, want=want, values=cached)
    model[want] = cached
    model = nearest_fill(model, want)
    mhhw = model + G.idw(glat, glon, glat_s, glon_s, diff).reshape(glat.shape)
    low = np.argwhere(want & (mhhw < LOW_MHHW))
    print("cells near a region with MHHW below %.1f m: %d" % (LOW_MHHW, len(low)), flush=True)
    for i, j in low[:20]:
        print("  %.3f N %.3f E: %.3f m" % (glat[i, j], glon[i, j], mhhw[i, j]), flush=True)
    for field, v in (("msl", msl), ("mhhw", mhhw)):
        lo, hi = SANE[field]
        if not (np.isfinite(v).all() and lo <= v.min() and v.max() <= hi):
            raise ValueError("%s grid outside %s..%s m: %.3f..%.3f" % (field, lo, hi, np.nanmin(v), np.nanmax(v)))
        write(FILES[field], field, v)
        print("wrote %s (%d x %d, %.3f .. %.3f m)" % (os.path.basename(FILES[field]), v.shape[1], v.shape[0],
                                                     v.min(), v.max()), flush=True)
    print("leave-one-out MHHW: median %.3f m, max %.3f m" % (np.median(err), err.max()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
