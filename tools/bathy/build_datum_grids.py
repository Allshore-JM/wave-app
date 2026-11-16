"""The Lower-48 datum grids (plan section 98): two small GeoTIFFs the bathymetry builder reads like a geoid.

  us_msl_navd88.tif   mean sea level's height above NAVD88 (m). A source in NAVD88 heights subtracts it (the builder's
                      `geoid` rule): height above mean sea level = NAVD88 height - this grid.
  us_mhhw_msl.tif     mean higher high water's height above mean sea level (m): the water rule's threshold per pixel
                      (a region with mhhw "grid").

Both come from NOAA CO-OPS tide-station datums (mdapi stations?type=datums, then each station's datums.json, metric,
the current epoch), interpolated by inverse distance squared over the nearest NEAR stations, on a 0.05-degree grid over
the Lower 48's coasts. NOAA's VDatum grids would be finer, but their download refused automated access (2026-11-15).
Great Lakes stations are left out (the lakes use their Low Water Datum). A station counts for a grid only when it
publishes both datums of that grid.

The tool prints a leave-one-out check: each station predicted from the others (the error a pixel next to it would see).
MSL-NAVD88 leaves out, worst first, every station the others predict more than LOO_MAX off: river-stage gauges far
inland, whose mean level is the river's (13 of 968 on 2026-11-15: Carrollton, Sacramento, the Savannah, Ashley, Pee Dee,
Merrimack and Atchafalaya ...). Built 2026-11-15 from 1,795 stations: MSL-NAVD88 955 stations, leave-one-out median
0.022 m / p90 0.079 m / max 0.294 m; MHHW-MSL 1,740 stations, median 0.022 m / p90 0.122 m (its largest errors are real
tide contrasts across barriers: Cape Cod Bay vs Nantucket Sound, lagoons; not filtered). The stations are kept in us_datum_stations.json, so a rebuild needs no network and gives the same bytes
(the pins in bathy_sources.DATUM_GRIDS).

  python tools/bathy/build_datum_grids.py --cache datums.json        # fetch (paced; ~10 min) or reuse the cache
  python tools/bathy/build_datum_grids.py --cache datums.json --check # the leave-one-out numbers only
  python tools/bathy/build_datum_grids.py --cache tools/bathy/us_datum_stations.json --offline   # the committed build
"""
import argparse
import json
import os
import sys
import time

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
MD = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi"
UA = "allshoresurf.com bathymetry build (datum grid)"
BOX = (24.0, -125.5, 49.5, -66.5)       # S, W, N, E: the Lower 48 and its offshore band
STEP = 0.05                             # degrees
NEAR = 4                                # stations per pixel
FILES = {"msl_navd88": os.path.join(HERE, "us_msl_navd88.tif"),
         "mhhw_msl": os.path.join(HERE, "us_mhhw_msl.tif")}
SANE = {"msl_navd88": (-3.0, 4.0), "mhhw_msl": (0.0, 4.5)}   # outside these a station's numbers are a typo
# A station the others predict worse than this is left out, worst first, one at a time (MSL-NAVD88 only): a river-stage
# gauge's mean level is the river's, not the sea's, and it spreads its error over the coast around it (Carrollton on the
# Mississippi put Lake Pontchartrain's centre 0.43 m off). One at a time, because dropping the worst can bring a good
# neighbour back in line (New Canal). MHHW-MSL is not filtered: its large errors are real tide contrasts across barriers.
LOO_MAX = {"msl_navd88": 0.3}


def _get(url, tries=3):
    import ssl
    import urllib.request
    import certifi
    ctx = ssl.create_default_context(cafile=certifi.where())
    err = None
    for _ in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, context=ctx, timeout=60) as r:
                return json.loads(r.read())
        except Exception as e:                  # one station's failure is retried, then left out
            err = e
            time.sleep(2)
    raise err


def fetch(cache):
    """{station id: {lat, lon, MSL, MHHW, NAVD88}} for the Lower-48 datum stations, from `cache` when it holds them."""
    have = {}
    if cache and os.path.exists(cache):
        with open(cache) as f:
            have = json.load(f)
    st = _get(MD + "/stations.json?type=datums")["stations"]
    s, w, n, e = BOX
    todo = [x for x in st if s <= x["lat"] <= n and w <= x["lng"] <= e and not x.get("greatlakes")]
    print("datum stations in the box (not the Great Lakes):", len(todo), flush=True)
    for i, x in enumerate(todo):
        if x["id"] in have:
            continue
        try:
            d = _get("%s/stations/%s/datums.json?units=metric" % (MD, x["id"]))
        except Exception as ex:
            print("left out", x["id"], ex, flush=True)
            continue
        dd = {v["name"]: v["value"] for v in (d.get("datums") or []) if v.get("value") is not None}
        have[x["id"]] = {"name": x["name"], "lat": x["lat"], "lon": x["lng"],
                         "MSL": dd.get("MSL"), "MHHW": dd.get("MHHW"), "NAVD88": dd.get("NAVD88")}
        time.sleep(0.25)
        if cache and i % 100 == 0:
            with open(cache, "w") as f:
                json.dump(have, f)
            print(i, len(have), flush=True)
    if cache:
        with open(cache, "w") as f:
            json.dump(have, f)
    return have


def points(stations, field):
    """(lat, lon, value, id) arrays of the stations publishing `field`'s two datums, typos left out."""
    rows = []
    for sid, v in stations.items():
        if field == "msl_navd88":
            a, b = v.get("MSL"), v.get("NAVD88")
        else:
            a, b = v.get("MHHW"), v.get("MSL")
        if a is None or b is None:
            continue
        val = float(a) - float(b)
        lo, hi = SANE[field]
        if lo <= val <= hi:
            rows.append((float(v["lat"]), float(v["lon"]), val, str(sid)))
    if len(rows) < NEAR + 1:
        raise ValueError("too few stations for %s: %d" % (field, len(rows)))
    rows.sort()
    a = np.array([r[:3] for r in rows], dtype=np.float64)
    return a[:, 0], a[:, 1], a[:, 2], np.array([r[3] for r in rows])


def _km(lat0, lon0, lat, lon):
    """Great-circle km from each (lat0, lon0) (arrays, shape (k,)) to each station (shape (m,)): (k, m)."""
    p0, p = np.radians(lat0)[:, None], np.radians(lat)[None, :]
    dl = np.radians(lon)[None, :] - np.radians(lon0)[:, None]
    h = np.sin((p - p0) / 2) ** 2 + np.cos(p0) * np.cos(p) * np.sin(dl / 2) ** 2
    return 6371.0 * 2 * np.arcsin(np.sqrt(np.clip(h, 0, 1)))


def idw(lat, lon, slat, slon, sval, near=NEAR, chunk=20000):
    """Inverse-distance-squared value at each (lat, lon) from its `near` nearest stations (a station within 1 km
    counts as 1 km away, so a pixel on a station gets nearly its value)."""
    lat, lon = np.ravel(lat), np.ravel(lon)
    out = np.empty(lat.shape, np.float64)
    for i in range(0, lat.size, chunk):
        d = _km(lat[i:i + chunk], lon[i:i + chunk], slat, slon)
        k = np.argpartition(d, near - 1, axis=1)[:, :near]
        dk = np.take_along_axis(d, k, axis=1)
        wk = 1.0 / np.maximum(dk, 1.0) ** 2
        out[i:i + chunk] = (wk * sval[k]).sum(axis=1) / wk.sum(axis=1)
    return out


def loo(slat, slon, sval, near=NEAR):
    """Each station predicted from the others: |error| array."""
    err = np.empty(sval.size)
    for i in range(sval.size):
        keep = np.arange(sval.size) != i
        err[i] = abs(idw(slat[i:i + 1], slon[i:i + 1], slat[keep], slon[keep], sval[keep], near)[0] - sval[i])
    return err


def drop_outliers(slat, slon, sval, ids, limit, near=NEAR):
    """Leave out the station the others predict worst while that error is above `limit`, one at a time (each removal
    changes its neighbours' predictions). Returns (lat, lon, value, id) of the kept stations and [(id, error)] dropped."""
    keep = np.ones(sval.size, bool)
    dropped = []
    while keep.sum() > near + 1:
        idx = np.flatnonzero(keep)
        err = loo(slat[idx], slon[idx], sval[idx], near)
        j = int(np.argmax(err))
        if not err[j] > limit:
            break
        dropped.append((str(ids[idx[j]]), float(err[j])))
        keep[idx[j]] = False
    return slat[keep], slon[keep], sval[keep], ids[keep], dropped


def grid_axes():
    s, w, n, e = BOX
    rows = int(round((n - s) / STEP))
    cols = int(round((e - w) / STEP))
    lat = n - (np.arange(rows) + 0.5) * STEP           # north to south, pixel centres
    lon = w + (np.arange(cols) + 0.5) * STEP
    return lat, lon


def write(path, field, values):
    """A GeoTIFF (EPSG:4326, float32 rounded to mm, NaN nodata) the builder warps bilinearly like a geoid."""
    import rasterio
    from rasterio.transform import from_origin
    s, w, n, e = BOX
    profile = dict(driver="GTiff", width=values.shape[1], height=values.shape[0], count=1, dtype="float32",
                   crs="EPSG:4326", transform=from_origin(w, n, STEP, STEP), nodata=float("nan"),
                   compress="deflate", predictor=3, tiled=False)
    with rasterio.open(path, "w", **profile) as d:
        d.write(np.round(values, 3).astype(np.float32), 1)
        d.update_tags(source="NOAA CO-OPS tide-station datums, inverse distance squared over the nearest %d" % NEAR,
                      field=field)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--cache", help="station datums json (read when present, written after fetching)")
    ap.add_argument("--check", action="store_true", help="the leave-one-out numbers only; nothing written")
    ap.add_argument("--offline", action="store_true", help="use the cache only (no network)")
    a = ap.parse_args(argv)
    if a.offline:
        with open(a.cache) as f:
            stations = json.load(f)
    else:
        stations = fetch(a.cache)
    lat, lon = grid_axes()
    glat, glon = np.meshgrid(lat, lon, indexing="ij")
    for field, path in FILES.items():
        slat, slon, sval, ids = points(stations, field)
        if field in LOO_MAX:
            slat, slon, sval, ids, dropped = drop_outliers(slat, slon, sval, ids, LOO_MAX[field])
            for sid, e in dropped:
                print("  %s: left out %s %s (the others predict it %.2f m off)"
                      % (field, sid, stations.get(sid, {}).get("name", ""), e), flush=True)
        err = loo(slat, slon, sval)
        print("%s: %d stations, leave-one-out |error| median %.3f m, p90 %.3f m, max %.3f m"
              % (field, sval.size, np.median(err), np.percentile(err, 90), err.max()), flush=True)
        if a.check:
            continue
        v = idw(glat, glon, slat, slon, sval).reshape(glat.shape)
        write(path, field, v)
        print("  wrote %s (%d x %d, %.3f .. %.3f m)" % (os.path.relpath(path, HERE), v.shape[1], v.shape[0],
                                                       v.min(), v.max()), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
