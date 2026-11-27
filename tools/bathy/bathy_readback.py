"""Read a built region back against its sources (plan section 111): random water pixels of the finest-zoom tiles, each
compared with its source's own value at the pixel's centre, brought to mean sea level the way the builder does (the
source's geoid grid subtracted, its offset added). Blended, filled and coarse pixels are left out (they are not one
source's value by design).

    python tools/bathy/bathy_readback.py <built dir, e.g. build/bathy/nz-south/v1> [--n 2000] [--work <build work dir>]

A tile pixel is the average of the source's pixels it covers (or a bilinear read of a coarser source) and the source is
read here at the nearest pixel, so on a slope the two differ by about the slope times half a pixel: the medians, not
the worst pixel, are the test (plan: signed and absolute median <= 0.05 m). Prints one line per source and a summary;
exits 1 when a median is over the limit or no pixel could be compared.
"""
import argparse
import json
import os
import random
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import bathy_fetch as F    # noqa: E402
import bathy_stac as STAC  # noqa: E402
import bathy_tiles as T    # noqa: E402
import build_bathy as B    # noqa: E402

LIMIT_M = 0.05


def tile_bytes(out, z, x, y):
    with open(B.tile_path(out, z, x, y), "rb") as f:
        return f.read()


def sample_pixels(out, index, n, seed=111):
    """[(lon, lat, depth m, source id)] of up to n random unflagged water pixels of the region's finest-zoom tiles."""
    z = index["zmax"]
    found = B.scan_tiles(out).get(z, set())
    rnd = random.Random(seed)
    tiles = sorted(found)
    rnd.shuffle(tiles)
    per = max(1, n // max(1, min(len(tiles), 400)))
    picks = []
    for (x, y) in tiles[:400]:
        code, sf = T.unpack_tile(tile_bytes(out, z, x, y))
        c, s = code[1:-1, 1:-1], sf[1:-1, 1:-1]
        ok = (c > 0) & ((s & (T.FLAG_BLENDED | T.FLAG_FILLED | T.FLAG_COARSE)) == 0)
        rr, cc = np.nonzero(ok)
        if not rr.size:
            continue
        x0, y0, x1, y1 = T.tile_bounds(z, x, y)
        p = (x1 - x0) / T.TILE
        for k in rnd.sample(range(rr.size), min(per, rr.size)):
            lon, lat = T.xy_to_lonlat(x0 + (cc[k] + 0.5) * p, y1 - (rr[k] + 0.5) * p)
            picks.append((lon, lat, float(T.DECODE[c[rr[k], cc[k]]]), int(s[rr[k], cc[k]] & T.SRC_BITS)))
        if len(picks) >= n:
            break
    return picks[:n]


def source_files(reg, src, work):
    """[(gdal path, lon/lat rect)] of one source, as the builder reads it."""
    if src.get("kind") == "stac":
        return [(p, r) for (_, r, p) in STAC.read_items(src["stac"], B._https_bytes)]
    if src.get("kind") == "tilezip":
        files = F.fetch_mirror(src["mirror"], work, src["crs"], log=lambda *a: None)
        return [(f, r) for f in files for r in B.footprint({"local": f, "gdal": f})]
    if src.get("gdal"):
        return [(src["gdal"], (-180.0, -90.0, 180.0, 90.0))]
    raise SystemExit("%s: kind %s is not read back here" % (src["key"], src.get("kind")))


def read_at(files, pts, nodata):
    """The source's value at each (lon, lat) (NaN where none), first file with a value winning."""
    import rasterio
    from rasterio.warp import transform
    out = np.full(len(pts), np.nan)
    for path, r in files:
        idx = [i for i, (lo, la) in enumerate(pts) if np.isnan(out[i]) and r[0] <= lo <= r[2] and r[1] <= la <= r[3]]
        if not idx:
            continue
        with rasterio.open(path) as d:
            xs, ys = transform("EPSG:4326", d.crs, [pts[i][0] for i in idx], [pts[i][1] for i in idx])
            nd = d.nodata if d.nodata is not None else nodata
            for i, v in zip(idx, d.sample(list(zip(xs, ys)), indexes=1)):
                v = float(v[0])
                if (nd is None or v != nd) and abs(v) < B.S.SANE_M:
                    out[i] = v
    return out


def grid_at(path, pts):
    import rasterio
    with rasterio.open(path) as d:
        return np.array([float(v[0]) for v in d.sample(pts, indexes=1)])


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("dir")
    ap.add_argument("--n", type=int, default=2000)
    ap.add_argument("--work", help="the build's work folder (a tilezip source's GeoTIFFs are reused from it)")
    a = ap.parse_args(argv)
    B.gdal_env()
    index = json.load(open(os.path.join(a.dir, "index.json")))
    reg = B.region_config(index["region"])
    work = a.work or os.path.join(os.path.dirname(os.path.dirname(a.dir)), "_readback_" + index["region"])
    os.makedirs(work, exist_ok=True)
    picks = sample_pixels(a.dir, index, a.n)
    bad = not picks
    for s in reg["srcs"]:
        mine = [p for p in picks if p[3] == s["id"]]
        if not mine:
            continue
        pts = [(p[0], p[1]) for p in mine]
        v = read_at(source_files(reg, s, work), pts, s.get("nodata"))
        if s.get("geoid_path"):
            v = v - grid_at(s["geoid_path"], pts)
        v = v + s.get("offset", 0.0)
        tile_depth = np.array([p[2] for p in mine])
        ok = np.isfinite(v)
        d = tile_depth[ok] - (-v[ok])
        if not d.size:
            print("%s: none of %d pixels has a source value" % (s["key"], len(mine)))
            bad = True
            continue
        med, amed = float(np.median(d)), float(np.median(np.abs(d)))
        print("%s (id %d): %d pixels, tile - source depth: median %+.3f m, |median| %.3f m, p90 |%.3f| m, max |%.3f| m;"
              " within 0.1 m %.1f %%" % (s["key"], s["id"], d.size, med, amed, float(np.percentile(np.abs(d), 90)),
                                         float(np.abs(d).max()), 100.0 * float((np.abs(d) <= 0.1).mean())))
        bad = bad or abs(med) > LIMIT_M or amed > LIMIT_M
    print("READBACK %s: %s" % (index["region"], "FAIL" if bad else "ok"))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
