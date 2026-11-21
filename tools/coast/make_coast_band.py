"""The open-coast band for coast set v5 (plan section 105): where the US East and Gulf coasts are traced at full detail.

Inside a two-tier region (build_coast_v2.ALL_REGIONS mode "two_tier") the coast set traces the bathymetry's zoom-15
class inside this band (the beaches) and a coarse zoom-12 class in the rest of the box (bays, sounds, marsh, tidal
rivers), instead of GSHHG. This tool makes the band once, from the published zoom 8-10 bathy tiles and GSHHG v1, and
writes it as a committed, pinned file (tools/coast/band_v5.json): the builder reads the file, never recomputes it.

The band, per region, on a zoom-10 mosaic of its box + MARGIN_DEG:
  * class per zoom-10 px: the owner region's zoom-10 tile, else its zoom 9 / 8 ancestor (code > 0 water, code 0 with
    LAND_ID land); outside every region box, or 'outside' inside one, GSHHG v1 (pixel centre in its land) decides.
    Owner = the first region in ORDER whose box holds the px centre (us-sc-ga before us-gulf-east, the bathy layer's
    own exclude);
  * open ocean = the water opened with a disk of radius R_KM (every R-disk that fits in water), only its largest
    connected part: a bay or sound whose mouth is narrower than ~2 R drops out, a straight or convex coast keeps its
    sea against the beach;
  * band = every px within N_KM of the open ocean, plus the FORCE_IN boxes;
  * stored clipped to the region box + 2 px, as run-lengths per zoom-10 row (x0, y0 = the window's first px).
A band line is a zoom-10 pixel edge: a line of constant longitude or latitude, like a box line.

Usage:
  python tools/coast/make_coast_band.py --tiles build/bathy-tiles --v1 build/coast/v1 --fetch --out tools/coast/band_v5.json
  python tools/coast/make_coast_band.py --tiles ... --v1 ... --check tools/coast/band_v5.json   (rebuild and compare)
"""
import argparse
import base64
import hashlib
import json
import math
import os
import sys
from concurrent.futures import ThreadPoolExecutor

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import build_coast_v2 as B2   # noqa: E402

T = B2.T
ZB = 10                                  # the band's zoom (~125 m px at 35 N)
WB = 256 * 2 ** ZB
R_KM = 9.0                               # owner 2026-11-21 (plan section 105): bays with mouths under ~18 km drop out
N_KM = 0.4                               # owner 2026-11-21 option A: the fine trace reaches 0.4 km inland of the open sea
MARGIN_DEG = 0.6                         # the mosaic reaches this far past the region box (the sea beyond it)
CLIP_PX = 2                              # the stored band reaches this many px past the box
ORDER = B2.EAST_ORDER                    # the owner precedence (us-sc-ga before us-gulf-east)
# Section 105 step 1 (ladder rung b): the band line ran along a barrier island's bay shore for 700-850 m, the fine and
# coarse shores a pixel apart: here the band takes the bay strip, so its line runs in water both tiers agree on.
FORCE_IN = [
    (-80.140, 25.885, -80.110, 25.935),  # Haulover / Bal Harbour (Miami)
    (-87.535, 30.270, -87.475, 30.300),  # Perdido Key
]
OUT, LAND, WATER = B2.OUT, B2.LAND, B2.WATER


def px_lon(X):
    return np.asarray(X, np.float64) / WB * 360.0 - 180.0


def px_lat(Y):
    return np.degrees(np.arctan(np.sinh(np.pi * (1 - 2 * np.asarray(Y, np.float64) / WB))))


def lonlat_px(lon, lat):
    return (lon + 180.0) / 360.0 * WB, (1 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2 * WB


def banded():
    return [n for n in ORDER if B2.ALL_REGIONS[n].get("mode") == "two_tier"]


def fetch(tiles_dir):
    """The zoom <= 10 tiles of every ORDER region (the builder's layout: <tiles>/<region>/v<N>/t/z/x/y.png)."""
    for name in ORDER:
        reg = B2.ALL_REGIONS[name]
        base = "%s/static/bathy/%s/v%d" % (B2.PUBLIC, name, reg["v"])
        d = os.path.join(tiles_dir, name, "v%d" % reg["v"])
        os.makedirs(d, exist_ok=True)
        raw = B2._get(base + "/index.json")
        index = json.loads(raw.decode("utf-8"))
        if index.get("digest") != reg["digest"]:
            raise ValueError("%s: published digest %s is not the pinned %s" % (name, index.get("digest"), reg["digest"]))
        open(os.path.join(d, "index.json"), "wb").write(raw)
        want = []
        for z, L in index.get("levels", {}).items():
            if int(z) <= ZB:
                m = T.unpack_bits(base64.b64decode(L["bits"]), (L["h"], L["w"]))
                want += [(int(z), L["x0"] + int(c), L["y0"] + int(r)) for r, c in zip(*np.nonzero(m))]

        def one(t):
            p = os.path.join(d, "t", str(t[0]), str(t[1]), "%d.png" % t[2])
            if not os.path.exists(p):
                os.makedirs(os.path.dirname(p), exist_ok=True)
                open(p, "wb").write(B2._get(base + "/t/%d/%d/%d.png" % t))
        with ThreadPoolExecutor(16) as ex:
            list(ex.map(one, want))
        B2.log("band fetch %s: %d tiles at zooms <= %d" % (name, len(want), ZB))


def cls_tile(path):
    with open(path, "rb") as f:
        code, sf = T.unpack_tile(f.read())
    code, src = code[1:1 + T.TILE, 1:1 + T.TILE], sf[1:1 + T.TILE, 1:1 + T.TILE] & T.SRC_BITS
    c = np.full(code.shape, OUT, np.uint8)
    c[code > 0] = WATER
    c[(code == 0) & (src == T.LAND_ID)] = LAND
    return c


def region_class(tiles_dir, name, X0, Y0, W, H):
    """name's zoom-10 class over the window (OUT where it has no tile at zooms 10..8)."""
    d = os.path.join(tiles_dir, name, "v%d" % B2.ALL_REGIONS[name]["v"], "t")
    out = np.full((H, W), OUT, np.uint8)
    for tx in range(X0 // 256, (X0 + W - 1) // 256 + 1):
        for ty in range(Y0 // 256, (Y0 + H - 1) // 256 + 1):
            c = None
            for z in (10, 9, 8):
                sh = ZB - z
                p = os.path.join(d, str(z), str(tx >> sh), "%d.png" % (ty >> sh))
                if os.path.exists(p):
                    a = cls_tile(p)
                    k = 256 >> sh
                    ox, oy = (tx - ((tx >> sh) << sh)) * k, (ty - ((ty >> sh) << sh)) * k
                    c = a[oy:oy + k, ox:ox + k]
                    if sh:
                        c = np.repeat(np.repeat(c, 1 << sh, 0), 1 << sh, 1)
                    break
            if c is None:
                continue
            sx0, sy0 = max(X0, tx * 256), max(Y0, ty * 256)
            sx1, sy1 = min(X0 + W, tx * 256 + 256), min(Y0 + H, ty * 256 + 256)
            out[sy0 - Y0:sy1 - Y0, sx0 - X0:sx1 - X0] = c[sy0 - ty * 256:sy1 - ty * 256, sx0 - tx * 256:sx1 - tx * 256]
    return out


def band_mask(water, px_m, r_km=R_KM, n_km=N_KM):
    """The band on a water raster (True = water): within n_km of the largest part of the water opened by an r_km disk."""
    from scipy import ndimage
    r_px, n_px = r_km * 1000.0 / px_m, n_km * 1000.0 / px_m
    seed = ndimage.distance_transform_edt(water) >= r_px
    opened = (ndimage.distance_transform_edt(~seed) <= r_px) & water
    lab, nl = ndimage.label(opened)
    if not nl:
        return np.zeros(water.shape, bool)
    sizes = ndimage.sum(opened, lab, index=np.arange(1, nl + 1))
    ocean = lab == int(np.argmax(sizes)) + 1
    return ndimage.distance_transform_edt(~ocean) <= n_px


def runs(mask):
    out = []
    for y in range(mask.shape[0]):
        d = np.diff(np.concatenate([[0], mask[y].astype(np.int8), [0]]))
        s, e = np.nonzero(d == 1)[0], np.nonzero(d == -1)[0]
        if len(s):
            out.append([int(y)] + [int(v) for p in zip(s, e) for v in p])
    return out


def make(tiles_dir, v1_dir, log=B2.log):
    v1 = B2.load_v1(v1_dir)
    res = {"format": "coast-band-v1", "zoom": ZB, "r_km": R_KM, "n_km": N_KM, "margin_deg": MARGIN_DEG,
           "clip_px": CLIP_PX, "order": ORDER, "force_in": [list(b) for b in FORCE_IN],
           "v1_tier1_sha": v1["tier1"]["sha256"],
           "bathy": {n: B2.ALL_REGIONS[n]["digest"] for n in ORDER}, "regions": {}}
    for name in banded():
        b = B2.ALL_REGIONS[name]["box"]
        mb = (b[0] - MARGIN_DEG, b[1] - MARGIN_DEG, b[2] + MARGIN_DEG, b[3] + MARGIN_DEG)
        X0, Y0 = (int(v) for v in lonlat_px(mb[0], mb[3]))
        X1, Y1 = (int(v) + 1 for v in lonlat_px(mb[2], mb[1]))
        W, H = X1 - X0, Y1 - Y0
        lons, lats = px_lon(X0 + np.arange(W) + 0.5), px_lat(Y0 + np.arange(H) + 0.5)
        own = np.zeros((H, W), np.int8)
        for i, r in enumerate(ORDER):
            rb = B2.ALL_REGIONS[r]["box"]
            m = ((lats >= rb[1]) & (lats <= rb[3]))[:, None] & ((lons >= rb[0]) & (lons <= rb[2]))[None, :]
            own[(own == 0) & m] = i + 1
        cls = np.full((H, W), OUT, np.uint8)
        for i, r in enumerate(ORDER):
            m = own == i + 1
            if m.any():
                cls[m] = region_class(tiles_dir, r, X0, Y0, W, H)[m]
        need = cls == OUT
        gs = B2.gshhg_near(v1_dir, v1, mb)
        g = B2.gshhg_fill(gs, lons, lats, need) if gs is not None else np.zeros((H, W), bool)
        water = (cls == WATER) | (need & ~g)
        px_m = 40075016.686 / WB * math.cos(math.radians((b[1] + b[3]) / 2))
        band = band_mask(water, px_m)
        for fb in FORCE_IN:
            band |= ((lats >= fb[1]) & (lats <= fb[3]))[:, None] & ((lons >= fb[0]) & (lons <= fb[2]))[None, :]
        cx0, cy0 = (int(v) - CLIP_PX for v in lonlat_px(b[0], b[3]))
        cx1, cy1 = (int(v) + 1 + CLIP_PX for v in lonlat_px(b[2], b[1]))
        clip = band[cy0 - Y0:cy1 - Y0, cx0 - X0:cx1 - X0]
        res["regions"][name] = {"x0": cx0, "y0": cy0, "w": cx1 - cx0, "h": cy1 - cy0, "px_m": round(px_m, 3),
                                "band_px": int(clip.sum()), "runs": runs(clip)}
        log("band %s: %d x %d px, %d band px" % (name, cx1 - cx0, cy1 - cy0, int(clip.sum())))
    return res


def dumps(res):
    return json.dumps(res, separators=(",", ":"), sort_keys=True)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--tiles", required=True)
    ap.add_argument("--v1", required=True)
    ap.add_argument("--fetch", action="store_true")
    ap.add_argument("--out")
    ap.add_argument("--check", metavar="FILE")
    a = ap.parse_args(argv)
    if a.fetch:
        if not os.path.exists(os.path.join(a.v1, "index.json")):
            B2.fetch_v1(a.v1)
        fetch(a.tiles)
    s = dumps(make(a.tiles, a.v1))
    sha = hashlib.sha256(s.encode()).hexdigest()
    if a.check:
        old = open(a.check, encoding="utf-8").read()
        same = old == s
        print("band %s: %s (rebuilt %s, file %s)" % (a.check, "identical" if same else "DIFFERS", sha[:12],
                                                     hashlib.sha256(old.encode()).hexdigest()[:12]))
        return 0 if same else 1
    if a.out:
        with open(a.out, "w", encoding="utf-8", newline="\n") as f:
            f.write(s)
    print("band sha256 %s, %d bytes" % (sha, len(s)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
