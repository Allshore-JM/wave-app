"""Builds the Nearshore Analysis transfer tables (plan section 109): for every region of nearshore_regions.json a 10 m
grid from the published bathymetry tiles, and one linear SWAN 41.51 run per offshore swell bin (a unit-height swell of
one frequency from one 15-degree sector: refraction and shoaling only; no wind, breaking, whitecapping or friction).
The surf band's height gain and local wave direction per bin are the table the publisher (nearshore.py) reads.

The runs are split into chunks so a matrix of GitHub jobs can do them side by side (.github/workflows/nearshore-table.yml):

  python tools/model_frames/nearshore_build.py plan                                  # the matrix: every region x chunk
  python tools/model_frames/nearshore_build.py chunk --region R --chunk K --out D    # the grid, then the chunk's bins
  python tools/model_frames/nearshore_build.py assemble --region R --in D --out D2   # the region's table.npz
  python tools/model_frames/nearshore_build.py put-chunk / get-chunks / put-table / put-manifest ...   (R2; see main)

Layout in the bucket (immutable): static/nearshore/<set>/build/<region>/c<K>.npz, static/nearshore/<set>/<region>/table.npz,
static/nearshore/<set>/manifest.json (written last: the regions' frames and their tables' sha256).
SWAN: the conda-forge build (swan.exe, one process per bin). A modelled estimate, not safety guidance."""
import argparse
import hashlib
import io
import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import nearshore_engine as E  # noqa: E402

CONFIG = os.path.join(HERE, "nearshore_regions.json")
BATHY = "https://models.allshoresurf.com/static/bathy"
UA = {"User-Agent": "allshoresurf.com nearshore table build (https://allshoresurf.com)"}
STATIC = "static/nearshore"
R_EARTH = 6378137.0
WORLD = 2 * math.pi * R_EARTH
KY = math.pi / 180.0 * 6371008.8
MDC, MSC, REL = 72, 4, 0.06          # 5-degree directions; a 5-frequency grid +-6 % with the energy in the middle one
SECTOR = 15.0                        # the offshore bin's width in direction
EXC = -5.0                           # the bottom file's dry value
BOUNDARY_TOL = 0.02                  # a bin's offshore-row height must be within this of 1 m ...
GRAZING_DEG = 78.0                   # ... but for sectors this far off the region's shore normal: they run along the coast and
                                     # never cross the offshore side whole (the pilot: 0.81-0.97 at 90 and 105 degrees off)


def config(path=CONFIG):
    with open(path, encoding="utf-8") as f:
        c = json.load(f)
    ids = [r["id"] for r in c["regions"]]
    if len(set(ids)) != len(ids) or not all(re.fullmatch(r"[a-z][a-z0-9-]{1,20}", i) for i in ids):
        raise ValueError("bad region ids")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,20}", c["set"]):
        raise ValueError("bad set name")
    return c


def region_of(cfg, rid):
    for r in cfg["regions"]:
        if r["id"] == rid:
            return r
    raise ValueError(f"no region {rid!r}")


def bins(cfg):
    return [(f, d) for f in cfg["freqs"] for d in cfg["dirs"]]


def chunk_bins(cfg, k):
    """The bins of chunk k (0-based): every chunks-th bin, so each chunk holds a spread of periods (a like run time)."""
    return bins(cfg)[k::cfg["chunks"]]


# ---------------------------------------------------------------- the grid

def frame(cfg, r):
    """The region's SWAN frame in its own flat-earth metres about ref: xpc, ypc (the grid's origin), alpc, mx, my, dx."""
    a = math.radians(r["alpha"]); ux = (math.cos(a), math.sin(a)); uy = (-math.sin(a), math.cos(a)); dx = cfg["dx"]
    x0, x1 = r["x"]
    return dict(lon0=r["ref"][0], lat0=r["ref"][1], alpc=r["alpha"], dx=dx, mx=int(round((x1 - x0) / dx)), my=int(round(cfg["olen"] / dx)),
                xpc=x0 * ux[0] + r["y0"] * uy[0], ypc=x0 * ux[1] + r["y0"] * uy[1], ux=ux, uy=uy)


def nodes_lonlat(fr):
    i = np.arange(fr["mx"] + 1) * fr["dx"]; j = np.arange(fr["my"] + 1) * fr["dx"]
    J, I = np.meshgrid(j, i, indexing="ij")                # rows = y (offshore), columns = x (alongshore)
    e = fr["xpc"] + I * fr["ux"][0] + J * fr["uy"][0]; n = fr["ypc"] + I * fr["ux"][1] + J * fr["uy"][1]
    return fr["lon0"] + e / (KY * math.cos(math.radians(fr["lat0"]))), fr["lat0"] + n / KY


def decode_depth(code):
    """The bathymetry tiles' depth code (tools/bathy/bathy_tiles.py) -> metres below mean sea level; nan = none."""
    code = code.astype(np.int64); d = np.full(code.shape, np.nan)
    m = (code >= 1) & (code <= 41001); d[m] = (code[m] - 1) / 100.0 - 10.0
    m = (code > 41001) & (code <= 62001); d[m] = 400 + (code[m] - 41001) / 10.0
    m = (code > 62001) & (code <= 65401); d[m] = 2500 + (code[m] - 62001) * 2.5
    return d


def tile_xy(lon, lat, z):
    mx = R_EARTH * math.radians(lon); my = R_EARTH * math.log(math.tan(math.pi / 4 + math.radians(lat) / 2)); n = 2 ** z
    return int((mx + WORLD / 2) / WORLD * n), int((WORLD / 2 - my) / WORLD * n)


def fetch_tile(region, version, z, x, y, cache, opener=urllib.request.urlopen):
    """(depth[256, 256], land[256, 256]) of one bathymetry tile, or None where the region has none (404)."""
    from PIL import Image
    fn = os.path.join(cache, f"{region}_v{version}_{z}_{x}_{y}.png")
    if os.path.exists(fn + ".404"):
        return None
    if not os.path.exists(fn):
        url = f"{BATHY}/{region}/v{version}/t/{z}/{x}/{y}.png"; last = None
        for attempt in range(4):
            try:
                with opener(urllib.request.Request(url, headers=UA), timeout=60) as resp:
                    body = resp.read()
                with open(fn + ".part", "wb") as f:
                    f.write(body)
                os.replace(fn + ".part", fn); last = None
                break
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    open(fn + ".404", "w").close()
                    return None
                last = exc
            except Exception as exc:                     # noqa: BLE001
                last = exc
            time.sleep(1 + 2 * attempt)
        if last is not None:
            raise RuntimeError(f"bathymetry tile {region} {z}/{x}/{y}: {last}")
    a = np.array(Image.open(fn).convert("L"), dtype=np.int64)
    if a.shape != (774, 258):
        raise ValueError(f"bathymetry tile {region} {z}/{x}/{y}: not 258 x 774")
    code = a[0:258] * 256 + a[258:516]; src = a[516:774]
    return decode_depth(code)[1:-1, 1:-1], ((code == 0) & ((src & 0x1F) == 30))[1:-1, 1:-1]


def mosaic(region, version, z, box, cache, opener=urllib.request.urlopen):
    x0, y1 = tile_xy(box[0], box[1], z); x1, y0 = tile_xy(box[2], box[3], z)
    D = np.full(((y1 - y0 + 1) * 256, (x1 - x0 + 1) * 256), np.nan); L = np.zeros(D.shape, bool); got = 0
    for ty in range(y0, y1 + 1):
        for tx in range(x0, x1 + 1):
            t = fetch_tile(region, version, z, tx, ty, cache, opener)
            if t is None:
                continue
            got += 1; r0 = (ty - y0) * 256; c0 = (tx - x0) * 256
            D[r0:r0 + 256, c0:c0 + 256] = t[0]; L[r0:r0 + 256, c0:c0 + 256] = t[1]
    return dict(D=D, L=L, x0=x0, y0=y0, z=z, tiles=got)


def sample(m, lon, lat):
    """Bilinear depth of a mosaic at lon / lat (nan where a corner has none) and the nearest pixel's land flag."""
    n = 2 ** m["z"]
    mx = R_EARTH * np.radians(lon); my = R_EARTH * np.log(np.tan(np.pi / 4 + np.radians(lat) / 2))
    px = (mx + WORLD / 2) / WORLD * n * 256 - m["x0"] * 256 - 0.5; py = (WORLD / 2 - my) / WORLD * n * 256 - m["y0"] * 256 - 0.5
    D = m["D"]; H, W = D.shape
    i0 = np.clip(np.floor(py).astype(int), 0, H - 2); j0 = np.clip(np.floor(px).astype(int), 0, W - 2)
    fy = np.clip(py - i0, 0, 1); fx = np.clip(px - j0, 0, 1)
    v = D[i0, j0] * (1 - fx) * (1 - fy) + D[i0, j0 + 1] * fx * (1 - fy) + D[i0 + 1, j0] * (1 - fx) * fy + D[i0 + 1, j0 + 1] * fx * fy
    return v, m["L"][np.clip(np.rint(py).astype(int), 0, H - 1), np.clip(np.rint(px).astype(int), 0, W - 1)]


def build_grid(cfg, r, cache, opener=urllib.request.urlopen):
    """-> (frame, depth[my + 1, mx + 1] in metres, EXC where dry or unknown, stats). The later bathymetry layers of the
    config (the lidar) take the place of the earlier ones wherever they have data."""
    fr = frame(cfg, r); lon, lat = nodes_lonlat(fr)
    box = (lon.min() - 0.01, lat.min() - 0.01, lon.max() + 0.01, lat.max() + 0.01)
    dep = np.full(lon.shape, np.nan); land = np.zeros(lon.shape, bool); used = []
    for name, version, z in cfg["bathy"]:
        m = mosaic(name, version, z, box, cache, opener); d, l = sample(m, lon, lat); ok = np.isfinite(d)
        land = np.where(ok, l, land | (l & ~np.isfinite(dep))); dep = np.where(ok, d, dep)
        used.append(dict(layer=name, version=version, z=z, tiles=m["tiles"], share=round(float(ok.mean()), 4)))
    out = np.where(np.isfinite(dep), dep, EXC); out = np.where(land & ~np.isfinite(dep), EXC, out)
    stats = dict(layers=used, dry=round(float((out <= 0).mean()), 4), base_dry=round(float((out[0] <= 0).mean()), 4),
                 offshore_min=round(float(out[-1].min()), 1), offshore_median=round(float(np.median(out[-1])), 1))
    return fr, out, stats


# ---------------------------------------------------------------- SWAN

def spectral_grid(f0):
    flow, fhigh = f0 * (1 - REL), f0 * (1 + REL)
    return flow, fhigh, flow * (fhigh / flow) ** (np.arange(MSC + 1) / MSC), np.arange(MDC) * 360.0 / MDC


def unit_spectrum(f0, th0):
    """E[f, d] (m2/Hz/deg) of a 1 m swell at f0 from a SECTOR-wide top-hat about th0, as SWAN integrates it."""
    flow, fhigh, f, d = spectral_grid(f0); dd = 360.0 / MDC
    E2 = np.zeros((len(f), len(d)))
    for off in np.arange(-SECTOR / 2 + dd / 2, SECTOR / 2, dd):
        E2[MSC // 2, int(np.argmin(np.abs(((d - (th0 + off) + 180) % 360) - 180)))] += 1
    m0 = (E2.sum(1) * f * np.log(fhigh / flow) / MSC).sum() * dd
    return f, d, E2 * (1 / 16.0) / m0


def spec_text(f, d, E2):
    fac = max(E2.max(), 1e-12) / 99999.0
    L = ["SWAN   1", "$ unit bin", "LOCATIONS", "    1", "  0.0  0.0", "AFREQ", f"   {len(f)}"] + [f"  {v:.6f}" for v in f]
    L += ["NDIR", f"   {len(d)}"] + [f"  {v:.4f}" for v in d]
    L += ["QUANT", "    1", "VaDens", "m2/Hz/degr", "   -0.9900E+02", "FACTOR", f"  {fac:.6E}"]
    L += [" ".join(str(v) for v in row) for row in np.rint(E2 / fac).astype(int)]
    return "\n".join(L) + "\n"


def input_text(fr, f0):
    flow, fhigh, _, _ = spectral_grid(f0); mx, my, dx = fr["mx"], fr["my"], fr["dx"]
    L = ["PROJECT 'ns' 'tb'", "SET LEVEL 0.0 NAUTICAL", "MODE STATIONARY TWODIMENSIONAL", "COORDINATES CARTESIAN",
         f"CGRID REGULAR {fr['xpc']:.2f} {fr['ypc']:.2f} {fr['alpc']} {mx * dx} {my * dx} {mx} {my} CIRCLE {MDC} {flow:.6f} {fhigh:.6f} {MSC}",
         f"INPGRID BOTTOM REGULAR {fr['xpc']:.2f} {fr['ypc']:.2f} {fr['alpc']} {mx} {my} {dx} {dx} EXC {EXC}",
         "READINP BOTTOM 1. 'bot.dat' 3 0 FREE"]
    for n, s in enumerate(((0, my, mx, my), (0, 0, 0, my), (mx, 0, mx, my))):     # the offshore side and the two ends
        L.append(f"BOUNDSPEC SEGMENT IJ {s[0]} {s[1]} {s[2]} {s[3]} CONSTANT FILE 'bnd{n}.sp2' 1")
    L += ["OFF WINDG", "OFF QUAD", "OFF WCAP", "OFF BREA", "NUM STOPC 0.005 0.01 0.005 99.5 STAT 30",
          "BLOCK 'COMPGRID' NOHEADER 'hs.txt' LAY 3 HSIGN 1.", "BLOCK 'COMPGRID' NOHEADER 'dir.txt' LAY 3 DIR 1.", "COMPUTE", "STOP"]
    return "\n".join(L) + "\n"


def run_bin(work, fr, bot, f0, th0, rows, swan="swan.exe", timeout=3 * 3600):
    """One bin in its own folder -> dict(K codes[rows, nx], DIR codes[rows, nx], seconds, rc, boundary, iterations)."""
    run = os.path.join(work, f"f{f0:.4f}_d{int(th0):03d}"); os.makedirs(run, exist_ok=True)
    f, d, E2 = unit_spectrum(f0, th0); text = spec_text(f, d, E2)
    for n in range(3):
        with open(os.path.join(run, f"bnd{n}.sp2"), "w") as fh:
            fh.write(text)
    with open(os.path.join(run, "INPUT"), "w") as fh:
        fh.write(input_text(fr, f0))
    shutil.copyfile(bot, os.path.join(run, "bot.dat"))
    t0 = time.monotonic(); env = dict(os.environ, OMP_NUM_THREADS="1")
    p = subprocess.run([swan], cwd=run, env=env, capture_output=True, text=True, timeout=timeout)
    secs = time.monotonic() - t0
    prt = ""
    for name in ("PRINT", "PRINT-001"):
        if os.path.exists(os.path.join(run, name)):
            with open(os.path.join(run, name), errors="replace") as fh:
                prt = fh.read()
            break
    shape = (fr["my"] + 1, fr["mx"] + 1); out = dict(f0=f0, th0=th0, seconds=round(secs, 1), rc=p.returncode)
    try:
        hs = np.array(open(os.path.join(run, "hs.txt")).read().split(), dtype=float).reshape(shape)
        dr = np.array(open(os.path.join(run, "dir.txt")).read().split(), dtype=float).reshape(shape)
    except Exception as exc:                             # noqa: BLE001
        out.update(rc=out["rc"] or 99, error=f"{exc.__class__.__name__}: {exc}"[:200], tail=(p.stdout + p.stderr + prt)[-600:])
        return out
    it = re.findall(r"iteration\s+(\d+); sweep 4", prt)
    if re.search(r"(?i)severe error|terminating error", prt):
        out["rc"] = out["rc"] or 98
    hs = np.where(hs > 0, hs, 0.0); dr = np.where(dr > -900, dr, 0.0)
    out.update(boundary=round(float(np.median(hs[-1])), 4), iterations=int(it[-1]) if it else None,
               K=E.Table.encode_k(hs[:rows] ** 2), DIR=(np.rint(dr[:rows] * (256.0 / 360.0)).astype(np.int64) % 256).astype(np.uint8))
    shutil.rmtree(run, ignore_errors=True)
    return out


def chunk_name(k):
    return f"c{k}.npz"


def run_chunk(cfg, rid, k, out, cache, parallel=4, swan="swan.exe", only=None, budget_s=5 * 3600, log=print):
    """The grid of the region, then the bins of chunk k, `parallel` at a time -> the path of the chunk's npz.
    only = run just the first `only` bins (a smoke test). Past the budget no new bin is started."""
    r = region_of(cfg, rid); os.makedirs(out, exist_ok=True); os.makedirs(cache, exist_ok=True)
    t0 = time.monotonic(); fr, dep, stats = build_grid(cfg, r, cache)
    log(f"{rid}: grid {dep.shape[1]} x {dep.shape[0]} nodes, {json.dumps(stats)}, {time.monotonic() - t0:.0f} s")
    work = os.path.join(out, f"work_{rid}_{k}"); os.makedirs(work, exist_ok=True)
    bot = os.path.join(work, "bot.dat"); np.savetxt(bot, dep, fmt="%.2f")
    todo = chunk_bins(cfg, k)[:only] if only else chunk_bins(cfg, k); rows = r["rows"]; deadline = t0 + budget_s

    def go(b):
        if time.monotonic() > deadline:
            return dict(f0=b[0], th0=b[1], rc=97, error="the chunk's time budget ran out", seconds=0.0)
        res = run_bin(work, fr, bot, b[0], b[1], rows, swan)
        log(f"  {rid} f {b[0]:.4f} d {int(b[1]):03d}: {res['seconds']:.0f} s rc {res['rc']} boundary {res.get('boundary')} " + res.get("error", ""))
        return res

    with ThreadPoolExecutor(parallel) as ex:
        results = list(ex.map(go, todo))
    good = [x for x in results if x["rc"] == 0 and "K" in x]
    meta = [{k2: v for k2, v in x.items() if k2 not in ("K", "DIR")} for x in results]
    path = os.path.join(out, f"{rid}_{chunk_name(k)}")
    np.savez_compressed(path, set=np.array(cfg["set"]), region=np.array(rid), chunk=np.array(k), smoke=np.array(bool(only)),
                        bins=np.array([[x["f0"], x["th0"]] for x in good], float).reshape(-1, 2),
                        K=np.stack([x["K"] for x in good]) if good else np.zeros((0, rows, dep.shape[1]), np.uint8),
                        DIR=np.stack([x["DIR"] for x in good]) if good else np.zeros((0, rows, dep.shape[1]), np.uint8),
                        dep_cm=np.clip(np.rint(dep[:rows] * 100.0), -32000, 32000).astype(np.int16),
                        frame=np.array(json.dumps({k2: fr[k2] for k2 in ("lon0", "lat0", "alpc", "dx", "mx", "my", "xpc", "ypc")})),
                        meta=np.array(json.dumps(meta)), grid=np.array(json.dumps(stats)))
    shutil.rmtree(work, ignore_errors=True)
    bad = [m for m in meta if m["rc"] != 0]
    log(f"{rid} chunk {k}: {len(good)} of {len(todo)} bins, {time.monotonic() - t0:.0f} s" + (f"; FAILED: {bad[:3]}" if bad else ""))
    return path, len(bad)


# ---------------------------------------------------------------- the region's table

def off_normal(alpha, th0):
    """Degrees between a swell direction (FROM) and the direction straight onshore of a region whose x axis is alpha."""
    return abs(((th0 - (360.0 - alpha) + 180.0) % 360.0) - 180.0)


def check_boundary(meta, alpha, tol=BOUNDARY_TOL):
    """The bins whose offshore-row height is wrong: off by more than tol, or for a grazing sector above 1 + tol."""
    bad = []
    for m in meta:
        grazing = off_normal(alpha, m["th0"]) >= GRAZING_DEG
        if (m["boundary"] > 1.0 + tol) or (not grazing and m["boundary"] < 1.0 - tol):
            bad.append((m["f0"], m["th0"], m["boundary"]))
    return bad


def assemble(cfg, rid, src, out, log=print):
    """The region's chunks -> table.npz (the format nearshore_engine.Table.from_npz reads) and its facts."""
    r = region_of(cfg, rid); want = bins(cfg); got, metas, dep, fr = {}, [], None, None
    for k in range(cfg["chunks"]):
        z = np.load(os.path.join(src, f"{rid}_{chunk_name(k)}"))
        if str(z["set"]) != cfg["set"] or str(z["region"]) != rid or bool(z["smoke"]):
            raise ValueError(f"{rid} chunk {k}: not a whole chunk of set {cfg['set']}")
        if dep is None:
            dep, fr = z["dep_cm"], json.loads(str(z["frame"]))
        elif not np.array_equal(dep, z["dep_cm"]) or fr != json.loads(str(z["frame"])):
            raise ValueError(f"{rid} chunk {k}: another grid than chunk 0's")
        for n, (f0, th0) in enumerate(z["bins"]):
            got[(round(float(f0), 4), int(th0))] = (z["K"][n], z["DIR"][n])
        metas += json.loads(str(z["meta"]))
    missing = [b for b in want if (round(b[0], 4), int(b[1])) not in got]
    if missing:
        raise ValueError(f"{rid}: {len(missing)} bins missing, e.g. {missing[:4]}")
    off = check_boundary(metas, r["alpha"])
    if off:
        raise ValueError(f"{rid}: offshore height off by more than {BOUNDARY_TOL:.0%} in {len(off)} bins, e.g. {off[:4]}")
    K = np.stack([got[(round(b[0], 4), int(b[1]))][0] for b in want]); D = np.stack([got[(round(b[0], 4), int(b[1]))][1] for b in want])
    os.makedirs(out, exist_ok=True); path = os.path.join(out, f"{rid}_table.npz"); version = f"{cfg['set']}/{rid}"
    np.savez_compressed(path, F=np.array(cfg["freqs"], float), D=np.array(cfg["dirs"], float), dx=float(fr["dx"]), alpc=float(fr["alpc"]),
                        xpc=float(fr["xpc"]), ypc=float(fr["ypc"]), lon0=float(fr["lon0"]), lat0=float(fr["lat0"]), version=version,
                        dep_cm=dep, K=K, DIR=D)
    with open(path, "rb") as f:
        sha = hashlib.sha256(f.read()).hexdigest()
    secs = [m["seconds"] for m in metas]
    facts = dict(id=rid, ref=r["ref"], alpha=r["alpha"], x=r["x"], y0=r["y0"], core=r["core"], rows=r["rows"], shape=list(dep.shape),
                 table=f"{STATIC}/{cfg['set']}/{rid}/table.npz", sha256=sha, bytes=os.path.getsize(path), bins=len(want),
                 boundary=[round(min(m["boundary"] for m in metas), 4), round(max(m["boundary"] for m in metas), 4)],
                 bin_seconds=[round(min(secs)), round(float(np.mean(secs))), round(max(secs))])
    log(f"{rid}: table {facts['bytes']} B, sha256 {sha[:12]}, boundary {facts['boundary']}, bin seconds {facts['bin_seconds']}")
    return path, facts


# ---------------------------------------------------------------- the bucket

def _store():
    import gfs_winds as W
    return W._store_from_env()


def _get(store, key):
    import nearshore as N
    return N.get_bytes(store, key)


def chunk_key(cfg, rid, k):
    return f"{STATIC}/{cfg['set']}/build/{rid}/{chunk_name(k)}"


def table_key(cfg, rid):
    return f"{STATIC}/{cfg['set']}/{rid}/table.npz"


def manifest_key(cfg):
    return f"{STATIC}/{cfg['set']}/manifest.json"


def main(argv=None, store=None):
    import publish as P
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("plan")
    c = sub.add_parser("chunk"); c.add_argument("--region", required=True); c.add_argument("--chunk", type=int, required=True)
    c.add_argument("--out", required=True); c.add_argument("--cache", default=None); c.add_argument("--parallel", type=int, default=4)
    c.add_argument("--only", type=int, default=0); c.add_argument("--swan", default="swan.exe")
    a = sub.add_parser("assemble"); a.add_argument("--region", required=True); a.add_argument("--in", dest="src", required=True); a.add_argument("--out", required=True)
    for name in ("put-chunk", "get-chunks", "put-table"):
        s = sub.add_parser(name); s.add_argument("--region", required=True); s.add_argument("--dir", required=True)
        if name == "put-chunk":
            s.add_argument("--chunk", type=int, required=True)
    m = sub.add_parser("put-manifest"); m.add_argument("--dir", required=True)
    args = ap.parse_args(argv); cfg = config()
    if args.cmd == "plan":
        print(json.dumps([{"region": r["id"], "chunk": k} for r in cfg["regions"] for k in range(cfg["chunks"])]))
        return 0
    if args.cmd == "chunk":
        if not 0 <= args.chunk < cfg["chunks"]:
            print("no such chunk"); return 2
        _, bad = run_chunk(cfg, args.region, args.chunk, args.out, args.cache or os.path.join(args.out, "tiles"), args.parallel, args.swan, args.only or None)
        return 1 if bad else 0
    if args.cmd == "assemble":
        path, facts = assemble(cfg, args.region, args.src, args.out)
        with open(os.path.join(args.out, f"{args.region}_facts.json"), "w", encoding="utf-8") as f:
            json.dump(facts, f)
        return 0
    store = store or _store()
    if args.cmd == "put-chunk":
        key = chunk_key(cfg, args.region, args.chunk)
        with open(os.path.join(args.dir, f"{args.region}_{chunk_name(args.chunk)}"), "rb") as f:
            body = f.read()
        if bool(np.load(io.BytesIO(body))["smoke"]):
            key = key.replace("/build/", "/smoke/")       # a smoke test never sits where a whole chunk is looked for
        store.put(key, body, "application/octet-stream", P.POINTER)
        print(f"put {key} {len(body)} B")
        return 0
    if args.cmd == "get-chunks":
        os.makedirs(args.dir, exist_ok=True)
        for k in range(cfg["chunks"]):
            body = _get(store, chunk_key(cfg, args.region, k))
            if body is None:
                print(f"{args.region}: chunk {k} is not in the bucket"); return 1
            with open(os.path.join(args.dir, f"{args.region}_{chunk_name(k)}"), "wb") as f:
                f.write(body)
        return 0
    if args.cmd == "put-table":
        if store.get_json(manifest_key(cfg)) is not None:
            print(f"set {cfg['set']} has a manifest: its tables are immutable (name a new set)"); return 2
        with open(os.path.join(args.dir, f"{args.region}_table.npz"), "rb") as f:
            store.put(table_key(cfg, args.region), f.read(), "application/octet-stream", P.IMMUTABLE)
        print(f"put {table_key(cfg, args.region)}")
        return 0
    if args.cmd == "put-manifest":
        if store.get_json(manifest_key(cfg)) is not None:
            print(f"set {cfg['set']} has a manifest already"); return 2
        regions = []
        for r in cfg["regions"]:
            with open(os.path.join(args.dir, f"{r['id']}_facts.json"), encoding="utf-8") as f:
                facts = json.load(f)
            body = _get(store, facts["table"])
            if body is None or hashlib.sha256(body).hexdigest() != facts["sha256"]:
                print(f"{r['id']}: the table in the bucket is not the one assembled"); return 1
            regions.append(facts)
        doc = dict(format="nearshore-tables-v1", set=cfg["set"], dx=cfg["dx"], freqs=cfg["freqs"], dirs=cfg["dirs"], regions=regions,
                   built_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), swan="41.51 (conda-forge)", bathy=cfg["bathy"])
        store.put(manifest_key(cfg), json.dumps(doc, separators=(",", ":")).encode(), "application/json", P.IMMUTABLE)
        print(f"put {manifest_key(cfg)}: {len(regions)} regions")
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
