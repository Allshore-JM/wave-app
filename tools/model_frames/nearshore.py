"""Publishes the Nearshore Analysis product to R2 under gfswave/nearshore/v1/ (plan section 109, stage 5): for every step
of the wave overlay (209 steps to +384 h), the breaking zone and the break lines of the pilot stretch (Himalayas to
Chun's, Oahu), from the GFS-Wave 2-D spectrum at buoy 51201, a SWAN transfer table and the predicted tide.

Inputs
- the spectrum: gfswave/spectral/v1/<RUN>/spec2d/51201.npz, written by the spectral job (gfs_spectral.py). With
  --from-archive a run that has none is read from NOAA's archive directly (a manual run; the stream stops at the member).
- the data folder (--data): table.npz (nearshore_table.pack), atlas.json (the best-swell texts), tides.json (predicted
  highs and lows at Haleiwa) and manifest.json with their sha256. It is a checkout of the data branch at a pinned commit.

Layout (immutable but for the pointer)
  gfswave/nearshore/v1/<RUN>/f<HHH>.png   one 8-bit grey PNG per step, 256 wide: a header row, one 256x256 height plane
                                          per z14 tile (the index lists them), then the break lines' vertex table
  gfswave/nearshore/v1/<RUN>/index.json   steps, tiles, box, units, versions
  gfswave/nearshore/v1/latest.json        -> {run, index}, written last
  static/nearshore/<table version>/atlas.json   the best-swell texts (written once per table version)

Usage
  python tools/model_frames/nearshore.py --data D                 # publish the spectral job's live run, if not live here
  python tools/model_frames/nearshore.py --data D --dry-run --out O [--spec FILE.npz] [--run YYYYMMDDHH] [--from-archive]

Exit codes: 0 published or already live, 3 nothing to do yet (no spectrum for the run, storage busy), 1 failed.
A modelled estimate, not safety guidance."""
import argparse
import hashlib
import io
import json
import math
import os
import struct
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import gfs_winds as W  # noqa: E402
import nearshore_engine as E  # noqa: E402
import publish as P  # noqa: E402

PREFIX = P.NEARSHORE_PREFIX
SPEC_PREFIX = P.GFSSPEC_PREFIX
STATIC_PREFIX = "static/nearshore"
FORMAT = "nearshore-v1"
STATION = "51201"
STEPS = list(range(0, 121)) + list(range(123, 385, 3))      # the wave overlay's steps (fetch.STEPS; a test pins the match)
KEEP = 4
Z = 14
TILE = 256
HEIGHT_STEP_M = 0.05                 # the height plane's unit: code 1..255 = 0.05 .. 12.75 m; 0 = not breaking
VERTEX_BYTES = 10
MAGIC = b"NSH1"
MAX_TRAINS = 4
MIN_TRAIN_HS = 0.15                  # smaller partitions are folded into one remainder train
MSL_MINUS_MLLW_M = 0.2835            # Haleiwa 1612668 (bigwave's datum note)
MAX_VERTICES = 6000


def frame_key(run, step):
    return f"{PREFIX}/{run}/f{step:03d}.png"


def index_key(run):
    return f"{PREFIX}/{run}/index.json"


def spec2d_key(run, sid=STATION):
    return f"{SPEC_PREFIX}/{run}/spec2d/{sid}.npz"


def atlas_key(version):
    return f"{STATIC_PREFIX}/{version}/atlas.json"


# ---------------------------------------------------------------- the data folder

def load_data(folder):
    """-> (table, atlas bytes, tides dict, manifest). Every file's sha256 must match the manifest."""
    man = json.load(open(os.path.join(folder, "manifest.json"), encoding="utf-8"))
    got = {}
    for name in ("table.npz", "atlas.json", "tides.json"):
        with open(os.path.join(folder, name), "rb") as f:
            got[name] = f.read()
        if hashlib.sha256(got[name]).hexdigest() != man["sha256"][name]:
            raise ValueError(f"{name}: sha256 does not match the manifest")
    table = E.Table.from_npz(io.BytesIO(got["table.npz"]))
    if table.version != man["version"]:
        raise ValueError("table version does not match the manifest")
    return table, got["atlas.json"], json.loads(got["tides.json"]), man


def tide_msl(tides, when):
    """Predicted tide (m above mean sea level) at a UTC datetime: a cosine between the neighbouring high and low."""
    t = when.replace(tzinfo=timezone.utc).timestamp() if when.tzinfo is None else when.timestamp()
    hl = tides["hilo"]
    lo, hi = 0, len(hl) - 1
    if not hl or t < hl[0][0] or t > hl[-1][0]:
        raise ValueError("the tide file does not cover " + when.strftime("%Y-%m-%d %H:%M"))
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if hl[mid][0] <= t:
            lo = mid
        else:
            hi = mid
    (t0, h0), (t1, h1) = hl[lo], hl[hi]
    f = (t - t0) / (t1 - t0) if t1 > t0 else 0.0
    return h0 + (h1 - h0) * (1 - math.cos(math.pi * f)) / 2 - tides.get("msl_minus_mllw", MSL_MINUS_MLLW_M)


# ---------------------------------------------------------------- map tiles

def lonlat_to_tile(lon, lat, z=Z):
    n = 2.0 ** z
    return (lon + 180.0) / 360.0 * n, (1.0 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2.0 * n


def tile_pixels(x, y, z=Z):
    """lon[256], lat[256] of the pixel centres of tile (x, y)."""
    n = 2.0 ** z; k = (np.arange(TILE) + 0.5) / TILE
    return (x + k) / n * 360.0 - 180.0, np.degrees(np.arctan(np.sinh(math.pi * (1 - 2 * (y + k) / n))))


def tile_maps(table):
    """The z14 tiles that hold wet cells of the band, and for each the grid cell under every pixel:
    -> [(x, y, flat index array[256, 256] into the grid, -1 outside)], sorted, and the band's box (w, s, e, n)."""
    xs = np.array([0.0, table.nx - 1.0, 0.0, table.nx - 1.0]) * table.dx; ys = np.array([0.0, 0.0, table.ny - 1.0, table.ny - 1.0]) * table.dx
    lon, lat = table.lonlat(xs, ys); box = (float(lon.min()), float(lat.min()), float(lon.max()), float(lat.max()))
    x0, y1 = lonlat_to_tile(box[0], box[1]); x1, y0 = lonlat_to_tile(box[2], box[3])
    out = []
    for ty in range(int(y0), int(y1) + 1):
        for tx in range(int(x0), int(x1) + 1):
            plon, plat = tile_pixels(tx, ty)
            gx, gy = table.xy(plon[None, :], plat[:, None])
            i = np.rint(gx / table.dx).astype(int); j = np.rint(gy / table.dx).astype(int)
            ok = (i >= 0) & (i < table.nx) & (j >= 0) & (j < table.ny)
            idx = np.where(ok, j * table.nx + i, -1)
            if (table.dep.reshape(-1)[idx[ok]] > 0).any():
                out.append((tx, ty, idx))
    return out, box


# ---------------------------------------------------------------- one step

def trains_of(freqs, dirs, Es, wind, table):
    """The step's swell trains for the engine: the partitions of the spectrum, largest first, at most MAX_TRAINS, the
    rest folded into one remainder. -> (trains, the buoy's largest train as (hs m, tp s, dir deg) or None, lost share)."""
    import partition2d as P2
    parts, lab = P2.partition_v2(freqs, dirs, Es, wind=wind)
    if not parts:
        return [], None, 0.0
    big = [p for p in parts if p["hs"] >= MIN_TRAIN_HS][:MAX_TRAINS - 1] or parts[:1]
    keep = {p["label"] for p in big}
    trains, total, lost = [], 0.0, 0.0
    for p in big:
        Wt, _ = E.weights(freqs, dirs, Es, table.F, table.D, mask=lab == p["label"])
        trains.append(dict(W=Wt, tp=float(p["tp"])))
    rest = ~np.isin(lab, list(keep))
    if rest.any():
        Wt, _ = E.weights(freqs, dirs, Es, table.F, table.D, mask=rest)
        others = [p for p in parts if p["label"] not in keep]
        if Wt.sum() > 0 and others:
            trains.append(dict(W=Wt, tp=float(others[0]["tp"])))
    _, lost = E.weights(freqs, dirs, Es, table.F, table.D)
    trains = [t for t in trains if t["W"].sum() > 0]
    return trains, (float(parts[0]["hs"]), float(parts[0]["tp"]), float(parts[0]["dir"])), float(lost)


def pack_vertices(lines, table, box):
    """The break lines as VERTEX_BYTES per vertex: x, y (uint16 shares of the box, west-east and south-north), peel
    (deg in bits 0-6; bit 7 = the break runs along the line in the order of its vertices), flags (bit 0 right, bit 1 mixed swell, bits 2-3 breaker, bits 4-6 band, bit 7 first vertex of a line),
    ride length (5 m units), breaking height (HEIGHT_STEP_M units), score (0-100), energy share (percent)."""
    n = int(lines["x"].size)
    if n == 0:
        return b"", 0
    if n > MAX_VERTICES:                                   # never in the pilot; keep the frame bounded
        keep = np.zeros(n, bool); keep[np.argsort(-lines["score"], kind="stable")[:MAX_VERTICES]] = True
        lines = {k: v[keep] for k, v in lines.items()}; n = MAX_VERTICES
    lon, lat = table.lonlat(lines["x"], lines["y"])
    qx = np.clip(np.rint((lon - box[0]) / (box[2] - box[0]) * 65535), 0, 65535).astype("<u2")
    qy = np.clip(np.rint((lat - box[1]) / (box[3] - box[1]) * 65535), 0, 65535).astype("<u2")
    first = np.concatenate(([True], lines["line"][1:] != lines["line"][:-1]))
    flags = ((lines["side"] > 0) * 1 + (lines["mixed"] > 0) * 2 + (lines["breaker"].astype(int) & 3) * 4
             + (lines["band"].astype(int) & 7) * 16 + first * 128).astype(np.uint8)
    rec = np.zeros(n, dtype=[("x", "<u2"), ("y", "<u2"), ("peel", "u1"), ("flags", "u1"), ("ride", "u1"), ("hb", "u1"), ("score", "u1"), ("share", "u1")])
    rec["x"], rec["y"], rec["flags"] = qx, qy, flags
    rec["peel"] = np.clip(np.rint(lines["peel"]), 0, 90).astype(np.uint8) | (np.asarray(lines["fwd"], bool) * 128).astype(np.uint8)
    rec["ride"] = np.clip(np.rint(lines["ride_m"] / 5.0), 0, 255)
    rec["hb"] = np.clip(np.rint(lines["hb"] / HEIGHT_STEP_M), 0, 255)
    rec["score"] = np.clip(np.rint(lines["score"]), 0, 100)
    rec["share"] = np.clip(np.rint(lines["share"] * 100.0), 0, 100)
    return rec.tobytes(), n


def header_row(ntiles, nvert, tide_m, buoy, ntrains, lost):
    """256 bytes: magic, version, tiles, vertices (uint16), tide (int16 cm), the buoy's largest train: Hs (uint16 cm),
    Tp (uint16 0.1 s), direction (uint16 deg FROM), trains, lost energy (percent)."""
    hs, tp, dr = buoy if buoy else (0.0, 0.0, 0.0)
    head = MAGIC + struct.pack("<BBHhHHHBB", 1, ntiles, nvert, int(round(tide_m * 100)), int(round(hs * 100)), int(round(tp * 10)),
                               int(round(dr)) % 360, ntrains, int(round(min(max(lost, 0.0), 1.0) * 100)))
    return head + bytes(TILE - len(head))


def png_bytes(rows):
    from PIL import Image
    buf = io.BytesIO()
    Image.frombytes("L", (TILE, len(rows) // TILE), rows).save(buf, "PNG", optimize=False, compress_level=9)
    return buf.getvalue()


def build_frame(table, maps, box, freqs, dirs, Es, wind, tide):
    """One step -> (PNG bytes, stats dict)."""
    trains, buoy, lost = trains_of(freqs, dirs, Es, wind, table)
    planes = bytearray(); nvert = 0; vbytes = b""; zone_cells = 0; hmax = 0.0
    if trains:
        r = E.analyse(table, trains, tide)
        code = np.where(r["zone"], np.clip(np.rint(r["hset"] / HEIGHT_STEP_M), 1, 255), 0).astype(np.uint8).reshape(-1)
        zone_cells = int(r["zone"].sum()); hmax = float(r["hset"][r["zone"]].max()) if zone_cells else 0.0
        for _, _, idx in maps:
            planes += np.where(idx >= 0, code[np.maximum(idx, 0)], 0).astype(np.uint8).tobytes()
        vbytes, nvert = pack_vertices(r["lines"], table, box)
    else:
        planes = bytearray(len(maps) * TILE * TILE)
    pad = (-len(vbytes)) % TILE
    rows = header_row(len(maps), nvert, tide, buoy, len(trains), lost) + bytes(planes) + vbytes + bytes(pad)
    return png_bytes(rows), dict(vertices=nvert, zone_cells=zone_cells, hmax_m=round(hmax, 2), trains=len(trains),
                                 buoy=[round(v, 2) for v in buoy] if buoy else None, tide_m=round(tide, 3))


def decode_frame(png):
    """The inverse, for the tests and for checks by hand: -> dict(header fields, planes[n, 256, 256], vertices)."""
    from PIL import Image
    im = Image.open(io.BytesIO(png)); a = np.frombuffer(im.tobytes(), np.uint8).reshape(im.size[1], TILE)
    head = a[0].tobytes()
    if head[:4] != MAGIC:
        raise ValueError("not a nearshore frame")
    ver, ntiles, nvert, tide, hs, tp, dr, ntr, lost = struct.unpack("<BBHhHHHBB", head[4:18])
    planes = a[1:1 + ntiles * TILE].reshape(ntiles, TILE, TILE)
    raw = a[1 + ntiles * TILE:].tobytes()[:nvert * VERTEX_BYTES]
    v = np.frombuffer(raw, dtype=[("x", "<u2"), ("y", "<u2"), ("peel", "u1"), ("flags", "u1"), ("ride", "u1"), ("hb", "u1"), ("score", "u1"), ("share", "u1")])
    return dict(version=ver, tiles=ntiles, vertices=v, tide_m=tide / 100.0, buoy=(hs / 100.0, tp / 10.0, dr), trains=ntr, lost=lost / 100.0,
                planes=planes)


def build_run(table, tides, spec, run_dt, steps=STEPS, log=print):
    """Every step's frame -> ({step: png}, index document). `spec` = the opened spec2d npz."""
    maps, box = tile_maps(table)
    if not maps:
        raise ValueError("no map tile holds water")
    freqs = np.asarray(spec["freqs"], float); dirs = np.asarray(spec["dirs"], float)
    hours = {int(h): n for n, h in enumerate(np.asarray(spec["hours"]))}
    frames, stats, t0 = {}, {}, time.monotonic()
    for step in steps:
        n = hours.get(step)
        if n is None:
            continue
        when = run_dt + timedelta(hours=step)
        frames[step], stats[step] = build_frame(table, maps, box, freqs, dirs, np.asarray(spec["E"][n], float),
                                                (float(spec["u10"][n]), float(spec["udir"][n])), tide_msl(tides, when))
    if len(frames) < 0.9 * len(steps):
        raise ValueError(f"{len(frames)} of {len(steps)} steps in the spectrum")
    doc = {"format": FORMAT, "run": W.run_name(run_dt), "station": STATION, "steps": sorted(frames), "z": Z,
           "tiles": [[x, y] for x, y, _ in maps], "box": [round(v, 6) for v in box], "height_step_m": HEIGHT_STEP_M,
           "vertex_bytes": VERTEX_BYTES, "bands": list(E.PARAMS["bands"]), "band_names": list(E.BAND_NAMES),
           "table": table.version, "params": E.PARAMS["version"], "atlas": atlas_key(table.version),
           "bytes": sum(len(b) for b in frames.values()), "vertices_max": max(s["vertices"] for s in stats.values()),
           "hmax_m": max(s["hmax_m"] for s in stats.values()), "build_s": round(time.monotonic() - t0, 1),
           "built_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "note": "Modelled estimate from the GFS-Wave spectrum at buoy 51201. Not safety guidance."}
    log(f"nearshore {doc['run']}: {len(frames)} steps, {len(maps)} tiles, {doc['bytes']} B, {doc['build_s']} s")
    return frames, doc, stats


# ---------------------------------------------------------------- reading the spectrum

def get_bytes(store, key):
    """The object's bytes, or None when the key does not exist."""
    try:
        r = store.client.get_object(Bucket=store.bucket, Key=key)
    except Exception as exc:                             # noqa: BLE001
        if P._is_missing(exc):
            return None
        raise
    return r["Body"].read()


def spec_from_archive(run_dt, opener=urllib.request.urlopen):
    """The station's member straight from NOAA's archive (stops reading at the member) -> spec2d npz bytes."""
    import gfs_spectral as G
    raw = W.open_archive(f"{W.S3}/{W.archive_key(run_dt)}", opener)
    with raw:
        for sid, data in W.members(raw):
            if sid == STATION:
                return G.encode_spec2d(data, run_dt)
    raise ValueError(f"{STATION} is not in the archive")


# ---------------------------------------------------------------- publishing

def publish(store, run, frames, doc, atlas=None):
    if atlas is not None and not store.list_keys(atlas_key(doc["table"])):
        store.put(atlas_key(doc["table"]), atlas, "application/json", P.IMMUTABLE)
    for step in sorted(frames):
        store.put(frame_key(run, step), frames[step], "image/png", P.IMMUTABLE)
    store.put(index_key(run), json.dumps(doc, separators=(",", ":")).encode(), "application/json", P.IMMUTABLE)
    pointer = {"run": run, "index": index_key(run), "steps": len(frames), "table": doc["table"],
               "published_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    store.put(P.latest_key(PREFIX), json.dumps(pointer, sort_keys=True).encode(), "application/json", P.POINTER)
    return pointer


def prune(store, keep=KEEP):
    """Keep the newest `keep` runs that have an index (and the pointer's); a run without one that is newer than the
    newest whole run is being written and is left alone."""
    latest = store.get_json(P.latest_key(PREFIX)) or {}
    runs = {}
    for k in store.list_keys(PREFIX + "/"):
        name, _, rest = k[len(PREFIX) + 1:].partition("/")
        if name.isdigit() and len(name) == 10 and rest:
            runs.setdefault(name, False)
            if rest == "index.json":
                runs[name] = True
    whole = sorted(r for r, ok in runs.items() if ok)
    protect = set(whole[-keep:]) | ({latest["run"]} if latest.get("run") else set())
    newest = whole[-1] if whole else None
    deleted = []
    for run in sorted(runs):
        if run in protect or (not runs[run] and newest and run > newest):
            continue
        store.delete_prefix(f"{PREFIX}/{run}/")
        deleted.append(run)
    return deleted


def _write_out(out, frames, doc, stats):
    os.makedirs(out, exist_ok=True)
    for step, png in frames.items():
        with open(os.path.join(out, f"f{step:03d}.png"), "wb") as f:
            f.write(png)
    with open(os.path.join(out, "index.json"), "w", encoding="utf-8") as f:
        json.dump(dict(doc, stats={str(k): v for k, v in stats.items()}), f)


def main(argv=None, store=None, opener=urllib.request.urlopen):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--data", required=True, help="the data folder (table.npz, atlas.json, tides.json, manifest.json)")
    ap.add_argument("--run", help="YYYYMMDDHH (default: the spectral job's live run)")
    ap.add_argument("--spec", help="a spec2d npz file to read instead of the bucket's (implies --dry-run)")
    ap.add_argument("--from-archive", action="store_true", help="read the spectrum from NOAA's archive when the bucket has none")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--out", help="with --dry-run: write the frames and the index into this directory")
    ap.add_argument("--steps", help="comma-separated steps (a quick check; dry run only)")
    a = ap.parse_args(argv)
    live = not (a.dry_run or a.spec)
    if live and (a.out or a.steps):
        print("--out and --steps only with --dry-run")
        return 2
    if a.run and (len(a.run) != 10 or not a.run.isdigit()):
        print(f"{a.run}: not YYYYMMDDHH")
        return 2
    try:
        table, atlas, tides, man = load_data(a.data)
    except Exception as exc:                             # noqa: BLE001
        print(f"data folder: {exc.__class__.__name__}: {exc}")
        return 1
    run = a.run
    try:
        if a.spec:
            with open(a.spec, "rb") as f:
                raw = f.read()
            run = run or str(np.load(io.BytesIO(raw))["run"])
        else:
            if store is None:
                store = W._store_from_env()
            if not run:
                run = (store.get_json(P.latest_key(SPEC_PREFIX)) or {}).get("run")
                if not run:
                    print("the spectral job has no live run")
                    return 3
            latest = store.get_json(P.latest_key(PREFIX)) or {}
            if live:
                if latest.get("run") == run and latest.get("table") == table.version:
                    print(f"{run} already live")
                    return 0
                if latest.get("run", "") > run:
                    print(f"{run} is older than the live run {latest['run']}: nothing to do")
                    return 0
            raw = get_bytes(store, spec2d_key(run))
            if raw is None and a.from_archive:
                print(f"{run}: no spectrum in the bucket, reading NOAA's archive")
                raw = spec_from_archive(datetime.strptime(run, "%Y%m%d%H").replace(tzinfo=timezone.utc), opener)
            if raw is None:
                print(f"{run}: the spectral job kept no spectrum for {STATION} (a run from before this job)")
                return 3
        run_dt = datetime.strptime(run, "%Y%m%d%H").replace(tzinfo=timezone.utc)
        spec = np.load(io.BytesIO(raw))
        if str(spec["run"]) != run:
            raise ValueError(f"the spectrum is of run {spec['run']}, not {run}")
        steps = [int(s) for s in a.steps.split(",")] if a.steps else STEPS
        frames, doc, stats = build_run(table, tides, spec, run_dt, steps)
        if not live:
            if a.out:
                _write_out(a.out, frames, doc, stats)
            print("dry run: nothing published")
            return 0
        publish(store, run, frames, doc, atlas)
    except Exception as exc:                             # noqa: BLE001
        if P.is_storage_busy(exc):
            print(f"storage busy ({exc.__class__.__name__}): the next tick tries again")
            return 3
        line = f"FAILED: nearshore {run}: {exc.__class__.__name__}: {P.redact(str(exc), getattr(store, 'bucket', None))}"
        print(line)
        W._summary(line)
        return 1
    deleted = P.safe_prune(lambda: prune(store), store.bucket, W._summary)
    line = f"nearshore {run}: {len(frames)} steps, {doc['bytes']} B, {doc['build_s']} s; pruned {deleted or 'nothing'}"
    print(line)
    W._summary(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
