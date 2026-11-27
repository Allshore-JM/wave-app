"""Publishes the Nearshore Analysis product to R2 under gfswave/nearshore/v1/ (plan section 109): for every step of the
wave overlay (209 steps to +384 h), the breaking zone and the break lines of Oahu's North Shore, region by region (Kaena
Point to Kahuku Point), from the GFS-Wave 2-D spectrum at buoy 51201, each region's SWAN transfer table and the predicted
tide.

Inputs
- the spectrum: gfswave/spectral/v1/<RUN>/spec2d/51201.npz, written by the spectral job (gfs_spectral.py). With
  --from-archive a run that has none is read from NOAA's archive directly (the stream stops at the member).
- the tables: static/nearshore/<SET>/manifest.json and <region>/table.npz in the bucket (nearshore_build.py), read by the
  manifest's sha256 pinned in the data folder. Where regions' cores overlap, the earlier region of the manifest owns the
  water; a region's picture holds only what it owns.
- the data folder (--data): tides.json (predicted highs and lows at Haleiwa) and manifest.json (the table set, the sha256
  of its manifest and of tides.json). It is a checkout of the data branch at a pinned commit.

Layout (immutable but for the pointer). The table set is part of the path: the same run built from another set is
another set of objects, never the same keys with other bytes.
  gfswave/nearshore/v1/<RUN>/<SET>/<region>/f<HHH>.png   one 8-bit grey PNG per region and step, 256 wide: a header row, one
                                                          256x256 height plane per z14 tile (the index lists them), then
                                                          the lines' vertex table
  gfswave/nearshore/v1/<RUN>/<SET>/index.json             steps, regions (id, box, tiles), units, versions
  gfswave/nearshore/v1/latest.json                        -> {run, set, index}, written last
  gfswave/nearshore/v1/failed/<RUN>.<SET>.json            the attempts of a build that failed (again after an hour, six times)

Usage
  python tools/model_frames/nearshore.py --data D                 # publish the spectral job's live run, if not live here
  python tools/model_frames/nearshore.py --data D --dry-run --out O [--spec FILE.npz] [--tables DIR] [--run YYYYMMDDHH]
      (--tables DIR: a local copy of the bucket's static/nearshore/<SET>/ folder, for a run without the bucket)

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
FORMAT = "nearshore-v2"
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


def frame_key(run, tset, rid, step):
    return f"{PREFIX}/{run}/{tset}/{rid}/f{step:03d}.png"


def index_key(run, tset):
    return f"{PREFIX}/{run}/{tset}/index.json"


def failed_label(run, tset):
    return f"{run}.{tset}"


def spec2d_key(run, sid=STATION):
    return f"{SPEC_PREFIX}/{run}/spec2d/{sid}.npz"


def tables_key(tset, name="manifest.json"):
    return f"{STATIC_PREFIX}/{tset}/{name}"


# ---------------------------------------------------------------- the data folder

def load_data(folder):
    """-> (tides dict, manifest). The manifest names the table set and pins the sha256 of its manifest and of tides.json."""
    with open(os.path.join(folder, "manifest.json"), encoding="utf-8") as f:
        man = json.load(f)
    with open(os.path.join(folder, "tides.json"), "rb") as f:
        raw = f.read()
    if hashlib.sha256(raw).hexdigest() != man["sha256"]["tides.json"]:
        raise ValueError("tides.json: sha256 does not match the manifest")
    t = man.get("tables") or {}
    if not isinstance(t.get("set"), str) or not isinstance(t.get("manifest_sha256"), str) or len(t["manifest_sha256"]) != 64:
        raise ValueError("the data manifest names no table set")
    return json.loads(raw), man


# ---------------------------------------------------------------- the regions' tables

def load_tables(fetch, man):
    """The table set's manifest from the bucket (fetch(key) -> bytes or None), checked against the data folder's pin."""
    tset = man["tables"]["set"]; raw = fetch(tables_key(tset))
    if raw is None:
        raise ValueError(f"table set {tset}: no manifest in the bucket")
    if hashlib.sha256(raw).hexdigest() != man["tables"]["manifest_sha256"]:
        raise ValueError(f"table set {tset}: the manifest is not the pinned one")
    doc = json.loads(raw)
    if doc.get("format") != "nearshore-tables-v1" or doc.get("set") != tset or not doc.get("regions"):
        raise ValueError(f"table set {tset}: not a table manifest")
    return doc


def region_xy(facts, lon, lat):
    """lon, lat -> the region's own metres about its ref (x alongshore, y offshore)."""
    ky = math.pi / 180.0 * 6371008.8; kx = ky * math.cos(math.radians(facts["ref"][1])); a = math.radians(facts["alpha"])
    e = (np.asarray(lon) - facts["ref"][0]) * kx; n = (np.asarray(lat) - facts["ref"][1]) * ky
    return e * math.cos(a) + n * math.sin(a), -e * math.sin(a) + n * math.cos(a)


def in_core(facts, dx, lon, lat):
    """True where lon, lat lies in the band the region owns: its core alongshore, its kept rows offshore."""
    x, y = region_xy(facts, lon, lat); e = 0.01          # a centimetre: a cell on the edge itself is inside
    return (x >= facts["core"][0] - e) & (x <= facts["core"][1] + e) & (y >= facts["y0"] - e) & (y <= facts["y0"] + (facts["rows"] - 1) * dx + e)


def owner(facts, earlier, dx):
    """owns(lon, lat) -> True where the point is in this region's core and in no earlier region's of the manifest.
    The one rule for a map pixel, a grid cell and a line's vertex, so neighbouring regions meet without a seam."""
    def owns(lon, lat):
        ok = in_core(facts, dx, lon, lat)
        for other in earlier:
            ok = ok & ~in_core(other, dx, lon, lat)
        return ok
    return owns


def open_region(fetch, doc, k):
    """Region k of the table manifest, ready to build: its table (sha256 checked), what it owns, its map tiles."""
    facts = doc["regions"][k]; raw = fetch(facts["table"])
    if raw is None:
        raise ValueError(f"region {facts['id']}: no table in the bucket")
    if hashlib.sha256(raw).hexdigest() != facts["sha256"]:
        raise ValueError(f"region {facts['id']}: the table is not the manifest's")
    table = E.Table.from_npz(io.BytesIO(raw))
    if list(table.F) != list(doc["freqs"]) or list(table.D) != [float(d) for d in doc["dirs"]]:
        raise ValueError(f"region {facts['id']}: the table's bins are not the set's")
    owns = owner(facts, doc["regions"][:k], doc["dx"])
    X, Y = np.meshgrid(np.arange(table.nx) * table.dx, np.arange(table.ny) * table.dx)
    maps, box = tile_maps(table, owns)
    return dict(id=facts["id"], table=table, own=owns(*table.lonlat(X, Y)), owns=owns, maps=maps, box=box)


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


def tides_end(tides):
    return datetime.fromtimestamp(tides["hilo"][-1][0], timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if tides.get("hilo") else None


def tide_warning(tides, run_dt, horizon_h=STEPS[-1], margin_days=60):
    """A line for the summary when the tide file ends within margin_days of the run's last step, else None."""
    if not tides.get("hilo"):
        return "WARNING: nearshore: the tide file is empty; mean sea level is used"
    left = (tides["hilo"][-1][0] - (run_dt + timedelta(hours=horizon_h)).timestamp()) / 86400.0
    if left < 0:
        return f"WARNING: nearshore: the tide file ended {tides_end(tides)}; steps past it use mean sea level. Rebuild the data folder."
    if left < margin_days:
        return f"WARNING: nearshore: the tide file ends {tides_end(tides)} ({left:.0f} days of forecasts left). Rebuild the data folder."
    return None


# ---------------------------------------------------------------- map tiles

def lonlat_to_tile(lon, lat, z=Z):
    n = 2.0 ** z
    return (lon + 180.0) / 360.0 * n, (1.0 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2.0 * n


def tile_pixels(x, y, z=Z):
    """lon[256], lat[256] of the pixel centres of tile (x, y)."""
    n = 2.0 ** z; k = (np.arange(TILE) + 0.5) / TILE
    return (x + k) / n * 360.0 - 180.0, np.degrees(np.arctan(np.sinh(math.pi * (1 - 2 * (y + k) / n))))


def tile_maps(table, owns=None):
    """The z14 tiles that hold wet water the region owns (owns(lon, lat), asked at every pixel's centre; None = all), and
    for each the grid cell under every pixel: -> [(x, y, flat index array[256, 256] into the grid, -1 outside or not
    owned)], sorted, and the band's box (w, s, e, n)."""
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
            if owns is not None:
                idx = np.where(owns(plon[None, :], plat[:, None]), idx, -1)
            if (table.dep.reshape(-1)[idx[idx >= 0]] > 0).any():
                out.append((tx, ty, idx))
    return out, box


# ---------------------------------------------------------------- one step

def trains_of(freqs, dirs, Es, wind, F, D):
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
        Wt, _ = E.weights(freqs, dirs, Es, F, D, mask=lab == p["label"])
        trains.append(dict(W=Wt, tp=float(p["tp"])))
    rest = ~np.isin(lab, list(keep))
    if rest.any():
        Wt, _ = E.weights(freqs, dirs, Es, F, D, mask=rest)
        others = [p for p in parts if p["label"] not in keep]
        if Wt.sum() > 0 and others:
            trains.append(dict(W=Wt, tp=float(others[0]["tp"])))
    _, lost = E.weights(freqs, dirs, Es, F, D)
    trains = [t for t in trains if t["W"].sum() > 0]
    return trains, (float(parts[0]["hs"]), float(parts[0]["tp"]), float(parts[0]["dir"])), float(lost)


def pack_vertices(lines, table, box):
    """The break lines as VERTEX_BYTES per vertex: x, y (uint16 shares of the box, west-east and south-north), peel
    (deg in bits 0-6; bit 7 = the break runs along the line in the order of its vertices), flags (bit 0 right, bit 1 mixed swell, bits 2-3 breaker, bits 4-6 band, bit 7 first vertex of a line),
    ride length (5 m units), breaking height (HEIGHT_STEP_M units), score (0-100), energy share (percent)."""
    n = int(lines["x"].size)
    if n == 0:
        return b"", 0
    if n > MAX_VERTICES:                                   # never in the pilot; keep the frame bounded: whole lines go, the
        ids, start, count = np.unique(lines["line"], return_index=True, return_counts=True)   # ones with the best rides stay
        best = np.array([lines["score"][s:s + c].max() for s, c in zip(start, count)])
        keep_ids, total = set(), 0
        for k in np.argsort(-best, kind="stable"):
            if total + count[k] <= MAX_VERTICES:
                keep_ids.add(ids[k]); total += int(count[k])
        keep = np.isin(lines["line"], list(keep_ids))
        lines = {k: v[keep] for k, v in lines.items()}; n = int(keep.sum())
        if n == 0:
            return b"", 0
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


def clip_lines(lines, keep):
    """The vertices the region owns (keep[n]); a line cut by the edge becomes separate lines (never a join across the gap)."""
    n = int(lines["x"].size)
    if n == 0 or keep is None:
        return lines
    keep = np.asarray(keep, bool)
    if keep.all():
        return lines
    prev_kept = np.concatenate(([False], keep[:-1])); same = np.concatenate(([False], lines["line"][1:] == lines["line"][:-1]))
    starts = keep & ~(prev_kept & same)                    # the first kept vertex of a line, or the first after a gap
    out = {k: v[keep] for k, v in lines.items()}
    out["line"] = (np.cumsum(starts)[keep] - 1).astype(float)
    return out


def build_frame(table, maps, box, trains, buoy, lost, tide, own=None, owns=None):
    """One region's step -> (PNG bytes, stats dict). trains, buoy, lost = trains_of() of the step's spectrum;
    own[ny, nx] / owns(lon, lat) = what the region owns (None = everything)."""
    planes = bytearray(); nvert = 0; vbytes = b""; zone_cells = 0; hmax = 0.0
    if trains:
        r = E.analyse(table, trains, tide)
        zone = r["zone"] if own is None else r["zone"] & own
        code = np.where(zone, np.clip(np.rint(r["hset"] / HEIGHT_STEP_M), 1, 255), 0).astype(np.uint8).reshape(-1)
        zone_cells = int(zone.sum()); hmax = float(r["hset"][zone].max()) if zone_cells else 0.0
        for _, _, idx in maps:
            planes += np.where(idx >= 0, code[np.maximum(idx, 0)], 0).astype(np.uint8).tobytes()
        L = r["lines"]
        vbytes, nvert = pack_vertices(clip_lines(L, owns(*table.lonlat(L["x"], L["y"])) if owns is not None and L["x"].size else None), table, box)
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


def build_run(doc, fetch, tides, spec, run_dt, steps=STEPS, log=print):
    """Every region's frame of every step -> ({(region id, step): png}, index document, stats).
    doc = the table set's manifest, fetch(key) -> bytes, spec = the opened spec2d npz. The step's swell trains are split
    once and used by every region; one region's table is in memory at a time."""
    freqs = np.asarray(spec["freqs"], float); dirs = np.asarray(spec["dirs"], float)
    hours = {int(h): n for n, h in enumerate(np.asarray(spec["hours"]))}
    t0, no_tide, per_step = time.monotonic(), 0, {}
    for step in steps:
        n = hours.get(step)
        if n is None:
            continue
        try:
            tide = tide_msl(tides, run_dt + timedelta(hours=step))
        except ValueError:                               # past the end of the tide file: mean sea level, and said so
            tide, no_tide = 0.0, no_tide + 1
        per_step[step] = (trains_of(freqs, dirs, np.asarray(spec["E"][n], float), (float(spec["u10"][n]), float(spec["udir"][n])),
                                    doc["freqs"], doc["dirs"]), tide)
    if len(per_step) < 0.9 * len(steps):
        raise ValueError(f"{len(per_step)} of {len(steps)} steps in the spectrum")
    frames, stats, regions = {}, {}, []
    for k in range(len(doc["regions"])):
        t1 = time.monotonic(); reg = open_region(fetch, doc, k)
        if not reg["maps"]:
            raise ValueError(f"region {reg['id']}: no map tile holds water it owns")
        nbytes, vmax, hmax = 0, 0, 0.0
        for step, ((trains, buoy, lost), tide) in per_step.items():
            png, st = build_frame(reg["table"], reg["maps"], reg["box"], trains, buoy, lost, tide, reg["own"], reg["owns"])
            frames[(reg["id"], step)] = png; stats[(reg["id"], step)] = st
            nbytes += len(png); vmax = max(vmax, st["vertices"]); hmax = max(hmax, st["hmax_m"])
        regions.append(dict(id=reg["id"], box=[round(v, 6) for v in reg["box"]], tiles=[[x, y] for x, y, _ in reg["maps"]],
                            bytes=nbytes, vertices_max=vmax, hmax_m=hmax))
        log(f"  {reg['id']}: {len(reg['maps'])} tiles, {nbytes} B, {time.monotonic() - t1:.0f} s")
        del reg
    out = {"format": FORMAT, "run": W.run_name(run_dt), "set": doc["set"], "station": STATION, "steps": sorted(per_step), "z": Z,
           "regions": regions, "height_step_m": HEIGHT_STEP_M, "vertex_bytes": VERTEX_BYTES, "bands": list(E.PARAMS["bands"]),
           "band_names": list(E.BAND_NAMES), "params": E.PARAMS["version"], "steps_without_tide": no_tide,
           "tides_end_utc": tides_end(tides), "bytes": sum(r["bytes"] for r in regions),
           "vertices_max": max(r["vertices_max"] for r in regions), "hmax_m": max(r["hmax_m"] for r in regions),
           "build_s": round(time.monotonic() - t0, 1), "built_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "note": "Modelled estimate from the GFS-Wave spectrum at buoy 51201. Not safety guidance."}
    log(f"nearshore {out['run']}: {len(per_step)} steps, {len(regions)} regions, {out['bytes']} B, {out['build_s']} s")
    return frames, out, stats


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


ARCHIVE_BUDGET_S = 6 * 60            # the archive fallback gives up after this long (the next try is an hour on)


def spec_from_archive(run_dt, opener=urllib.request.urlopen, budget_s=ARCHIVE_BUDGET_S):
    """The station's member straight from NOAA's archive (stops reading at the member) -> spec2d npz bytes.
    Raises BudgetExceeded past the budget: a slow stream must not hold the tables' workflow."""
    import gfs_spectral as G
    raw = W.open_archive(f"{W.S3}/{W.archive_key(run_dt)}", opener)
    with raw:
        for sid, data in W.members(raw, time.monotonic() + budget_s):
            if sid == STATION:
                return G.encode_spec2d(data, run_dt)
    raise ValueError(f"{STATION} is not in the archive")


# ---------------------------------------------------------------- publishing

def publish(store, run, frames, doc):
    tset = doc["set"]
    for rid, step in sorted(frames):
        store.put(frame_key(run, tset, rid, step), frames[(rid, step)], "image/png", P.IMMUTABLE)
    store.put(index_key(run, tset), json.dumps(doc, separators=(",", ":")).encode(), "application/json", P.IMMUTABLE)
    pointer = {"run": run, "index": index_key(run, tset), "steps": len(doc["steps"]), "set": tset, "regions": len(doc["regions"]),
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
            if rest.endswith("/index.json"):
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
    oldest = min(protect) if protect else None
    if oldest:                                           # failure records of runs older than every kept run
        old = [k for k in store.list_keys(f"{PREFIX}/failed/") if k.rsplit("/", 1)[-1][:10].isdigit() and k.rsplit("/", 1)[-1][:10] < oldest]
        if old:
            store.delete_keys(old, "old records")
    return deleted


def _skip_failed(store, run, tset):
    """True while a failed build of this run and table set waits for its next try (an hour), or has had its six."""
    rec = store.get_json(P.failed_key(failed_label(run, tset), PREFIX))
    if not rec:
        return False
    if rec.get("attempts", 0) >= W.FAILED_MAX_ATTEMPTS:
        return True
    last = datetime.strptime(rec["last_attempt_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - last).total_seconds() < W.FAILED_RETRY_AFTER_S


def _write_out(out, frames, doc, stats):
    for (rid, step), png in frames.items():
        os.makedirs(os.path.join(out, rid), exist_ok=True)
        with open(os.path.join(out, rid, f"f{step:03d}.png"), "wb") as f:
            f.write(png)
    with open(os.path.join(out, "index.json"), "w", encoding="utf-8") as f:
        json.dump(dict(doc, stats={f"{rid}/{step}": v for (rid, step), v in stats.items()}), f)


def local_fetch(folder, tset):
    """fetch(key) over a local copy of the bucket's static/nearshore/<SET>/ folder."""
    root = tables_key(tset, "")

    def fetch(key):
        path = os.path.join(folder, *key[len(root):].split("/")) if key.startswith(root) else None
        if not path or not os.path.exists(path):
            return None
        with open(path, "rb") as f:
            return f.read()
    return fetch


def main(argv=None, store=None, opener=urllib.request.urlopen):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--data", required=True, help="the data folder (tides.json, manifest.json)")
    ap.add_argument("--run", help="YYYYMMDDHH (default: the spectral job's live run)")
    ap.add_argument("--spec", help="a spec2d npz file to read instead of the bucket's (implies --dry-run)")
    ap.add_argument("--tables", help="a local copy of static/nearshore/<SET>/ to read instead of the bucket's")
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
        tides, man = load_data(a.data)
    except Exception as exc:                             # noqa: BLE001
        print(f"data folder: {exc.__class__.__name__}: {exc}")
        return 1
    tset = man["tables"]["set"]; run = a.run
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
                if latest.get("index") == index_key(run, tset):                   # the pointer names this very index
                    print(f"{run} already live")
                    return 0
                if latest.get("run", "") > run:
                    print(f"{run} is older than the live run {latest['run']}: nothing to do")
                    return 0
                if store.get_json(index_key(run, tset)) is not None:              # built before (the pointer moved on and back)
                    print(f"{run} / {tset} is already published: its objects are immutable, nothing rewritten")
                    return 0
                if _skip_failed(store, run, tset):
                    print(f"{run} failed recently: waiting before another try")
                    return 3
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
        if a.tables:
            fetch = local_fetch(a.tables, tset)
        else:
            if store is None:
                store = W._store_from_env()
            fetch = lambda key: get_bytes(store, key)    # noqa: E731
        doc = load_tables(fetch, man)
        steps = [int(v) for v in a.steps.split(",")] if a.steps else STEPS
        frames, out, stats = build_run(doc, fetch, tides, spec, run_dt, steps)
        if not live:
            if a.out:
                _write_out(a.out, frames, out, stats)
            print("dry run: nothing published")
            return 0
        publish(store, run, frames, out)
    except Exception as exc:                             # noqa: BLE001
        if P.is_storage_busy(exc):
            print(f"storage busy ({exc.__class__.__name__}): the next tick tries again")
            return 3
        line = f"FAILED: nearshore {run}: {exc.__class__.__name__}: {P.redact(str(exc), getattr(store, 'bucket', None))}"
        print(line)
        W._summary(line)
        if live and run:                                 # counted: tried again after an hour, six times at most (as the tables' job)
            try:
                P.record_failure(store, failed_label(run, tset), f"{exc.__class__.__name__}: {exc}", PREFIX)
            except Exception as exc2:                    # noqa: BLE001
                print(f"the failure could not be recorded ({exc2.__class__.__name__})")
        return 1
    warn = tide_warning(tides, run_dt)
    if warn:
        print(warn)
        W._summary(warn)
    deleted = P.safe_prune(lambda: prune(store), store.bucket, W._summary)
    line = f"nearshore {run}: {len(out['steps'])} steps, {len(out['regions'])} regions, {out['bytes']} B, {out['build_s']} s; pruned {deleted or 'nothing'}"
    print(line)
    W._summary(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
