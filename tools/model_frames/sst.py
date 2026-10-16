"""Sea surface temperature tiles job (plan section 44): the newest NASA JPL MUR SST v4.1 day -> R2 under sst/v1/.

Source: MUR v4.1 (GHRSST L4, doi:10.5067/GHGMR-4FJ04) through NOAA CoastWatch's ERDDAP mirror (anonymous HTTPS; one
NetCDF-4 file per day, ~750 MB, nominal 09Z, served ~26 h after it). Free and redistributable; credit "NASA JPL MUR
SST v4.1 via NOAA CoastWatch ERDDAP". The tiles (sst_tiles.py) and the layout:
  sst/v1/<YYYYMMDD>/t0.png                 the whole globe at 0.25 deg (1440 x 721)        (immutable)
  sst/v1/<YYYYMMDD>/t1/<lat0>_<lon0>.png   0.05 deg, 20-deg cells (402 x 402)               (immutable)
  sst/v1/<YYYYMMDD>/t2/<lat0>_<lon0>.png   0.01 deg, 5-deg cells (502 x 502)                (immutable)
  sst/v1/<YYYYMMDD>/stats-<published>.json, manifest-<published>.json                       (immutable)
  sst/v1/latest.json  -> {format, day, run, manifest, published_utc, complete}   (max-age=300, written LAST)
  sst/v1/failed/<YYYYMMDD>.json, sst/v1/stale-alarm.json
Exit codes: 0 published / nothing new; 1 failure (a listing, download or build error; a stale source, at most once in
STALE_ALARM_EVERY_H); 2 bad arguments.
"""
import argparse
import json
import os
import re
import sys
import tempfile
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import encode as E       # noqa: E402  (PNG writer)
import publish as P      # noqa: E402  (Store, R2 client, cache policies, redact)
import sst_tiles as T    # noqa: E402

PREFIX = "sst/v1"
FORMAT = "sst-v1"
ERDDAP = "https://coastwatch.pfeg.noaa.gov/erddap/files/jplMURSST41"
LISTING = ERDDAP + "/.csv"
FILE_RE = re.compile(r"^(\d{8})090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02\.0-fv04\.1\.nc$")
UA = "allshoresurf.com sst tiles (https://allshoresurf.com)"
SOURCE = {"name": "NASA JPL MUR SST v4.1 (GHRSST L4)", "doi": "10.5067/GHGMR-4FJ04",
          "url": "https://podaac.jpl.nasa.gov/dataset/MUR-JPL-L4-GLOB-v4.1",
          "via": "NOAA CoastWatch ERDDAP (jplMURSST41)",
          "credit": "SST: NASA JPL MUR v4.1 (doi:10.5067/GHGMR-4FJ04) via NOAA CoastWatch ERDDAP; tiled by Allshore Surf"}
MAX_FILE_BYTES = 1_500_000_000          # a day is ~750 MB; anything far larger is not a MUR day
MIN_FILE_BYTES = 300_000_000
DOWNLOAD_TRIES = 4
CELL_WORKERS = 4
UPLOAD_WORKERS = 8
UPLOAD_BACKLOG = 64
KEEP_DAYS = 3
STALE_DAYS = 4                          # a newest day older than this is an outage (MUR or the mirror)
STALE_ALARM_EVERY_H = 12
FAILED_RETRY_AFTER_S = 3600
FAILED_MAX_ATTEMPTS = 6
PNG = "image/png"
JSON = "application/json"


def _summary(line):
    print(line)
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")


# ---------------------------------------------------------------- keys
def day_dir(day):
    return f"{PREFIX}/{day}"


def latest_key():
    return f"{PREFIX}/latest.json"


def tile_key(day, tier, name=None):
    return f"{day_dir(day)}/t0.png" if tier == "t0" else f"{day_dir(day)}/{tier}/{name}.png"


def manifest_key(day, published):
    return f"{day_dir(day)}/manifest-{published.replace('-', '').replace(':', '')}.json"


def stats_key(day, published):
    return f"{day_dir(day)}/stats-{published.replace('-', '').replace(':', '')}.json"


def failed_key(day):
    return f"{PREFIX}/failed/{day}.json"


def stale_key():
    return f"{PREFIX}/stale-alarm.json"


def iso_day(day):
    return f"{day[:4]}-{day[4:6]}-{day[6:]}"


# ---------------------------------------------------------------- the source
def _get(url, headers=None, timeout=120):
    req = urllib.request.Request(url, headers=dict({"User-Agent": UA}, **(headers or {})))
    return urllib.request.urlopen(req, timeout=timeout)


def parse_listing(text):
    """ERDDAP's files/.csv listing -> [(day YYYYMMDD, file name, bytes)] for MUR day files, oldest first."""
    out = []
    for line in text.splitlines():
        parts = line.split(",")
        if len(parts) < 3:
            continue
        m = FILE_RE.match(parts[0].strip())
        if not m:
            continue
        try:
            size = int(parts[2])
        except ValueError:
            continue
        out.append((m.group(1), parts[0].strip(), size))
    return sorted(out)


def listing(get=_get):
    with get(LISTING, timeout=90) as r:
        return parse_listing(r.read().decode("utf-8", "replace"))


def download(name, size, dest, get=_get, tries=DOWNLOAD_TRIES, log=print):
    """Stream the day's file to `dest`, resuming a cut transfer with a Range request; the result must be exactly the
    listed size."""
    if not (MIN_FILE_BYTES <= size <= MAX_FILE_BYTES):
        raise ValueError(f"{name}: listed size {size} is not a MUR day")
    have = os.path.getsize(dest) if os.path.exists(dest) else 0
    last = None
    for attempt in range(1, tries + 1):
        if have >= size:
            break
        try:
            hdr = {"Range": f"bytes={have}-"} if have else {}
            with get(f"{ERDDAP}/{name}", headers=hdr, timeout=300) as r, open(dest, "ab" if have else "wb") as fh:
                if have and getattr(r, "status", 206) != 206:          # the server ignored the range: start again
                    fh.seek(0)
                    fh.truncate()
                    have = 0
                while True:
                    chunk = r.read(1 << 22)
                    if not chunk:
                        break
                    fh.write(chunk)
                    have += len(chunk)
                    if have > size:
                        raise ValueError(f"{name}: more bytes than listed")
        except Exception as exc:                                       # noqa: BLE001 - resets, timeouts
            last = exc
            log(f"download attempt {attempt} stopped at {have} of {size} bytes: {exc.__class__.__name__}")
            time.sleep(min(30, 5 * attempt))
    if have != size:
        raise IOError(f"{name}: {have} of {size} bytes after {tries} tries ({last!r})")
    return dest


def read_mur(path, day, geo=None):
    """-> (sst raw int16, mask int8), both north-up, after checking the file is the expected MUR day on `geo`'s lattice
    (MUR: 100 pixels per degree; tests write smaller files)."""
    import h5py                                                        # the job env only (pinned)
    geo = geo or T.Geo()
    d = 1.0 / geo.px
    with h5py.File(path, "r") as f:
        sst_v, mask_v, lat, lon = f["analysed_sst"], f["mask"], f["lat"][:], f["lon"][:]
        if sst_v.shape != (1, geo.rows, geo.cols) or mask_v.shape != sst_v.shape:
            raise ValueError(f"unexpected shape {sst_v.shape}")
        if not (abs(lat[0] + 90 - d) < 1e-4 and abs(lat[-1] - 90 + d) < 1e-4 and abs(lon[0] + 180 - d) < 1e-4 and abs(lon[-1] - 180.0) < 1e-4):
            raise ValueError("unexpected lat/lon axes")
        a = sst_v.attrs
        if not (abs(float(a["scale_factor"][0]) - 0.001) < 1e-12 and abs(float(a["add_offset"][0]) - 298.15) < 1e-9 and int(a["_FillValue"][0]) == T.NONE):
            raise ValueError("unexpected analysed_sst scaling")
        t = int(f["time"][0])
        when = datetime(1981, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=t)
        if when.strftime("%Y%m%d%H") != day + "09":
            raise ValueError(f"file time {when.isoformat()} is not {day} 09Z")
        sst = np.ascontiguousarray(sst_v[0][::-1])
        mask = np.ascontiguousarray(mask_v[0][::-1])
    return sst, mask


# ---------------------------------------------------------------- the build
class Uploads:
    """PUTs running behind the build (the frames job's rule: each future asked done() ONCE; the first failure stops the
    build; at most `backlog` trail it). With store None nothing is sent (a dry run); `local` writes files instead."""

    def __init__(self, store=None, local=None, workers=UPLOAD_WORKERS, backlog=UPLOAD_BACKLOG):
        self.store, self.local, self.backlog, self.pending = store, local, backlog, []
        self.pool = ThreadPoolExecutor(workers) if store is not None else None
        self.count = self.bytes = 0

    def put(self, key, body, ctype=PNG, cache=P.IMMUTABLE):
        self.count += 1
        self.bytes += len(body)
        if self.local is not None:
            path = os.path.join(self.local, *key.split("/"))
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as fh:
                fh.write(body)
        if self.pool is not None:
            self.pending.append(self.pool.submit(self.store.put, key, body, ctype, cache))
            self.drain(self.backlog)

    def drain(self, limit=0):
        keep = []
        for f in self.pending:
            if f.done():
                f.result()
            else:
                keep.append(f)
        self.pending[:] = keep
        while len(self.pending) > limit:
            self.pending.pop(0).result()

    def close(self):
        for f in self.pending:
            f.cancel()
        if self.pool is not None:
            self.pool.shutdown(wait=False, cancel_futures=True)


def t0_grid(geo):
    """t0's geometry: at MUR's 100 pixels per degree, the overlay frames' grid (run.GRID_FULL) exactly."""
    res = geo.res(T.T0_STEP)
    return {"cols": 360 * geo.px // T.T0_STEP, "rows": 180 * geo.px // T.T0_STEP + 1, "lon0": -180.0, "lat0": 90.0,
            "dlon": res, "dlat": -res, "registration": "center", "lon_periodic": True}


def build(day, sst, mask, uploads, geo=None, file_name=None, log=print):
    """Tiles of one day from its north-up (sst, mask) -> the manifest (pointer not written). `sst` is consumed (turned
    into the value grid in place). The lattice follows the arrays (MUR: 100 pixels per degree)."""
    px = sst.shape[1] // 360
    geo = geo or T.Geo(px)
    if sst.shape != (geo.rows, geo.cols):
        raise ValueError(f"grid {sst.shape} is not a {geo.px}-per-degree lattice")
    t0 = time.time()
    counts = T.value_grid(sst, mask)
    del mask
    stats = {"day": day, "pixels": counts, "tiers": {}}
    tiers = []
    pool = ThreadPoolExecutor(CELL_WORKERS)
    try:
        for tier in T.TIERS:
            name, step, cell = tier["name"], tier["step"], tier["cell"]
            nodes = None
            if step > 1:
                nodes, land = T.node_means(sst, geo, step)
                nfilled = T.node_fill(nodes, land)

            def one(c, step=step, cell=cell, nodes=nodes):
                lat0, lon0 = c
                if nodes is None:
                    v, filled = T.t2_cell(sst, geo, lat0, lon0, cell)
                else:
                    v, filled = T.node_cell(nodes, geo, step, lat0, lon0, cell), 0
                if not np.isfinite(v).any():
                    return None
                lo, hi = T.cell_range(v)
                return c, lo, hi, E.to_png(T.quantize(v, lo, hi)), filled

            cells, nbytes, filled_px, steps_ = {}, 0, 0, []
            for r in pool.map(one, T.cell_names(cell)):
                if r is None:
                    continue
                (lat0, lon0), lo, hi, png, filled = r
                cname = T.cell_name(lat0, lon0)
                uploads.put(tile_key(day, name, cname), png)
                cells[cname] = [lo, hi, len(png)]
                nbytes += len(png)
                filled_px += filled
                steps_.append((hi - lo) / 254.0)
            n = cell * geo.px // step
            tiers.append({"name": name, "cell": cell, "res": geo.res(step), "px": n, "apron": 1,
                          "dir": f"{day_dir(day)}/{name}", "cells": cells})
            stats["tiers"][name] = {"cells": len(cells), "bytes": nbytes, "max_step_c": round(max(steps_), 4),
                                    "mean_step_c": round(float(np.mean(steps_)), 4),
                                    "filled": filled_px if step == 1 else nfilled}
            log(f"{name}: {len(cells)} cells, {nbytes / 1e6:.1f} MB, max step {max(steps_):.4f} C ({time.time() - t0:.0f} s)")
        n0, l0 = T.node_means(sst, geo, T.T0_STEP)
        f0 = T.node_fill(n0, l0)
        lo, hi = T.RANGE
        q0 = T.quantize(n0, lo, hi)
        png0 = E.to_png(q0)
        uploads.put(tile_key(day, "t0"), png0)
        fin = np.isfinite(n0)
        stats["tiers"]["t0"] = {"bytes": len(png0), "filled": f0, "nodes": int(fin.sum()),
                                "clamped_low": int((n0[fin] < lo).sum()), "clamped_high": int((n0[fin] > hi).sum()),
                                "min_c": round(float(np.nanmin(n0)), 3), "max_c": round(float(np.nanmax(n0)), 3)}
        log(f"t0: {len(png0) / 1e3:.0f} KB ({time.time() - t0:.0f} s)")
        uploads.drain(0)                                              # every tile stored before the manifest
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    published = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    grid = t0_grid(geo)
    if (grid["rows"], grid["cols"]) != n0.shape:
        raise ValueError("t0 grid does not match its nodes")
    manifest = {
        "format": FORMAT, "day": iso_day(day), "run": day, "complete": True, "published_utc": published,
        "build_seconds": round(time.time() - t0, 1), "units": "degC", "range": list(T.RANGE),
        "source": dict(SOURCE, file=file_name, time_utc=f"{iso_day(day)}T09:00:00Z"),
        "encoding": "u8: code 0 = no value, else v = lo + (code - 1) / 254 * (hi - lo) deg C",
        "geometry": "cell '<lat0>_<lon0>' covers lat0 <= lat < lat0 + cell, lon0 <= lon < lon0 + cell; its PNG is "
                    "(px + 2) x (px + 2): row r / column c centred at lat = lat0 + cell - r * res, "
                    "lon = lon0 + (c - 1) * res (rows north to south; a one-node apron shared with the neighbours)",
        "values": "open sea; MUR's own coastal values within 3 km of open sea; a 2-pixel mean fill over other land "
                  "(t2), a 1-node fill (t1, t0); lakes and ice-covered water left empty",
        "tier0": {"key": tile_key(day, "t0"), "lo": T.RANGE[0], "hi": T.RANGE[1], "grid": grid},
        "tiers": tiers,
    }
    stats["build_seconds"] = manifest["build_seconds"]
    return manifest, stats


def publish_manifest(store, day, manifest, stats):
    """Stats sidecar, the immutable manifest, then the pointer (last)."""
    skey = stats_key(day, manifest["published_utc"])
    store.put(skey, json.dumps(stats, separators=(",", ":"), sort_keys=True, allow_nan=False).encode(), JSON, P.IMMUTABLE)
    body = dict(manifest, stats=skey)
    mkey = manifest_key(day, manifest["published_utc"])
    store.put(mkey, json.dumps(body, separators=(",", ":"), sort_keys=True, allow_nan=False).encode(), JSON, P.IMMUTABLE)
    pointer = {"format": FORMAT, "day": manifest["day"], "run": day, "manifest": mkey, "complete": True,
               "published_utc": manifest["published_utc"]}
    store.put(latest_key(), json.dumps(pointer, separators=(",", ":"), sort_keys=True).encode(), JSON, P.POINTER)
    return mkey, pointer


def list_days(store):
    """{day: has_manifest} for every day directory under the prefix."""
    days = {}
    for k in store.list_keys(PREFIX + "/"):
        rest = k[len(PREFIX) + 1:]
        name = rest.split("/", 1)[0]
        if name.isdigit() and len(name) == 8:
            days.setdefault(name, False)
            if rest.startswith(f"{name}/manifest-"):
                days[name] = True
    return days


def prune(store, keep=KEEP_DAYS):
    """Keep the newest `keep` complete days and the one the pointer names; never a manifest-less day newer than the
    newest complete one (a build in progress). Only under sst/v1/<8 digits>/."""
    if keep < 1:
        raise ValueError("keep must be >= 1")
    latest = store.get_json(latest_key()) or {}
    days = list_days(store)
    complete = sorted(d for d, ok in days.items() if ok)
    protect = set(complete[-keep:]) | ({latest["run"]} if latest.get("run") else set())
    newest = complete[-1] if complete else None
    deleted = []
    for d in sorted(days):
        if d in protect or (not days[d] and newest and d >= newest):
            continue
        store.delete_prefix(f"{day_dir(d)}/")
        deleted.append(d)
    return deleted


def record_failure(store, day, message):
    prev = store.get_json(failed_key(day)) or {"day": day, "attempts": 0}
    prev["attempts"] += 1
    prev["last_attempt_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    prev["last_error"] = P.redact(message, store.bucket)
    store.put(failed_key(day), json.dumps(prev, sort_keys=True).encode(), JSON, P.POINTER)
    return prev


def skip_failed(store, day, now=None):
    rec = store.get_json(failed_key(day))
    if not rec:
        return False
    if rec.get("attempts", 0) >= FAILED_MAX_ATTEMPTS:
        return True
    last = datetime.strptime(rec["last_attempt_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return ((now or datetime.now(timezone.utc)) - last).total_seconds() < FAILED_RETRY_AFTER_S


def stale_check(store, newest_day, now=None):
    """1 when the newest MUR day is older than STALE_DAYS and no alarm went out in STALE_ALARM_EVERY_H (GitHub mails a
    failed run), else None."""
    now = now or datetime.now(timezone.utc)
    age_d = (now - datetime.strptime(newest_day, "%Y%m%d").replace(tzinfo=timezone.utc)).total_seconds() / 86400
    if age_d <= STALE_DAYS:
        return None
    rec = store.get_json(stale_key()) or {}
    last = rec.get("last_alarm_utc")
    if last and (now - datetime.strptime(last, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)).total_seconds() < STALE_ALARM_EVERY_H * 3600:
        print(f"newest MUR day {newest_day} is {age_d:.1f} days old (alarm already sent)")
        return None
    store.put(stale_key(), json.dumps({"last_alarm_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "newest_day": newest_day}).encode(), JSON, P.POINTER)
    _summary(f"STALE: the newest MUR day on ERDDAP is {newest_day} ({age_d:.1f} days old)")
    return 1


def _store_from_env():
    env = os.environ
    client = P.r2_client(env["R2_ACCOUNT_ID"], env["R2_ACCESS_KEY_ID"], env["R2_SECRET_ACCESS_KEY"])
    return P.Store(client, env["R2_BUCKET"])


def main(argv=None, get=_get, store=None, reader=read_mur):
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="build, upload nothing")
    ap.add_argument("--local", default=None, help="also write every object under this folder")
    ap.add_argument("--file", default=None, help="a MUR file already on disk (with --day)")
    ap.add_argument("--day", default=None, help="YYYYMMDD (default: the newest on ERDDAP)")
    ap.add_argument("--force", action="store_true", help="publish even if the day is live")
    ap.add_argument("--keep", type=int, default=KEEP_DAYS)
    a = ap.parse_args(argv)
    if a.keep < 1 or (a.day and not re.fullmatch(r"\d{8}", a.day)) or (a.file and not a.day):
        print("bad arguments (--keep >= 1; --day YYYYMMDD; --file needs --day)")
        return 2

    if a.file:
        day, name, size = a.day, os.path.basename(a.file), os.path.getsize(a.file)
    else:
        try:
            days = listing(get)
        except Exception as exc:                                       # noqa: BLE001
            print(f"ERDDAP listing failed: {exc.__class__.__name__}: {exc}")
            return 1
        if a.day:
            days = [d for d in days if d[0] == a.day]
        if not days:
            print("no MUR day file in the listing" + (f" for {a.day}" if a.day else ""))
            return 1
        day, name, size = days[-1]

    live = a.dry_run is False and a.local is None
    if live and store is None:
        store = _store_from_env()
    if live:
        latest = store.get_json(latest_key()) or {}
        if latest.get("run") and latest["run"] >= day and not a.force:
            print(f"day {latest['run']} is live; nothing newer ({day})")
            return stale_check(store, day) or 0
        if not a.force and skip_failed(store, day):
            print(f"day {day} failed recently; waiting before retrying")
            return 0
    print(f"building MUR day {day} ({name}, {size / 1e6:.0f} MB){' [dry-run]' if not live else ''}")

    uploads = Uploads(store if live else None, local=a.local)
    tmp = None
    try:
        path = a.file
        if not path:
            tmp = tempfile.TemporaryDirectory()
            t = time.time()
            path = download(name, size, os.path.join(tmp.name, name), get=get)
            print(f"downloaded {size / 1e6:.0f} MB in {time.time() - t:.0f} s")
        sst, mask = reader(path, day)
        manifest, stats = build(day, sst, mask, uploads, file_name=name)
        del sst, mask
        if live:
            mkey, _ = publish_manifest(store, day, manifest, stats)
        elif a.local is not None:
            for key, obj in ((stats_key(day, manifest["published_utc"]), stats), (manifest_key(day, manifest["published_utc"]), manifest)):
                uploads.put(key, json.dumps(obj, separators=(",", ":"), sort_keys=True, allow_nan=False).encode(), JSON)
            pointer = {"format": FORMAT, "day": manifest["day"], "run": day, "complete": True,
                       "manifest": manifest_key(day, manifest["published_utc"]), "published_utc": manifest["published_utc"]}
            uploads.put(latest_key(), json.dumps(pointer, sort_keys=True).encode(), JSON, P.POINTER)
    except Exception as exc:                                           # noqa: BLE001
        if live:
            rec = record_failure(store, day, exc)
            print(f"build failed (attempt {rec['attempts']}): {exc!r}")
        raise
    finally:
        uploads.close()
        if tmp is not None:
            tmp.cleanup()
    if live:
        print("pruned:", prune(store, keep=a.keep))
    tt = stats["tiers"]
    _summary(f"### MUR day {day}: t2 {tt['t2']['cells']} cells, t1 {tt['t1']['cells']} cells, "
             f"{uploads.bytes / 1e6:.1f} MB, {manifest['build_seconds']} s, steps <= {max(tt['t2']['max_step_c'], tt['t1']['max_step_c']):.3f} C")
    _summary(f"pixels: {stats['pixels']}; coastal fill t2 {tt['t2']['filled']}, t1 {tt['t1']['filled']} nodes, t0 {tt['t0']['filled']} nodes; "
             f"t0 {tt['t0']['min_c']}..{tt['t0']['max_c']} C (clamped {tt['t0']['clamped_low']} low / {tt['t0']['clamped_high']} high)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
