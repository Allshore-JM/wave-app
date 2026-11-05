"""Gridded satellite wave height job (plan section 82): the newest day of Copernicus Marine's level-4 gridded
significant wave height -> R2 under altgrid/v1/ as four PNG frames (00 / 06 / 12 / 18Z) on the model's half grid.

Source: WAVE_GLO_PHY_SWH_L4_NRT_014_003 (doi 10.48670/moi-00180), dataset
cmems_obs-wave_glo_phy-swh_nrt_multi-l4-0.5deg_PT6H-i: `VAVH_INST`, "an estimate of the instantaneous wave field every
6 h at 0.5 degrees, using all available level-3 along-track measurements and accounting for their spatial and temporal
proximity". Anonymous on the CloudFerro bucket the level-3 jobs read (alt.py: STAC first, the bucket's listing as the
fallback). One ~9 MB NetCDF-4 a day,
  cmems_obs-wave_glo_phy-swh_nrt_multi_l4_0.5deg_PT6H-i_<day>T120000Z_P<production YYYYMMDDTHHMMSS>Z.nc,
holding the four steps of <day>, produced about 07:30Z the next day (the day's older production is purged): the newest
step is 13-37 h behind the clock. Verified on the real file of 2026-11-03 (production 2026-11-04 07:30Z): time (4,)
float32 days since 1950-01-01, latitude (359,) -89.5 .. 89.5 ascending, longitude (720,) -180 .. 179.5, VAVH_INST
(4, 359, 720) int16 x 0.001 m (fill -32767), VAVH_INST_FLAG (1 = an invalid value thresholded to 0 m: absent here);
open ocean only (55 % of the cells; blank within ~50 km of coasts and under ice). Credit "Generated using E.U.
Copernicus Marine Service Information" + the DOI, in the manifest and the pointer.

The grid is the model frames' half grid (run.GRID_HALF: 720 x 361, lat0 90, lon0 -180, 0.5 deg, centre registration,
periodic in longitude) without its two pole rows: the job places the 359 rows at rows 1..359 north-up and leaves rows 0
and 360 absent, so the client draws it through ModelGridLayer unchanged (validateGrid wants rows reaching +-90). The
values are quantised on encode.FIELDS["hs"] (0 .. 22.86 m; the same colours as the model and the satellite dots), with
no coastal fill (the product's blanks are honest).

Layout (immutable keys but the pointer):
  altgrid/v1/<day YYYYMMDD>/<production YYYYMMDDTHHMMSSZ>/hs-<HH>.png        one step (HH 00 / 06 / 12 / 18)
  altgrid/v1/<day>/manifest-<published>.json, stats-<published>.json
  altgrid/v1/latest.json -> {format, day, run, manifest, complete, newest_utc, production, production_utc,
      published_utc (the last change), checked_utc (every run), datasets, datasets_utc, credit}   (max-age 300, LAST)
  altgrid/v1/failed/<day>.json, altgrid/v1/stale-alarm.json
A production stamp names one input file and the encoding is deterministic: --force re-publishes identical bytes under
the same frame keys (an encoding change is a new prefix version, as the model frames).
Exit codes: 0 published / nothing new / a quiet outage while the map is fresh; 1 a failure (recorded, backed off) or a
stale map (the newest step older than STALE_H, at most once per 12 h); 2 bad arguments.
"""
import argparse
import json
import os
import re
import sys
import tempfile
from datetime import datetime, timedelta, timezone

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import alt as A          # noqa: E402  (STAC + listing, download, failures, alarms, pointer writer)
import encode as E       # noqa: E402  (FIELDS["hs"], encode_frame)
import publish as P      # noqa: E402  (cache policies)

PREFIX = "altgrid/v1"
FORMAT = "altgrid-v1"
STAC_ID = "WAVE_GLO_PHY_SWH_L4_NRT_014_003"
# The 0.5-degree 6-hourly dataset only (a 2-degree daily mean sits beside it); one group: alt._dataset_token wants one.
DATASET_RE = re.compile(r"swh_nrt_(multi)-l4-0\.5deg_PT6H-i", re.I)
FILE_RE = re.compile(r"^cmems_obs-wave_glo_phy-swh_nrt_multi_l4_0\.5deg_PT6H-i_(?P<day>\d{8})T120000Z_P(?P<prod>\d{8}T\d{6})Z\.nc$")
SOURCE = {"name": "Copernicus Marine WAVE_GLO_PHY_SWH_L4_NRT_014_003 (L4 gridded significant wave height, 0.5 deg, 6-hourly)",
          "doi": "10.48670/moi-00180", "url": "https://doi.org/10.48670/moi-00180",
          "credit": "Generated using E.U. Copernicus Marine Service Information; https://doi.org/10.48670/moi-00180"}
STEPS = (0, 6, 12, 18)
L4_ROWS, L4_COLS = 359, 720
# = run.GRID_HALF without its "derivation" (pinned by the tests; run.py is not imported here: it pulls the GRIB stack)
GRID = {"cols": 720, "rows": 361, "lon0": -180.0, "lat0": 90.0, "dlon": 0.5, "dlat": -0.5,
        "registration": "center", "lon_periodic": True}
HS = E.FIELDS["hs"]
MIN_FILE_BYTES, MAX_FILE_BYTES = 1_000_000, 40_000_000      # a real day is ~9 MB
KEEP_DAYS = 3                                               # complete days kept (+ the pointer's)
LOOKBACK_DAYS = 4                                           # the days listed (this month and the last when it straddles)
STALE_H = 48                                                # the newest step older than this = an outage (normal lag 13-37 h)
SUPERSEDED_H = 2                                            # an older production of the live day goes this long after the pointer left it
MAX_VALUE_M = 30.0                                          # a value above this (or below 0) is a fault: absent, counted
TIME_SLACK_S = 300                                          # the float32 time axis (~2.8-min quantum at 2026) rounds to the hour within this
EPOCH_1950 = datetime(1950, 1, 1, tzinfo=timezone.utc)
JSON = "application/json"


def _stamp(dt):
    return dt.strftime("%Y%m%dT%H%M%SZ")


class L4Product(A.Product):
    """alt.py's Product for the listing / STAC / failure / alarm machinery: one record per DAY (t0 = the day's 00Z,
    t1 = +24 h), the production stamp from the file name (the newest production per day wins in alt.list_files)."""

    def parse_file(self, name, dataset_token=None):
        p = parse_file(name)
        if not p:
            return None
        day, prod_dt, _prod = p
        t0 = datetime.strptime(day, "%Y%m%d").replace(tzinfo=timezone.utc)
        return "l4", t0, t0 + timedelta(hours=24), prod_dt


PRODUCT = L4Product(prefix=PREFIX, fmt=FORMAT, magic=b"", stac_id=STAC_ID, dataset_re=DATASET_RE, file_re=FILE_RE, reader=None,
                    missions={}, source=SOURCE, columns=[], collocate=False, build=None, keep_h=LOOKBACK_DAYS * 24,
                    format_key="altgrid-v1/1", label="gridded satellite wave height", min_bytes=MIN_FILE_BYTES,
                    max_bytes=MAX_FILE_BYTES, token_from_file=False, stale_h=STALE_H)


# ---------------------------------------------------------------- names and keys
def parse_file(name):
    """-> (day 'YYYYMMDD', production datetime, production stamp 'YYYYMMDDTHHMMSSZ') for an L4 file name, else None."""
    m = FILE_RE.match(name or "")
    if not m:
        return None
    try:
        datetime.strptime(m.group("day"), "%Y%m%d")
        prod_dt = A.parse_stamp(m.group("prod"))
    except ValueError:
        return None
    return m.group("day"), prod_dt, m.group("prod") + "Z"


def day_dir(day):
    return f"{PREFIX}/{day}"


def frame_key(day, prod, hour):
    return f"{PREFIX}/{day}/{prod}/hs-{hour:02d}.png"


def _published_stamp(published_utc):
    return published_utc.replace("-", "").replace(":", "")


def manifest_key(day, published_utc):
    return f"{PREFIX}/{day}/manifest-{_published_stamp(published_utc)}.json"


def stats_key(day, published_utc):
    return f"{PREFIX}/{day}/stats-{_published_stamp(published_utc)}.json"


# ---------------------------------------------------------------- the listing
def list_days(datasets, now, get=A._get):
    """{day: FileRec} of the L4 files of the last LOOKBACK_DAYS (the newest production per day; sizes bounded)."""
    recs = A.list_files(PRODUCT, datasets, now, get=get, since=now - timedelta(days=LOOKBACK_DAYS))
    out = {}
    for r in recs:
        p = parse_file(r.name)
        if p and (p[0] not in out or r.prod > out[p[0]].prod):
            out[p[0]] = r
    return out


# ---------------------------------------------------------------- the reader
def read_l4(path, day):
    """An L4 NetCDF -> {valid: [datetime x 4], hs: float32 (4, 359, 720) NORTH-UP (row 0 = 89.5 N), NaN = absent,
    flagged, dropped}. Refuses (ValueError) a file whose axes, scale, fill, units or steps are not the verified ones."""
    import h5py
    with h5py.File(path, "r") as f:
        for name in ("time", "latitude", "longitude", "VAVH_INST"):
            if name not in f:
                raise ValueError(f"{name} not found")
        tv = f["time"]
        tunits = A._attr(tv, "units", "")
        if not (isinstance(tunits, str) and re.match(r"^days since 1950-01-01", tunits)):
            raise ValueError(f"time units {tunits!r} are not days since 1950-01-01")
        t = np.asarray(tv[()], dtype=np.float64)
        lat = A._scaled(f["latitude"], expect_units="degrees_north")
        lon = A._scaled(f["longitude"], expect_units="degrees_east")
        hs = A._scaled(f["VAVH_INST"], expect_scale=0.001, expect_fill=-32767, expect_units="m")
        flag = np.asarray(f["VAVH_INST_FLAG"][()]) if "VAVH_INST_FLAG" in f else None
    if t.shape != (len(STEPS),):
        raise ValueError(f"{t.shape[0] if t.ndim else 0} time steps, not {len(STEPS)}")
    d0 = datetime.strptime(day, "%Y%m%d").replace(tzinfo=timezone.utc)
    valid = []
    for x in t:
        dt = EPOCH_1950 + timedelta(days=float(x))
        r = (dt + timedelta(minutes=30)).replace(minute=0, second=0, microsecond=0)
        if abs((dt - r).total_seconds()) > TIME_SLACK_S:
            raise ValueError(f"time step {dt.isoformat()} is not on the hour")
        valid.append(r)
    if valid != [d0 + timedelta(hours=h) for h in STEPS]:
        raise ValueError(f"time steps {[v.isoformat() for v in valid]} are not 00 / 06 / 12 / 18Z of {day}")
    if lat.shape != (L4_ROWS,) or abs(lat[0] + 89.5) > 1e-4 or not np.allclose(np.diff(lat), 0.5, atol=1e-4):
        raise ValueError(f"latitude axis {lat.shape} {lat[:2].tolist() if lat.size else []} is not -89.5 .. 89.5 by 0.5")
    if lon.shape != (L4_COLS,) or abs(lon[0] + 180.0) > 1e-4 or not np.allclose(np.diff(lon), 0.5, atol=1e-4):
        raise ValueError(f"longitude axis {lon.shape} is not -180 .. 179.5 by 0.5")
    if hs.shape != (len(STEPS), L4_ROWS, L4_COLS):
        raise ValueError(f"VAVH_INST shape {hs.shape} is not {(len(STEPS), L4_ROWS, L4_COLS)}")
    flagged = 0
    if flag is not None:
        if flag.shape != hs.shape:
            raise ValueError(f"VAVH_INST_FLAG shape {flag.shape} differs")
        bad = (flag != 0) & np.isfinite(hs)
        flagged = int(bad.sum())
        hs = np.where(flag != 0, np.nan, hs)
    out_of = np.isfinite(hs) & ((hs < 0) | (hs > MAX_VALUE_M))
    dropped = int(out_of.sum())
    hs = np.where(out_of, np.nan, hs)
    return {"valid": valid, "hs": hs[:, ::-1, :].astype(np.float32), "flagged": flagged, "dropped": dropped}


def place_half(step):
    """A north-up (359, 720) step -> the half grid (361, 720): row r is latitude 90 - 0.5 r; rows 0 and 360 (the
    poles) absent."""
    step = np.asarray(step)
    if step.shape != (L4_ROWS, L4_COLS):
        raise ValueError(f"step shape {step.shape}")
    out = np.full((GRID["rows"], GRID["cols"]), np.nan)
    out[1:L4_ROWS + 1] = step
    return out


def encode_step(grid361):
    """-> (PNG bytes, stats) on FIELDS["hs"], without the coastal fill."""
    enc = E.encode_frame(grid361, "hs", fill=False, resolutions=("full",))
    return enc["full"], enc["stats"]


# ---------------------------------------------------------------- build and publish
def build(day, prod, prod_dt, data, store, local, published_utc, log=print):
    """The four frames (stored at once: immutable keys) -> (manifest without its stats key, stats sidecar)."""
    frames, steps = [], {}
    for k, vt in enumerate(data["valid"]):
        png, st = encode_step(place_half(data["hs"][k]))
        key = frame_key(day, prod, vt.hour)
        A.put_object(store, local, key, png, "image/png", P.IMMUTABLE)
        frames.append({"step": vt.hour, "valid_utc": A.utc(vt), "file": key, "valid_cells": st["valid_points"],
                       "max_m": st["max"], "clamped_high": st["clamped_high"]})
        steps[f"{vt.hour:02d}"] = st
        log(f"{day} {vt.hour:02d}Z: {st['valid_points']} cells, max {st['max']} m, {st['clamped_high']} clamped")
    field = {k: HS[k] for k in ("lo", "hi", "legend", "units", "interpolation")}
    manifest = {"format": FORMAT, "day": f"{day[:4]}-{day[4:6]}-{day[6:]}", "run": day, "production": prod,
                "production_utc": A.utc(prod_dt), "build": _stamp(datetime.now(timezone.utc)), "complete": True,
                "published_utc": published_utc, "encoding": E.ENCODING, "encoding_spec": E.ENCODING_SPEC, "grid": dict(GRID),
                "fields": {"hs": field}, "fill": None, "frames": frames, "newest_utc": frames[-1]["valid_utc"],
                "source": SOURCE, "credit": SOURCE["credit"]}
    stats = {"day": day, "production": prod, "steps": steps, "flagged": data.get("flagged", 0), "dropped": data.get("dropped", 0)}
    return manifest, stats


def publish(store, local, day, manifest, stats, pointer_extra):
    """The stats sidecar, the immutable manifest, then the pointer LAST -> (manifest key, pointer)."""
    pub = manifest["published_utc"]
    skey, mkey = stats_key(day, pub), manifest_key(day, pub)
    A.put_object(store, local, skey, json.dumps(stats, separators=(",", ":"), sort_keys=True, allow_nan=False).encode(), JSON, P.IMMUTABLE)
    body = dict(manifest, stats=skey)
    A.put_object(store, local, mkey, json.dumps(body, separators=(",", ":"), sort_keys=True, allow_nan=False).encode(), JSON, P.IMMUTABLE)
    pointer = dict(pointer_extra, format=FORMAT, day=manifest["day"], run=day, manifest=mkey, complete=True,
                   newest_utc=manifest["newest_utc"], production=manifest["production"],
                   production_utc=manifest["production_utc"], published_utc=pub, checked_utc=pub, credit=SOURCE["credit"])
    A.write_pointer(store, PRODUCT, pointer, local=local)
    return mkey, pointer


# ---------------------------------------------------------------- prune
def stored_days(store):
    """{day: has_manifest} for every 8-digit day directory under the prefix."""
    days = {}
    for k in store.list_keys(PREFIX + "/"):
        rest = k[len(PREFIX) + 1:]
        name = rest.split("/", 1)[0]
        if name.isdigit() and len(name) == 8:
            days.setdefault(name, False)
            if rest.startswith(f"{name}/manifest-"):
                days[name] = True
    return days


def prune(store, pointer, keep=KEEP_DAYS, now=None):
    """Keep the newest `keep` complete days and the pointer's day (never a manifest-less day newer than the newest
    complete one: a build in progress); inside the pointer's day, the other productions and manifests once the pointer
    has named its own for SUPERSEDED_H (a client holding the previous pointer still reads them meanwhile); failure
    records of days older than every kept one. Only under altgrid/v1/. -> the deleted day dirs and keys."""
    if keep < 1:
        raise ValueError("keep must be >= 1")
    now = now or datetime.now(timezone.utc)
    days = stored_days(store)
    complete = sorted(d for d, ok in days.items() if ok)
    live = (pointer or {}).get("run")
    protect = set(complete[-keep:]) | ({live} if live else set())
    newest = complete[-1] if complete else None
    deleted = []
    for d in sorted(days):
        if d in protect or (not days[d] and newest and d >= newest):
            continue
        store.delete_prefix(f"{day_dir(d)}/")
        deleted.append(d)
    if live and live in days and pointer.get("published_utc"):
        try:
            settled = (now - A.parse_iso(pointer["published_utc"])) >= timedelta(hours=SUPERSEDED_H)
        except ValueError:
            settled = False
        if settled:
            keepers = {pointer.get("manifest"), stats_key(live, pointer["published_utc"])}
            prod_dir = f"{day_dir(live)}/{pointer.get('production')}/"
            old = [k for k in store.list_keys(f"{day_dir(live)}/")
                   if k not in keepers and not k.startswith(prod_dir)]
            if old:
                store.delete_keys(old, "superseded productions")
                deleted += old
    oldest = min(protect) if protect else None
    if oldest:
        old = [k for k in store.list_keys(f"{PREFIX}/failed/")
               if k.rsplit("/", 1)[-1][:8].isdigit() and k.rsplit("/", 1)[-1][:8] < oldest]
        if old:
            store.delete_keys(old, "failed records")
    return deleted


# ---------------------------------------------------------------- failures
def record_failure(store, day, digest, message, now):
    """alt.record_failure's record (same shape, so alt.skip_failed reads it) stamped with the run's `now`."""
    key = PRODUCT.failed_key(day)
    prev = store.get_json(key) or {"window": day, "attempts": 0}
    if prev.get("digest") != digest:
        prev = {"window": day, "attempts": 0, "digest": digest}
    prev["attempts"] += 1
    prev["last_attempt_utc"] = A.utc(now)
    prev["last_error"] = P.redact(message, store.bucket)
    store.put(key, json.dumps(prev, sort_keys=True).encode(), JSON, P.POINTER)
    return prev


# ---------------------------------------------------------------- main
def _file_rec(path):
    """--file: a local L4 file as a FileRec (its name must be a product name)."""
    name = os.path.basename(path)
    p = parse_file(name)
    if not p:
        raise ValueError(f"{name} is not an L4 product file name")
    t0 = datetime.strptime(p[0], "%Y%m%d").replace(tzinfo=timezone.utc)
    return A.FileRec("l4", name, "file://" + os.path.abspath(path), os.path.getsize(path), t0, t0 + timedelta(hours=24), p[1])


def main(argv=None, get=A._get, store=None, reader=None, now=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="download and build; upload nothing")
    ap.add_argument("--local", default=None, help="also write every object under this folder")
    ap.add_argument("--day", default=None, help="YYYYMMDD: build this day (no stale alarm; a failure exits 1)")
    ap.add_argument("--file", default=None, help="build from this local L4 file (its name gives the day and production)")
    ap.add_argument("--force", action="store_true", help="rebuild even when the pointer names this production")
    ap.add_argument("--keep", type=int, default=KEEP_DAYS)
    a = ap.parse_args(argv)
    if a.keep < 1 or (a.day and not re.fullmatch(r"\d{8}", a.day)):
        print("bad arguments (--keep >= 1; --day YYYYMMDD)")
        return 2
    rec_file = None
    if a.file:
        try:
            rec_file = _file_rec(a.file)
        except (OSError, ValueError) as exc:
            print(f"bad --file: {exc}")
            return 2
        if a.day and parse_file(rec_file.name)[0] != a.day:
            print("bad arguments (--day differs from the file's day)")
            return 2
    reader = reader or read_l4
    now = now or datetime.now(timezone.utc)
    live = not a.dry_run and a.local is None
    if live and store is None:
        store = A._store_from_env()
    pointer = (store.get_json(PRODUCT.latest_key()) if store is not None else None) or {}
    if pointer.get("format") not in (None, FORMAT):
        print(f"pointer format {pointer.get('format')!r} is not {FORMAT!r}: refusing to touch it")
        return 1
    scheduled = live and not a.day and not a.file

    def stale(why):
        return (A.stale_check(store, PRODUCT, pointer, now=now, why=why) or 0) if scheduled else 0

    def quiet(why):
        return stale(why) if scheduled else 1

    def heartbeat(why, datasets=None, datasets_utc=None):
        print(why)
        if live and pointer:
            pointer["checked_utc"] = A.utc(now)
            if datasets:
                pointer["datasets"], pointer["datasets_utc"] = datasets, datasets_utc
            A.write_pointer(store, PRODUCT, pointer)
            print(f"pruned: {len(prune(store, pointer, keep=a.keep, now=now))}")   # a superseded production's grace ends on a quiet run too
        return stale(why)

    datasets, datasets_utc = pointer.get("datasets"), pointer.get("datasets_utc")
    if rec_file is not None:
        rec, day, prod = rec_file, parse_file(rec_file.name)[0], parse_file(rec_file.name)[2]
    else:
        try:
            datasets, how, datasets_utc = A.discover_datasets(PRODUCT, pointer, get=get, now=now)
        except Exception as exc:                                       # noqa: BLE001
            print(f"no dataset list: {exc.__class__.__name__}: {exc}")
            return quiet("the dataset list cannot be read")
        print(f"{len(datasets)} dataset(s) ({how})")
        days = list_days(datasets, now, get=get)
        if a.day:
            days = {d: r for d, r in days.items() if d == a.day}
        if not days:
            print("no L4 file listed")
            if live and pointer:
                pointer["checked_utc"] = A.utc(now)
                pointer["datasets"], pointer["datasets_utc"] = datasets, datasets_utc
                A.write_pointer(store, PRODUCT, pointer)
            return quiet("no L4 file listed")
        day = max(days)
        rec = days[day]
        prod = parse_file(rec.name)[2]
    live_run, live_prod = pointer.get("run"), pointer.get("production")
    if not a.force and live_run:
        if day < live_run or (day == live_run and prod <= (live_prod or "")):
            return heartbeat(f"nothing newer: {day} {prod} (live {live_run} {live_prod})", datasets, datasets_utc)
    if live and not a.force and A.skip_failed(store, PRODUCT, day, prod, now):
        print(f"{day} {prod}: failed {A.FAILED_MAX_ATTEMPTS} times; waiting {A.FAILED_RETRY_AFTER_H} h between tries")
        return stale(f"day {day} keeps failing")
    t0 = datetime.now(timezone.utc)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            if rec_file is not None:
                path = a.file
            else:
                path = os.path.join(tmp, rec.name)
                A.download(rec, path, get=get, max_bytes=MAX_FILE_BYTES)
            data = reader(path, day)
        published = A.utc(now)
        manifest, stats = build(day, prod, rec.prod, data, store if live else None, a.local, published)
        stats["build_seconds"] = round((datetime.now(timezone.utc) - t0).total_seconds(), 1)
        extra = {"datasets": datasets, "datasets_utc": datasets_utc} if datasets else {}
        if live or a.local is not None:
            mkey, pointer_new = publish(store if live else None, a.local, day, manifest, stats, extra)
        else:
            mkey, pointer_new = manifest_key(day, published), None
    except Exception as exc:                                           # noqa: BLE001
        msg = f"{exc.__class__.__name__}: {exc}"
        print(f"{day} {prod}: FAILED {msg}")
        if live:
            record_failure(store, day, prod, msg, now)
        A._summary(f"FAILED: gridded satellite wave height {day} ({prod}): {P.redact(msg)}")
        return 1
    cells = [f["valid_cells"] for f in manifest["frames"]]
    A._summary(f"### gridded satellite wave height: {day} production {prod}, steps {[f['step'] for f in manifest['frames']]}, "
               f"newest {manifest['newest_utc']}, cells {cells}, max {max((f['max_m'] or 0) for f in manifest['frames'])} m, "
               f"clamped {sum(f['clamped_high'] for f in manifest['frames'])}, flagged {stats['flagged']}, dropped {stats['dropped']}"
               f"{' (dry run)' if a.dry_run else ''} -> {mkey}")
    if live:
        pointer.clear()
        pointer.update(pointer_new)
        deleted = prune(store, pointer, keep=a.keep, now=now)
        print(f"pruned: {len(deleted)}")
        return stale("")
    return 0


if __name__ == "__main__":
    sys.exit(main())
