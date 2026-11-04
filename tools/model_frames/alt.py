"""Satellite wave-height readings job (plan sections 45 and 81): the last ~30 h of along-track altimeter significant
wave height, collocated with the GFS-Wave frames, -> R2 under alt/v1/ as one chunk per 3-hour window.

Source: Copernicus Marine WAVE_GLO_PHY_SWH_L3_NRT_014_001 (1 Hz along-track SWH, every mission flying, buoy- and
cross-calibrated, EMD-filtered `VAVH`), read anonymously from the open CloudFerro bucket the site already reads European
buoys from (one NetCDF-4 file per mission per 3-hour window, regenerated every 30 min while data arrives, the older
production purged). Licence: free, any purpose; credit "Generated using E.U. Copernicus Marine Service Information",
doi 10.48670/moi-00179 (SOURCE below, in every chunk and the pointer). NOAA's Jason-3 OGDR (public domain) is a fourth
height source when its index is configured (ALT_JASON3_INDEX; off until verified against a real file), its readings
dropped where CMEMS's own Jason-3 pass already covers the time.

Layout (every key immutable but the pointer):
  alt/v1/w/<WINDOW YYYYMMDDTHH>/<BUILD YYYYMMDDTHHMMSSZ>.bin    one window, every mission (altfmt: magic ALT1)
  alt/v1/stats/<BUILD>.json                                    per build: counts per mission, obs - model
  alt/v1/failed/<WINDOW>.json, alt/v1/stale-alarm.json
  alt/v1/latest.json  -> the pointer (max-age 300, written LAST): {format, build, published_utc (the last chunk
      change), checked_utc (every run), newest_utc, window_s, keep_h, encoding, missions, chunks [{key, window, t0, t1,
      n, bytes, build, missions, newest_utc, inputs}], model, columns, stats, source}
A window is rebuilt (a new key) when the digest of its inputs (the files' names and sizes + FORMAT_KEY) differs from
the pointer's; nothing changed -> the pointer is rewritten with checked_utc only (a heartbeat).
Exit codes: 0 published / nothing new / a quiet outage; 1 a build failure (recorded, backed off), a stale newest
reading (> STALE_H, at most once per STALE_ALARM_EVERY_H), a model range change (the frames' hs lo / hi are not
encode.FIELDS["hs"]: a deliberate change here is needed first); 2 bad arguments.

The machinery (STAC + listing, the plan, downloads, publish, prune, stale) is parametrised by a Product so the swell
job (sarswell.py) runs the same code over its own product, prefix and chunk format.
"""
import argparse
import hashlib
import io
import json
import math
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import altfmt as F       # noqa: E402
import encode as E       # noqa: E402
import publish as P      # noqa: E402

S3 = "https://s3.waw3-1.cloudferro.com"
BUCKET = "mdl-native-05"
STAC = "https://stac.marine.copernicus.eu/metadata"
UA = "allshoresurf.com satellite waves (https://allshoresurf.com)"
JSON = "application/json"
BIN = "application/octet-stream"
EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)
WINDOW_S = 3 * 3600
KEEP_H = 30                                   # 24 h + the client's 3 h frame margin + slack
PRUNE_AFTER_H = KEEP_H + 6
MAX_CHUNKS = 12
MAX_FILE_BYTES = 20_000_000                   # a 1 Hz file is 100-215 KB; nothing far larger is a product file
MIN_FILE_BYTES = 1_000
DOWNLOAD_WORKERS = 8
DOWNLOAD_TRIES = 4
FAILED_MAX_ATTEMPTS = 3                       # on the same inputs digest: the window then waits FAILED_RETRY_AFTER_H between tries
FAILED_RETRY_AFTER_H = 2
DATASETS_REFRESH_H = 6                        # the dataset list is re-read from STAC this often (the pointer caches it between)
STALE_H = 12                                  # the newest reading older than this = an outage (summary STALE:, exit 1)
STALE_ALARM_EVERY_H = 12
MISSION_SILENT_H = 48                         # a mission with no file newer than this: a summary WARNING
BUILD_STALE_H = 2                             # a build superseded this long ago is pruned (clients re-read the pointer every 12 min)
STATS_KEEP_H = 24
FRAMES_ROOT = "https://models.allshoresurf.com"
FRAMES_PREFIX = P.PREFIX
RUN_LOOKBACK = (0, 6, 12, 18)                 # the model runs tried, newest first (hours behind the live run)
MAX_FRAME_CACHE = 64
HS_FIELD = "hs"
MODEL_RULE = "GFS-Wave hs: the newest run whose cycle <= t, linear in time between its hourly frames, bilinear over the present 0.25-degree nodes (w < 0.25 none)"
SOURCE = {"name": "Copernicus Marine WAVE_GLO_PHY_SWH_L3_NRT_014_001 (L3 along-track significant wave height, 1 Hz)",
          "doi": "10.48670/moi-00179", "url": "https://doi.org/10.48670/moi-00179",
          "credit": "Generated using E.U. Copernicus Marine Service Information; https://doi.org/10.48670/moi-00179"}
JASON3_SOURCE = {"name": "NOAA/NESDIS Jason-3 OGDR (near-real-time, public domain)", "credit": "NOAA/NESDIS Jason-3 OGDR"}
# Mission tokens as the file names carry them -> (short label, full name). Unknown tokens are kept with the token as
# their name (a new mission appears by itself; the summary names it).
MISSIONS = {
    "j3": ("Jason-3", "Jason-3"), "s3a": ("Sentinel-3A", "Sentinel-3A SRAL"), "s3b": ("Sentinel-3B", "Sentinel-3B SRAL"),
    "s6a": ("Sentinel-6A", "Sentinel-6 Michael Freilich"), "s6a_lr": ("Sentinel-6A", "Sentinel-6 Michael Freilich (LR)"),
    "s6b": ("Sentinel-6B", "Sentinel-6B"), "s6b_lr": ("Sentinel-6B", "Sentinel-6B (LR)"),
    "al": ("SARAL", "SARAL/AltiKa"), "c2": ("CryoSat-2", "CryoSat-2"), "cfo": ("CFOSAT", "CFOSAT SWIM nadir"),
    "h2b": ("HY-2B", "Haiyang-2B"), "h2c": ("HY-2C", "Haiyang-2C"), "h2d": ("HY-2D", "Haiyang-2D"),
    "swon": ("SWOT", "SWOT nadir altimeter"), "swot": ("SWOT", "SWOT nadir altimeter"),
    "j3_ogdr": ("Jason-3 OGDR", "Jason-3 OGDR (NOAA, near-real-time)"),
}
MISSION_SILENT_OVERRIDE_H = {"h2b": 96}       # HY-2B arrives 1-2 days late
FILE_RE = re.compile(r"^global_vavh_l3_rt_(?P<token>[a-z0-9]+(?:_[a-z0-9]+)?)_(?P<t0>\d{8}T\d{6})_(?P<t1>\d{8}T\d{6})_(?P<prod>\d{8}T\d{6})\.nc$")
DATASET_RE = re.compile(r"swh_nrt_([a-z0-9]+(?:-[a-z0-9]+)?)-l3_PT1S", re.I)   # the 1 Hz datasets only (5 Hz ones exist too)
NC_SUFFIX = ".nc"
JASON3_FILE_RE = re.compile(r"^JA3_OPN_2P[a-zA-Z]S(\d{3})_(\d{3})_(\d{8})_(\d{6})_(\d{8})_(\d{6})\.nc$")


def _summary(line):
    print(line)
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")


def _get(url, headers=None, timeout=60):
    req = urllib.request.Request(url, headers=dict({"User-Agent": UA}, **(headers or {})))
    return urllib.request.urlopen(req, timeout=timeout)


def utc(dt=None):
    return (dt or datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp(dt):
    return dt.strftime("%Y%m%dT%H%M%SZ")


def parse_stamp(s):
    return datetime.strptime(s, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)


def parse_iso(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def window_key(dt):
    return dt.strftime("%Y%m%dT%H")


def parse_window(key):
    return datetime.strptime(key, "%Y%m%dT%H").replace(tzinfo=timezone.utc)


def secs(dt):
    return (dt - EPOCH).total_seconds()


# ---------------------------------------------------------------- the product
class Product:
    """What differs between the heights job and the swell job: the bucket product, how files are named and read, the
    chunk format, the keys, and whether the model is collocated."""

    def __init__(self, prefix, fmt, magic, stac_id, dataset_re, file_re, reader, missions, source, columns, collocate, build,
                 window_s=WINDOW_S, keep_h=KEEP_H, format_key="1", label="satellite wave heights", min_bytes=MIN_FILE_BYTES,
                 max_bytes=MAX_FILE_BYTES, token_from_file=True, stale_h=STALE_H):
        self.prefix, self.format, self.magic, self.stac_id = prefix, fmt, magic, stac_id
        self.dataset_re, self.file_re, self.reader, self.missions, self.source = dataset_re, file_re, reader, missions, source
        self.columns, self.collocate, self.build, self.window_s, self.keep_h = columns, collocate, build, window_s, keep_h
        self.format_key, self.label, self.min_bytes, self.max_bytes = format_key, label, min_bytes, max_bytes
        self.token_from_file = token_from_file                         # else the dataset id names the mission
        self.stale_h = stale_h                                         # the newest reading older than this = an outage

    def latest_key(self):
        return f"{self.prefix}/latest.json"

    def chunk_key(self, window, build):
        return f"{self.prefix}/w/{window}/{build}.bin"

    def stats_key(self, build):
        return f"{self.prefix}/stats/{build}.json"

    def failed_key(self, window):
        return f"{self.prefix}/failed/{window}.json"

    def stale_key(self):
        return f"{self.prefix}/stale-alarm.json"

    def parse_file(self, name, dataset_token=None):
        """-> (token, t0, t1, production) for a product file name, else None. Groups: (token,) t0, t1, (production)."""
        m = self.file_re.search(name)
        if not m:
            return None
        g = m.groupdict()
        try:
            tok = g.get("token") if self.token_from_file else dataset_token
            t0, t1 = parse_stamp(g["t0"]), parse_stamp(g["t1"])
            prod = parse_stamp(g["prod"]) if g.get("prod") else t1
        except (ValueError, KeyError):
            return None
        if not tok or t1 < t0:
            return None
        return tok, t0, t1, prod


# ---------------------------------------------------------------- STAC and the bucket listing
def _stac_json(get, url):
    with get(url, timeout=60) as r:
        body = r.read(MAX_FILE_BYTES)
    return json.loads(body.decode("utf-8"))


def _dataset_token(product, dataset_id):
    m = product.dataset_re.search(dataset_id)
    return m.group(1).lower().replace("-", "_") if m else None


def stac_datasets(product, get=_get):
    """{dataset id: native prefix URL (bucket host + bucket + prefix, no trailing slash)} for the product's 1 Hz
    datasets, from the product's STAC collection (each item's assets.native.href). Raises on a STAC that cannot be read
    or holds no dataset (the caller falls back)."""
    base = f"{STAC}/{product.stac_id}"
    col = _stac_json(get, f"{base}/product.stac.json")
    hrefs = []
    for link in (col.get("links") or []):
        href = link.get("href") if isinstance(link, dict) else None
        if isinstance(href, str) and href.endswith("dataset.stac.json"):
            hrefs.append(href if "://" in href else f"{base}/{href.lstrip('./')}")
    out = {}
    for href in hrefs:
        if not product.dataset_re.search(href.rsplit("/", 2)[-2]):     # only the datasets wanted are fetched (paced)
            continue
        try:
            item = _stac_json(get, href)
        except Exception as exc:                                       # noqa: BLE001
            print(f"STAC item {href}: {exc.__class__.__name__}")
            continue
        did = item.get("id") or href.rsplit("/", 2)[-2]
        if not isinstance(did, str) or not product.dataset_re.search(did):
            continue
        native = ((item.get("assets") or {}).get("native") or {}).get("href")
        if isinstance(native, str) and native.startswith("https://") and did in native:
            out[did] = native.rstrip("/")
    if not out:
        raise ValueError("no dataset in the STAC collection")
    return out


def bucket_datasets(product, get=_get, s3=S3, bucket=BUCKET):
    """The product's dataset directories straight from the bucket (the STAC fallback): a delimiter listing."""
    url = f"{s3}/{bucket}/?list-type=2&prefix=native/{product.stac_id}/&delimiter=/"
    with get(url, timeout=60) as r:
        body = r.read(MAX_FILE_BYTES)
    out = {}
    for pre in parse_listing(body)[1]:
        did = pre.rstrip("/").rsplit("/", 1)[-1]
        if product.dataset_re.search(did):
            out[did] = f"{s3}/{bucket}/{pre.rstrip('/')}"
    if not out:
        raise ValueError("no dataset directory in the bucket listing")
    return out


_NS_RE = re.compile(r"^\{[^}]*\}")


def parse_listing(body):
    """A ListObjectsV2 answer, namespace or not -> ([(key, size)], [common prefixes], next token or None)."""
    root = ET.fromstring(body)
    keys, pres, token, truncated = [], [], None, False

    def local(el):
        return _NS_RE.sub("", el.tag)
    if local(root) != "ListBucketResult":
        raise ValueError("not a bucket listing")
    for el in root:
        tag = local(el)
        if tag == "Contents":
            key = size = None
            for c in el:
                t = local(c)
                if t == "Key":
                    key = c.text
                elif t == "Size":
                    try:
                        size = int(c.text)
                    except (TypeError, ValueError):
                        size = None
            if key and size is not None:
                keys.append((key, size))
        elif tag == "CommonPrefixes":
            for c in el:
                if local(c) == "Prefix" and c.text:
                    pres.append(c.text)
        elif tag == "NextContinuationToken":
            token = el.text
        elif tag == "IsTruncated":
            truncated = (el.text or "").strip().lower() == "true"
    return keys, pres, (token if truncated and token else None)


def list_prefix(url_base, prefix, get=_get, max_keys=1000):
    """Every (key, size) under `prefix` of the bucket `url_base` names (https://host/bucket), paginated."""
    out, token = [], None
    for _ in range(50):
        url = f"{url_base}/?list-type=2&prefix={urllib.request.quote(prefix)}&max-keys={max_keys}"
        if token:
            url += "&continuation-token=" + urllib.request.quote(token, safe="")
        with get(url, timeout=60) as r:
            body = r.read(MAX_FILE_BYTES)
        keys, _pres, token = parse_listing(body)
        out += keys
        if not token:
            return out
    raise RuntimeError("listing did not end")


class FileRec:
    __slots__ = ("token", "name", "url", "size", "t0", "t1", "prod", "mission")

    def __init__(self, token, name, url, size, t0, t1, prod):
        self.token, self.name, self.url, self.size, self.t0, self.t1, self.prod = token, name, url, size, t0, t1, prod
        self.mission = token

    def key(self):
        return f"{self.name}:{self.size}"


def months_touching(start, end):
    out, d = [], start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while d <= end:
        out.append(d)
        d = (d + timedelta(days=32)).replace(day=1)
    return out


def list_files(product, datasets, now, get=_get, since=None):
    """[FileRec] of the product's files whose window ends after `since` (now - keep_h): newest production per
    (mission, window start); a listing that fails for one dataset is reported and skipped (the window's inputs then
    miss that mission: its digest changes back when the listing returns)."""
    since = since or (now - timedelta(hours=product.keep_h))
    best = {}
    for did, native in sorted(datasets.items()):
        host, _, rest = native.partition("://")
        parts = rest.split("/", 2)                                     # host, bucket, prefix
        if len(parts) < 3:
            print(f"dataset {did}: unusable native href")
            continue
        url_base, prefix = f"{host}://{parts[0]}/{parts[1]}", parts[2]
        for month in months_touching(since, now):
            try:
                keys = list_prefix(url_base, f"{prefix}/{month:%Y}/{month:%m}/", get=get)
            except Exception as exc:                                   # noqa: BLE001
                print(f"listing {did} {month:%Y/%m}: {exc.__class__.__name__}: {exc}")
                continue
            dtok = _dataset_token(product, did)
            for key, size in keys:
                name = key.rsplit("/", 1)[-1]
                parsed = product.parse_file(name, dtok)
                if not parsed:
                    continue
                token, t0, t1, prod = parsed
                if t1 <= since or t0 > now + timedelta(hours=1):
                    continue
                if not (product.min_bytes <= size <= product.max_bytes):
                    print(f"{name}: {size} bytes is not a product file")
                    continue
                rec = FileRec(token, name, f"{url_base}/{key}", size, t0, t1, prod)
                k = (token, t0)
                if k not in best or prod > best[k].prod or (prod == best[k].prod and name > best[k].name):
                    best[k] = rec
    return sorted(best.values(), key=lambda r: (r.t0, r.token, r.name))


# ---------------------------------------------------------------- the plan
def windows_for(product, now, since=None):
    """Window starts (datetime) whose end is after `since`, up to now."""
    since = since or (now - timedelta(hours=product.keep_h))
    w = product.window_s
    first = datetime.fromtimestamp(math.floor(since.timestamp() / w) * w, tz=timezone.utc)
    out, d = [], first
    while d <= now:
        if d + timedelta(seconds=w) > since:
            out.append(d)
        d += timedelta(seconds=w)
    return out


def files_of_window(files, w0, window_s):
    w1 = w0 + timedelta(seconds=window_s)
    return [f for f in files if f.t0 < w1 and f.t1 > w0]


def digest_of(product, files, extra=""):
    h = hashlib.sha256()
    for k in sorted(f.key() for f in files):
        h.update(k.encode() + b"\n")
    h.update(("format:" + product.format_key + ("|" + extra if extra else "")).encode())
    return h.hexdigest()[:32]


def plan(product, pointer, files, now, force=False, only=None, since=None, run=None):
    """-> {window key: {files, digest, old}}: every window in reach with its files and digest, `old` the pointer's chunk
    record for it (None without one); `build` is set where the digest differs (or force, or the one window asked for).
    `run` (a datetime: the live model run) joins the digest of the windows ending after its cycle: their readings get that
    run when it arrives (the newest run whose cycle <= t), so a new run rebuilds them; older windows keep their model."""
    old = {c["window"]: c for c in (pointer or {}).get("chunks", []) if isinstance(c, dict) and "window" in c}
    out = {}
    for w0 in windows_for(product, now, since):
        key = window_key(w0)
        if only and key != only:
            continue
        fs = files_of_window(files, w0, product.window_s)
        d = digest_of(product, fs, "run:" + P.run_key(run) if run is not None and w0 + timedelta(seconds=product.window_s) > run else "")
        rec = old.get(key)
        inputs = rec.get("inputs") if rec else None
        out[key] = {"t0": w0, "files": fs, "digest": d, "old": rec,
                    "build": bool(fs) and (force or only == key or not rec or inputs != d)}
    return out


# ---------------------------------------------------------------- downloads
def download(rec, dest, get=_get, tries=DOWNLOAD_TRIES, log=print, max_bytes=MAX_FILE_BYTES):
    """One file, whole; the size must equal the listing's (an unknown size, 0, is bounded by max_bytes)."""
    last = None
    limit = rec.size or max_bytes
    for attempt in range(1, tries + 1):
        have = 0
        try:
            with get(rec.url, timeout=120) as r, open(dest, "wb") as fh:
                while True:
                    chunk = r.read(1 << 20)
                    if not chunk:
                        break
                    fh.write(chunk)
                    have += len(chunk)
                    if have > limit:
                        raise ValueError(f"{rec.name}: more bytes than {'listed' if rec.size else 'allowed'}")
            if have == rec.size or (not rec.size and have > 0):
                return dest
            last = IOError(f"ended at {have} of {rec.size} bytes")
        except ValueError:
            raise
        except urllib.error.HTTPError as exc:
            if exc.code == 404:                                        # purged between the listing and the GET (common): no retries
                raise IOError(f"{rec.name}: HTTP 404")
            last = exc
        except Exception as exc:                                       # noqa: BLE001
            last = exc
        log(f"{rec.name}: attempt {attempt} stopped at {have} of {rec.size}: {last.__class__.__name__}")
        if attempt < tries:
            time.sleep(min(20, 3 * attempt))
    raise IOError(f"{rec.name}: {last!r} after {tries} tries")


def download_all(files, folder, get=_get, workers=DOWNLOAD_WORKERS, log=print):
    """-> ({name: path}, {name: error text}) for the files, `workers` at a time."""
    paths, failed = {}, {}

    def one(rec):
        dest = os.path.join(folder, rec.name)
        try:
            download(rec, dest, get=get, log=log)
            return rec.name, dest, None
        except Exception as exc:                                       # noqa: BLE001
            return rec.name, None, f"{exc.__class__.__name__}: {exc}"
    with ThreadPoolExecutor(max(1, workers)) as pool:
        for name, dest, err in pool.map(one, files):
            if err:
                failed[name] = err
            else:
                paths[name] = dest
    return paths, failed


# ---------------------------------------------------------------- reading the L3 files
def _attr(ds, name, default=None):
    v = ds.attrs.get(name, default)
    if isinstance(v, np.ndarray) and v.size == 1:
        v = v[0]
    if isinstance(v, (bytes, np.bytes_)):
        return v.decode("utf-8", "replace")
    if isinstance(v, np.str_):
        return str(v)
    return v


def _scaled(ds, expect_scale=None, expect_fill=None, expect_units=None):
    """A NetCDF variable's values as float64 with its scale / offset applied and its fill as NaN, after checking the
    scale, fill and units the job relies on (a product change refuses the file instead of mis-reading it)."""
    raw = ds[()]
    scale = float(_attr(ds, "scale_factor", 1.0))
    offset = float(_attr(ds, "add_offset", 0.0))
    fill = _attr(ds, "_FillValue")
    units = _attr(ds, "units")
    if expect_scale is not None and abs(scale - expect_scale) > 1e-12 * max(1.0, abs(expect_scale)):
        raise ValueError(f"{ds.name}: scale_factor {scale} is not {expect_scale}")
    if expect_fill is not None and (fill is None or int(fill) != expect_fill):
        raise ValueError(f"{ds.name}: _FillValue {fill} is not {expect_fill}")
    if expect_units is not None and not (isinstance(units, str) and units.startswith(expect_units)):
        raise ValueError(f"{ds.name}: units {units!r} are not {expect_units!r}")
    v = raw.astype(np.float64)
    if fill is not None:
        v = np.where(raw == fill, np.nan, v)
    return v * scale + offset


def _find(f, names):
    for n in names:
        if n in f:
            return f[n]
    raise ValueError(f"none of {names} in the file")


def read_l3(path, t0=None, t1=None):
    """A CMEMS L3 SWH file -> {t (s since EPOCH), lat, lon (-180..180), swh (m)} of the readings with a value, within
    the window [t0, t1) when given, time-sorted. Checks: VAVH scale 0.001 with fill -32767 and units m; latitude /
    longitude scale 1e-6; time in seconds since 2000-01-01; 0 <= swh <= 30 m."""
    import h5py                                                        # the job env only (pinned)
    with h5py.File(path, "r") as f:
        tv = _find(f, ["time"])
        tunits = _attr(tv, "units", "")
        if not (isinstance(tunits, str) and re.match(r"^seconds since 2000-01-01[ T]00:00:00", tunits)):
            raise ValueError(f"time units {tunits!r} are not seconds since 2000-01-01")
        t = _scaled(tv)
        lat = _scaled(_find(f, ["latitude", "lat"]), expect_scale=1e-6, expect_units="degrees_north")
        lon = _scaled(_find(f, ["longitude", "lon"]), expect_scale=1e-6, expect_units="degrees_east")
        swh = _scaled(_find(f, ["VAVH"]), expect_scale=0.001, expect_fill=-32767, expect_units="m")
    if not (t.shape == lat.shape == lon.shape == swh.shape) or t.ndim != 1:
        raise ValueError("time / lat / lon / VAVH shapes differ")
    lon = (lon + 180.0) % 360.0 - 180.0
    ok = np.isfinite(t) & np.isfinite(lat) & np.isfinite(lon) & np.isfinite(swh) & (swh >= 0) & (swh <= 30) & (np.abs(lat) <= 90)
    if t0 is not None:
        ok &= in_window(t, t0, t1)
    idx = np.nonzero(ok)[0]
    idx = idx[np.argsort(t[idx], kind="stable")]
    return {"t": t[idx], "lat": lat[idx], "lon": lon[idx], "swh": swh[idx]}


def in_window(t, t0, t1):
    """The readings of a window: t0 <= t < t1 (half-open, so a reading at a boundary lands in ONE chunk: a daily file is
    split into windows without duplicates)."""
    return (t >= secs(t0)) & (t < secs(t1))


def read_jason3_ogdr(path, t0=None, t1=None):
    """A NOAA Jason-3 OGDR (GDR-F NetCDF-4, groups data_01/...; the flat GDR-D layout too) -> the same dict as read_l3.
    Keeps Ku-band ocean SWH with its quality flag 0 and the surface classed as open ocean."""
    import h5py
    with h5py.File(path, "r") as f:
        if "data_01" in f:                                             # GDR-F
            g = f["data_01"]
            tv = g["time"]
            lat = _scaled(g["latitude"], expect_units="degrees_north")
            lon = _scaled(g["longitude"], expect_units="degrees_east")
            swh = _scaled(g["ku"]["swh_ocean"], expect_units="m")
            qual = g["ku"]["swh_ocean_qual"][()] if "swh_ocean_qual" in g["ku"] else np.zeros(swh.shape, np.int8)
            surf = g["surface_classification_flag"][()] if "surface_classification_flag" in g else np.zeros(swh.shape, np.int8)
        else:                                                          # GDR-D: flat
            tv = f["time"]
            lat = _scaled(f["lat"], expect_units="degrees_north")
            lon = _scaled(f["lon"], expect_units="degrees_east")
            swh = _scaled(f["swh_ku"], expect_units="m")
            qual = f["qual_alt_1hz_swh_ku"][()] if "qual_alt_1hz_swh_ku" in f else np.zeros(swh.shape, np.int8)
            surf = f["surface_type"][()] if "surface_type" in f else np.zeros(swh.shape, np.int8)
        tunits = _attr(tv, "units", "")
        if not (isinstance(tunits, str) and re.match(r"^seconds since 2000-01-01[ T]00:00:00", tunits)):
            raise ValueError(f"time units {tunits!r} are not seconds since 2000-01-01")
        t = _scaled(tv)
    lon = (lon + 180.0) % 360.0 - 180.0
    ok = np.isfinite(t) & np.isfinite(lat) & np.isfinite(lon) & np.isfinite(swh) & (swh >= 0) & (swh <= 30) & (qual == 0) & (surf == 0)
    if t0 is not None:
        ok &= in_window(t, t0, t1)
    idx = np.nonzero(ok)[0]
    idx = idx[np.argsort(t[idx], kind="stable")]
    return {"t": t[idx], "lat": lat[idx], "lon": lon[idx], "swh": swh[idx]}


# ---------------------------------------------------------------- the model (collocation)
class NotReady(Exception):
    """A frame that is not on the host (404)."""


class Frames:
    """The GFS-Wave hs frames over HTTPS (the public root), decoded once per (run, step) into an LRU. The live run's
    manifest fixes the field's range (it must be encode.FIELDS["hs"]: else RangeChanged) and the grid; older runs
    (RUN_LOOKBACK) use the same template (their keys are deterministic) and are taken only where their frame exists."""

    class RangeChanged(Exception):
        pass

    def __init__(self, root=FRAMES_ROOT, prefix=FRAMES_PREFIX, get=_get, max_cache=MAX_FRAME_CACHE):
        self.root, self.prefix, self.get, self.max_cache = root.rstrip("/"), prefix, get, max_cache
        self.manifest = None
        self.run = None
        self.lo = self.hi = None
        self.cache = {}
        self.order = []
        self.missing = set()
        self.fetched = 0

    def _json(self, url):
        with self.get(url, timeout=60) as r:
            return json.loads(r.read(MAX_FILE_BYTES).decode("utf-8"))

    def load(self):
        ptr = self._json(f"{self.root}/{self.prefix}/latest.json")
        if not (isinstance(ptr, dict) and ptr.get("complete") is True and isinstance(ptr.get("run"), str) and re.fullmatch(r"\d{10}", ptr["run"]) and isinstance(ptr.get("manifest"), str)):
            raise ValueError("frames pointer unusable")
        m = self._json(f"{self.root}/{ptr['manifest']}")
        if m.get("run") != ptr["run"] or m.get("encoding") != E.ENCODING or m.get("schema") != 3:
            raise ValueError("frames manifest unusable")
        g = m.get("grid") or {}
        if not (g.get("cols") == 1440 and g.get("rows") == 721 and g.get("lon0") == -180.0 and g.get("lat0") == 90.0 and abs(g.get("dlon", 0) - 0.25) < 1e-9 and abs(g.get("dlat", 0) + 0.25) < 1e-9):
            raise ValueError("frames grid is not the 0.25-degree world")
        f = (m.get("fields") or {}).get(HS_FIELD)
        if not f or "full" not in (f.get("resolutions") or []):
            raise ValueError("frames have no full-resolution hs")
        if abs(float(f["lo"]) - E.FIELDS[HS_FIELD]["lo"]) > 1e-9 or abs(float(f["hi"]) - E.FIELDS[HS_FIELD]["hi"]) > 1e-9:
            raise Frames.RangeChanged(f"hs range {f['lo']}..{f['hi']} is not encode.FIELDS: a deliberate alt change is needed")
        self.manifest, self.run, self.lo, self.hi = m, ptr["run"], float(f["lo"]), float(f["hi"])
        return self

    def runs(self):
        """The candidate runs, newest first: the live run and the cycles RUN_LOOKBACK hours behind it."""
        r0 = datetime.strptime(self.run, "%Y%m%d%H").replace(tzinfo=timezone.utc)
        return [r0 - timedelta(hours=h) for h in RUN_LOOKBACK]

    def frame(self, run_dt, step):
        """The hs codes (721 x 1440 uint8) of a run's step, or None when the host has no such frame (404)."""
        run = P.run_key(run_dt)
        key = (run, step)
        if key in self.cache:
            self.order.remove(key)
            self.order.append(key)
            return self.cache[key]
        if key in self.missing:
            return None
        url = f"{self.root}/{P.frame_key(run, HS_FIELD, step, prefix=self.prefix)}"
        try:
            body = self._bytes(url)
        except NotReady:
            self.missing.add(key)
            return None
        from PIL import Image
        im = Image.open(io.BytesIO(body))
        q = np.asarray(im.convert("L") if im.mode != "L" else im, dtype=np.uint8)
        if q.shape != (721, 1440):
            raise ValueError(f"frame {url} is {q.shape}, not 721 x 1440")
        self.cache[key] = q
        self.order.append(key)
        self.fetched += 1
        while len(self.order) > self.max_cache:
            old = self.order.pop(0)
            self.cache.pop(old, None)
        return q

    def _bytes(self, url, tries=4):
        last = None
        for attempt in range(tries):
            try:
                with self.get(url, timeout=60) as r:
                    return r.read(MAX_FILE_BYTES)
            except urllib.error.HTTPError as exc:
                if exc.code in (403, 404):
                    raise NotReady(url) from None
                last = f"HTTP {exc.code}"
            except Exception as exc:                                   # noqa: BLE001
                last = repr(exc)
            time.sleep(2 * (attempt + 1))
        raise IOError(f"{url}: {last} after {tries} tries")


def sample_bilinear(q, lat, lon):
    """The client's rule over the 0.25-degree frame codes q (721 x 1440): bilinear over the present neighbours in code
    space, w < 0.25 -> none. -> (code float array, NaN = none; edge bool array: a neighbour was absent)."""
    rows, cols = q.shape
    r = (90.0 - np.asarray(lat, np.float64)) / 0.25
    c = np.mod((np.asarray(lon, np.float64) + 180.0) / 0.25, cols)
    r = np.clip(r, 0, rows - 1)
    r0 = np.floor(r).astype(np.int64)
    r1 = np.minimum(r0 + 1, rows - 1)
    fr = r - r0
    c0 = np.floor(c).astype(np.int64)
    c1 = np.where(c0 + 1 == cols, 0, c0 + 1)
    fc = c - c0
    a = q[r0, c0].astype(np.float64)
    b = q[r0, c1].astype(np.float64)
    cc = q[r1, c0].astype(np.float64)
    d = q[r1, c1].astype(np.float64)
    wa = np.where(a > 0, (1 - fr) * (1 - fc), 0.0)
    wb = np.where(b > 0, (1 - fr) * fc, 0.0)
    wc = np.where(cc > 0, fr * (1 - fc), 0.0)
    wd = np.where(d > 0, fr * fc, 0.0)
    w = wa + wb + wc + wd
    acc = a * wa + b * wb + cc * wc + d * wd
    with np.errstate(invalid="ignore", divide="ignore"):
        code = np.where(w >= 0.25, acc / np.where(w > 0, w, 1), np.nan)
    edge = (a == 0) | (b == 0) | (cc == 0) | (d == 0)
    return code, edge


def near_land_of(lat, lon, allow=None):
    """True where any of the four 0.25-degree nodes around the point lies in the coastal fill mask (within one cell of
    GSHHG land: encode.fill_allow)."""
    allow = E.fill_allow() if allow is None else allow
    rows, cols = allow.shape
    r = np.clip((90.0 - np.asarray(lat, np.float64)) / 0.25, 0, rows - 1)
    c = np.mod((np.asarray(lon, np.float64) + 180.0) / 0.25, cols)
    r0 = np.floor(r).astype(np.int64)
    r1 = np.minimum(r0 + 1, rows - 1)
    c0 = np.floor(c).astype(np.int64)
    c1 = np.where(c0 + 1 == cols, 0, c0 + 1)
    return allow[r0, c0] | allow[r0, c1] | allow[r1, c0] | allow[r1, c1]


def collocate(frames, t, lat, lon, log=print):
    """Per reading the GFS-Wave hs (m) of the NEWEST run whose cycle <= t (frames.runs(), newest first; a run with no
    frame for the time -> the next older), linear in time between the hourly frames bracketing t, each sampled
    bilinearly. -> (model m float NaN = none, run index u1 255 none, edge bool, runs [run ids in index order])."""
    n = len(t)
    model = np.full(n, np.nan)
    run_idx = np.full(n, F.NONE_U1, np.uint8)
    edge = np.zeros(n, bool)
    if frames is None or n == 0:
        return model, run_idx, edge, []
    runs = frames.runs()
    lo, hi = frames.lo, frames.hi
    done = np.zeros(n, bool)
    ts = np.asarray(t, np.float64)
    for ri, run_dt in enumerate(runs):
        rs = secs(run_dt)
        lead = (ts - rs) / 3600.0
        cand = ~done & (lead >= 0) & (lead <= 120)
        if not cand.any():
            continue
        k = np.floor(lead).astype(np.int64)
        k = np.where(k >= 120, 119, k)
        for step in np.unique(k[cand]):
            sel = cand & (k == step)
            qa = frames.frame(run_dt, int(step))
            qb = frames.frame(run_dt, int(step) + 1)
            if qa is None or qb is None:
                continue                                               # this run lacks the frame: an older run may have it
            ca, ea = sample_bilinear(qa, lat[sel], lon[sel])
            cb, eb = sample_bilinear(qb, lat[sel], lon[sel])
            f = lead[sel] - step
            va = lo + (ca - 1) / 254.0 * (hi - lo)
            vb = lo + (cb - 1) / 254.0 * (hi - lo)
            v = va * (1 - f) + vb * f
            ids = np.nonzero(sel)[0]
            model[ids] = v
            edge[ids] = ea | eb
            run_idx[ids] = ri
            done[ids] = True                                           # (none stays none: the model has no value there)
    return model, run_idx, edge, [P.run_key(r) for r in runs]


# ---------------------------------------------------------------- the build
def dedupe_against(t_keep, t_drop, gap_s=60.0, tol_s=1.5):
    """Indices into t_drop of the readings OUTSIDE every span of t_keep (both sorted): a span runs from one t_keep
    reading to the next while gaps stay within gap_s, widened by tol_s at both ends. An OGDR reading inside a CMEMS pass
    is dropped even where CMEMS rejected its own reading (quality: rain, ice, the coast); only the hours CMEMS has not
    delivered yet survive."""
    if len(t_keep) == 0 or len(t_drop) == 0:
        return np.arange(len(t_drop))
    tk = np.asarray(t_keep, np.float64)
    cut = np.nonzero(np.diff(tk) > gap_s)[0] + 1
    if len(cut):
        starts = np.concatenate([[tk[0]], tk[cut]]) - tol_s
        ends = np.concatenate([tk[cut - 1], [tk[-1]]]) + tol_s
    else:
        starts, ends = np.array([tk[0] - tol_s]), np.array([tk[-1] + tol_s])
    td = np.asarray(t_drop, np.float64)
    i = np.searchsorted(starts, td, side="right") - 1
    inside = (i >= 0) & (td <= ends[np.clip(i, 0, len(ends) - 1)])
    return np.nonzero(~inside)[0]


def mission_index(missions_seen, pointer_missions, table):
    """The mission list (stable order: the pointer's first, then new tokens alphabetically) -> [{id, short, name}]."""
    order = [m["id"] for m in (pointer_missions or []) if isinstance(m, dict) and isinstance(m.get("id"), str)]
    for tok in sorted(missions_seen):
        if tok not in order:
            order.append(tok)
    out = []
    for tok in order:
        short, name = table.get(tok, (tok, tok))
        out.append({"id": tok, "short": short, "name": name})
    return out


def build_heights(window, w0, w1, read, missions, frames, build_id, inputs, log=print):
    """One window's chunk from the readings per file (`read`: {file name: (token, dict of arrays)}) -> (bytes, record,
    stats). Readings are grouped per mission (segments), time-sorted; Jason-3 OGDR readings within 1.5 s of a CMEMS
    Jason-3 reading are dropped (one pass, two sources)."""
    by_tok = {}
    for name in sorted(read):
        tok, d = read[name]
        by_tok.setdefault(tok, []).append(d)
    per = {}
    for tok, parts in by_tok.items():
        cat = {k: np.concatenate([p[k] for p in parts]) if parts else np.zeros(0) for k in ("t", "lat", "lon", "swh")}
        o = np.argsort(cat["t"], kind="stable")
        per[tok] = {k: v[o] for k, v in cat.items()}
    if "j3_ogdr" in per and "j3" in per:
        keep = dedupe_against(per["j3"]["t"], per["j3_ogdr"]["t"])
        dropped = len(per["j3_ogdr"]["t"]) - len(keep)
        per["j3_ogdr"] = {k: v[keep] for k, v in per["j3_ogdr"].items()}
        if dropped:
            log(f"{window}: {dropped} Jason-3 OGDR readings already in CMEMS's pass dropped")
    ids = {m["id"]: i for i, m in enumerate(missions)}
    segs, cols = [], {k: [] for k in ("t", "lat", "lon", "swh", "mission")}
    pos = 0
    for tok in sorted(per, key=lambda k: ids[k]):
        d = per[tok]
        n = len(d["t"])
        if not n:
            continue
        segs.append([ids[tok], pos, n])
        pos += n
        for k in ("t", "lat", "lon", "swh"):
            cols[k].append(d[k])
        cols["mission"].append(np.full(n, ids[tok], np.uint8))
    t = np.concatenate(cols["t"]) if cols["t"] else np.zeros(0)
    lat = np.concatenate(cols["lat"]) if cols["lat"] else np.zeros(0)
    lon = np.concatenate(cols["lon"]) if cols["lon"] else np.zeros(0)
    swh = np.concatenate(cols["swh"]) if cols["swh"] else np.zeros(0)
    mission = np.concatenate(cols["mission"]) if cols["mission"] else np.zeros(0, np.uint8)
    model, run_idx, edge, runs = collocate(frames, t, lat, lon, log=log)
    near = near_land_of(lat, lon) if len(t) else np.zeros(0, bool)
    flags = (near.astype(np.uint8) * F.FLAG_NEAR_LAND) | (edge.astype(np.uint8) * F.FLAG_MODEL_EDGE)
    arrays = {"t": F.to_stored(t, 1, None, "u4"), "lat": F.to_stored(lat, 1e-6, None, "i4"), "lon": F.to_stored(lon, 1e-6, None, "i4"),
              "swh": F.to_stored(swh, 0.001, F.NONE_U2, "u2"), "model": F.to_stored(np.clip(model, 0, 65.534), 0.001, F.NONE_U2, "u2"),
              "mission": mission.astype(np.uint8), "run": run_idx, "flags": flags}
    newest = float(t.max()) if len(t) else None
    credit = SOURCE["credit"] + ("; " + JASON3_SOURCE["credit"] if "j3_ogdr" in per and len(per["j3_ogdr"]["t"]) else "")
    header = {"window": window, "t0": utc(w0), "t1": utc(w1), "build": build_id, "missions": missions, "segments": segs, "runs": runs,
              "model": {"field": HS_FIELD, "lo": frames.lo if frames else None, "hi": frames.hi if frames else None, "rule": MODEL_RULE, "source": "NOAA GFS-Wave"},
              "inputs": inputs, "credit": credit, "flags": {"near_land": F.FLAG_NEAR_LAND, "model_edge": F.FLAG_MODEL_EDGE}}
    body = F.encode_chunk(F.MAGIC_ALT, header, {}, arrays)
    per_mission = {m["id"]: int((mission == i).sum()) for i, m in enumerate(missions) if (mission == i).any()}
    newest_by = {tok: utc(EPOCH + timedelta(seconds=float(d["t"].max()))) for tok, d in per.items() if len(d["t"])}
    ok = np.isfinite(model) & ~near
    diff = (swh - model)[ok]
    stats = {"window": window, "n": int(len(t)), "missions": per_mission, "bytes": len(body), "near_land": int(near.sum()), "model_edge": int(edge.sum()),
             "collocated": int(np.isfinite(model).sum()), "runs": runs,
             "obs_minus_model_open": {"n": int(len(diff)), "median": round(float(np.median(diff)), 3) if len(diff) else None,
                                      "mean": round(float(diff.mean()), 3) if len(diff) else None,
                                      "median_abs": round(float(np.median(np.abs(diff))), 3) if len(diff) else None,
                                      "p90_abs": round(float(np.percentile(np.abs(diff), 90)), 3) if len(diff) else None}}
    record = {"window": window, "t0": utc(w0), "t1": utc(w1), "n": int(len(t)), "bytes": len(body), "build": build_id,
              "missions": sorted(per_mission), "newest_utc": utc(EPOCH + timedelta(seconds=newest)) if newest is not None else None,
              "newest_by_mission": newest_by, "inputs": inputs}
    return body, record, stats


# ---------------------------------------------------------------- publish, prune, stale
def write_pointer(store, product, pointer, local=None):
    body = json.dumps(pointer, separators=(",", ":"), sort_keys=True, allow_nan=False).encode()
    if local is not None:
        _write_local(local, product.latest_key(), body)
    if store is not None:
        store.put(product.latest_key(), body, JSON, P.POINTER)


def _write_local(local, key, body):
    path = os.path.join(local, *key.split("/"))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(body)


def put_object(store, local, key, body, ctype, cache=P.IMMUTABLE):
    if local is not None:
        _write_local(local, key, body)
    if store is not None:
        store.put(key, body, ctype, cache)


def list_window_keys(store, product):
    """{window: [(build, key)]} of the chunk objects under the prefix."""
    out = {}
    pre = f"{product.prefix}/w/"
    for k in store.list_keys(pre):
        rest = k[len(pre):]
        parts = rest.split("/")
        if len(parts) == 2 and re.fullmatch(r"\d{8}T\d{2}", parts[0]) and re.fullmatch(r"\d{8}T\d{6}Z\.bin", parts[1]):
            out.setdefault(parts[0], []).append((parts[1][:-4], k))
    return out


def prune(store, product, pointer, now=None):
    """Windows older than PRUNE_AFTER_H and not named by the pointer go whole (their failure records too); in kept
    windows, builds the pointer does not name once superseded for BUILD_STALE_H (the pointer's `superseded` list says
    when: a client holding the previous pointer, max-age 300 + the 12-minute recheck, still reads them meanwhile) or,
    never named by any pointer, older than BUILD_STALE_H by their stamp; failure records of windows out of reach; stats not
    named and older than STATS_KEEP_H. Only under the prefix."""
    now = now or datetime.now(timezone.utc)
    named = {(c["window"], c["build"]) for c in pointer.get("chunks", [])}
    named_windows = {c["window"] for c in pointer.get("chunks", [])}
    named_builds = {c["build"] for c in pointer.get("chunks", [])} | ({pointer["build"]} if pointer.get("build") else set())
    superseded = {}
    for e in pointer.get("superseded") or []:
        try:
            superseded[e["key"]] = parse_iso(e["at"])
        except (KeyError, TypeError, ValueError):
            continue
    deleted = []
    for window, builds in sorted(list_window_keys(store, product).items()):
        w0 = parse_window(window)
        if window not in named_windows and w0 + timedelta(seconds=product.window_s) < now - timedelta(hours=PRUNE_AFTER_H):
            keys = [k for _b, k in builds]
            store.delete_keys(keys, window)
            deleted += keys
            fk = product.failed_key(window)
            if store.get_json(fk) is not None:
                store.delete_keys([fk], "failed")
                deleted.append(fk)
            continue
        for build, key in builds:
            if (window, build) in named:
                continue
            if key in superseded:
                age = now - superseded[key]
            else:
                try:
                    age = now - datetime.strptime(build, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
            if age > timedelta(hours=BUILD_STALE_H):
                store.delete_keys([key], window)
                deleted.append(key)
    fpre = f"{product.prefix}/failed/"
    for k in store.list_keys(fpre):
        w = k[len(fpre):]
        if not (w.endswith(".json") and re.fullmatch(r"\d{8}T\d{2}", w[:-5])):
            continue
        if parse_window(w[:-5]) + timedelta(seconds=product.window_s) < now - timedelta(hours=PRUNE_AFTER_H):
            store.delete_keys([k], "failed")
            deleted.append(k)
    spre = f"{product.prefix}/stats/"
    for k in store.list_keys(spre):
        b = k[len(spre):]
        if not b.endswith(".json"):
            continue
        b = b[:-5]
        if b in named_builds:
            continue
        try:
            age = now - datetime.strptime(b, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if age > timedelta(hours=STATS_KEEP_H):
            store.delete_keys([k], "stats")
            deleted.append(k)
    return deleted


def superseded_list(prev_pointer, new_chunks, now, prev_superseded=None):
    """The pointer's `superseded` entries: the previous pointer's chunk keys this run replaced (their window got a new
    key), stamped now, plus the earlier entries still within BUILD_STALE_H."""
    out = []
    for e in prev_superseded or []:
        try:
            if now - parse_iso(e["at"]) <= timedelta(hours=BUILD_STALE_H):
                out.append({"key": e["key"], "at": e["at"]})
        except (KeyError, TypeError, ValueError):
            continue
    keep = {c["key"] for c in new_chunks}
    for c in (prev_pointer or {}).get("chunks", []):
        if isinstance(c, dict) and c.get("key") and c["key"] not in keep and not any(e["key"] == c["key"] for e in out):
            out.append({"key": c["key"], "at": utc(now)})
    return out


def record_failure(store, product, window, digest, message):
    prev = store.get_json(product.failed_key(window)) or {"window": window, "attempts": 0}
    if prev.get("digest") != digest:
        prev = {"window": window, "attempts": 0, "digest": digest}
    prev["attempts"] += 1
    prev["last_attempt_utc"] = utc()
    prev["last_error"] = P.redact(message, store.bucket)
    store.put(product.failed_key(window), json.dumps(prev, sort_keys=True).encode(), JSON, P.POINTER)
    return prev


def skip_failed(store, product, window, digest, now=None):
    """After FAILED_MAX_ATTEMPTS on the same inputs the window waits FAILED_RETRY_AFTER_H since the last try (a lasting
    upstream fault is retried a few times a day, never every 10 minutes, never given up for good)."""
    rec = store.get_json(product.failed_key(window))
    if not (rec and rec.get("digest") == digest and rec.get("attempts", 0) >= FAILED_MAX_ATTEMPTS):
        return False
    try:
        last = parse_iso(rec["last_attempt_utc"])
    except (KeyError, TypeError, ValueError):
        return False
    return ((now or datetime.now(timezone.utc)) - last) < timedelta(hours=FAILED_RETRY_AFTER_H)


def alarm_once(store, product, line, now=None, kind="stale"):
    """1 when no alarm of this kind went out in STALE_ALARM_EVERY_H (the record stale-alarm.json keeps one stamp per
    kind; GitHub mails a failed run): the summary line is written then; else None (the line is printed only)."""
    now = now or datetime.now(timezone.utc)
    rec = store.get_json(product.stale_key()) or {}
    field = "last_alarm_utc" if kind == "stale" else f"last_{kind}_alarm_utc"
    last = rec.get(field)
    if last:
        try:
            if (now - parse_iso(last)).total_seconds() < STALE_ALARM_EVERY_H * 3600:
                print(f"{line} (alarm already sent)")
                return None
        except ValueError:
            pass
    rec[field] = utc(now)
    store.put(product.stale_key(), json.dumps(rec, sort_keys=True).encode(), JSON, P.POINTER)
    _summary(line)
    return 1


def stale_check(store, product, pointer, now=None, why=""):
    """1 when the newest reading the pointer names is older than the product's stale_h (none published counts as stale)
    and no alarm went out in STALE_ALARM_EVERY_H; else None."""
    now = now or datetime.now(timezone.utc)
    newest = pointer.get("newest_utc") if pointer else None
    age_h = math.inf
    if newest:
        try:
            age_h = (now - parse_iso(newest)).total_seconds() / 3600
        except ValueError:
            age_h = math.inf
    if age_h <= product.stale_h:
        return None
    return alarm_once(store, product, f"STALE: the newest {product.label} reading is {newest or 'none'} ({age_h:.1f} h old){': ' + why if why else ''}", now=now)


def mission_newest(chunks, prev_missions):
    """{token: newest_utc} over the pointer's chunks in reach, else the previous pointer's value (a mission that has
    gone quiet keeps its last time until it falls out of reach of the warning)."""
    out = {}
    for m in prev_missions or []:
        if isinstance(m, dict) and isinstance(m.get("id"), str) and isinstance(m.get("newest_utc"), str):
            out[m["id"]] = m["newest_utc"]
    for c in chunks:
        for tok, t in (c.get("newest_by_mission") or {}).items():
            if isinstance(t, str) and (tok not in out or t > out[tok]):
                out[tok] = t
    return out


def mission_silence(newest, now):
    """[(token, hours since its newest reading)] for the missions whose newest reading is older than their threshold."""
    out = []
    for tok, t in sorted(newest.items()):
        try:
            h = (now - parse_iso(t)).total_seconds() / 3600
        except ValueError:
            continue
        if h > MISSION_SILENT_OVERRIDE_H.get(tok, MISSION_SILENT_H):
            out.append((tok, round(h, 1)))
    return out


def _store_from_env():
    env = os.environ
    client = P.r2_client(env["R2_ACCOUNT_ID"], env["R2_ACCESS_KEY_ID"], env["R2_SECRET_ACCESS_KEY"])
    return P.Store(client, env["R2_BUCKET"])


# ---------------------------------------------------------------- Jason-3 OGDR (the fourth height source)
def jason3_index_files(index_url, now, get=_get, since=None):
    """[FileRec] from an HTML index of OGDR files (NOAA's directory listing): names JA3_OPN_2P?S<cycle>_<pass>_<start>_
    <end>.nc; only the files whose span ends after `since`."""
    with get(index_url, timeout=60) as r:
        html = r.read(MAX_FILE_BYTES).decode("utf-8", "replace")
    out = {}
    for m in re.finditer(r'href="([^"]+\.nc)"', html):
        name = m.group(1).rsplit("/", 1)[-1]
        mm = JASON3_FILE_RE.match(name)
        if not mm:
            continue
        try:
            t0 = datetime.strptime(mm.group(3) + mm.group(4), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            t1 = datetime.strptime(mm.group(5) + mm.group(6), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if since and t1 <= since:
            continue
        href = m.group(1) if "://" in m.group(1) else index_url.rstrip("/") + "/" + m.group(1).lstrip("./")
        out[name] = FileRec("j3_ogdr", name, href, 0, t0, t1, t1)        # size unknown (0): bounded by MAX_FILE_BYTES
    return sorted(out.values(), key=lambda r: r.t0)


# ---------------------------------------------------------------- main
HEIGHTS = Product(prefix="alt/v1", fmt="alt-v1", magic=F.MAGIC_ALT, stac_id="WAVE_GLO_PHY_SWH_L3_NRT_014_001", dataset_re=DATASET_RE,
                  file_re=FILE_RE, reader=read_l3, missions=MISSIONS, source=SOURCE, columns=F.ALT_COLUMNS, collocate=True,
                  build=build_heights, format_key="alt-v1/1", label="satellite wave height")


def _pointer_datasets(pointer):
    ds = (pointer or {}).get("datasets")
    return ds if isinstance(ds, dict) and all(isinstance(k, str) and isinstance(v, str) for k, v in ds.items()) and ds else None


def discover_datasets(product, pointer, get=_get, log=print, now=None):
    """The pointer's cached list while younger than DATASETS_REFRESH_H (STAC is read a few times a day, not every 10
    minutes), else STAC, else the cached list whatever its age, else the bucket's directory listing. -> (datasets, how,
    read_utc)."""
    now = now or datetime.now(timezone.utc)
    cached, read = _pointer_datasets(pointer), (pointer or {}).get("datasets_utc")
    if cached and isinstance(read, str):
        try:
            if now - parse_iso(read) < timedelta(hours=DATASETS_REFRESH_H):
                return cached, "pointer", read
        except ValueError:
            pass
    try:
        return stac_datasets(product, get=get), "stac", utc(now)
    except Exception as exc:                                           # noqa: BLE001
        log(f"STAC unavailable ({exc.__class__.__name__}: {exc}); falling back")
    if cached:
        return cached, "pointer", read if isinstance(read, str) else utc(now)
    return bucket_datasets(product, get=get), "bucket", utc(now)


def main(argv=None, get=_get, store=None, product=HEIGHTS, frames_factory=None, now=None, readers=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="plan, download and build; upload nothing")
    ap.add_argument("--local", default=None, help="also write every object under this folder")
    ap.add_argument("--window", default=None, help="YYYYMMDDTHH: build this one window (forced; no stale alarm; a failure exits 1)")
    ap.add_argument("--force", action="store_true", help="rebuild every window in reach")
    ap.add_argument("--keep-h", type=int, default=product.keep_h)
    ap.add_argument("--frames-root", default=os.environ.get("ALT_FRAMES_ROOT", FRAMES_ROOT))
    ap.add_argument("--no-model", action="store_true", help="no collocation (the model column stays empty)")
    ap.add_argument("--jason3-index", default=os.environ.get("ALT_JASON3_INDEX", ""), help="HTML index URL of NOAA's Jason-3 OGDR files (empty: no OGDR)")
    a = ap.parse_args(argv)
    if a.keep_h < 3 or (a.window and not re.fullmatch(r"\d{8}T\d{2}", a.window)):
        print("bad arguments (--keep-h >= 3; --window YYYYMMDDTHH)")
        return 2
    now = now or datetime.now(timezone.utc)
    live = a.dry_run is False and a.local is None
    if live and store is None:
        store = _store_from_env()
    pointer = (store.get_json(product.latest_key()) if store is not None else None) or {}
    if pointer.get("format") not in (None, product.format):
        print(f"pointer format {pointer.get('format')!r} is not {product.format!r}: refusing to touch it")
        return 1
    # every quiet path of a scheduled live run ends in the stale check (the SST rule); a manual --window run, a dry run
    # or a local build reports its own failure as 1
    scheduled = live and not a.window
    stale = (lambda why: (stale_check(store, product, pointer, now=now, why=why) or 0)) if scheduled else (lambda why: 0)
    quiet = (lambda why: stale(why)) if scheduled else (lambda why: 1)
    since = now - timedelta(hours=a.keep_h)

    try:
        datasets, how, datasets_utc = discover_datasets(product, pointer, get=get, now=now)
    except Exception as exc:                                           # noqa: BLE001
        print(f"no dataset list: {exc.__class__.__name__}: {exc}")
        return quiet("the dataset list cannot be read")
    print(f"{len(datasets)} datasets ({how})")
    files = list_files(product, datasets, now, get=get, since=since)
    if a.jason3_index and product is HEIGHTS:
        try:
            files += jason3_index_files(a.jason3_index, now, get=get, since=since)
        except Exception as exc:                                       # noqa: BLE001
            _summary(f"WARNING: Jason-3 OGDR index unavailable ({exc.__class__.__name__}: {exc})")
    if not files:
        print("no files in reach")
        if live and pointer:
            pointer["checked_utc"] = utc(now)
            write_pointer(store, product, pointer)
        return quiet("no product files listed")

    # the model frames first (their run is part of the newest windows' digests): a frames host that cannot be read ends
    # this run quietly (nothing is charged to the windows: the next run tries again); a range change alarms once per
    # STALE_ALARM_EVERY_H and publishes nothing (a deliberate alt change is needed first)
    frames = None
    if product.collocate and not a.no_model:
        try:
            frames = (frames_factory or (lambda: Frames(root=a.frames_root, get=get)))().load()
        except Frames.RangeChanged as exc:
            line = f"MODEL RANGE CHANGED: {exc}"
            if live:
                return alarm_once(store, product, line, now=now, kind="range") or 0
            _summary(line)
            return 1
        except Exception as exc:                                       # noqa: BLE001
            print(f"model frames unavailable: {exc.__class__.__name__}: {exc}")
            return quiet("the model frames cannot be read")
    pl = plan(product, pointer, files, now, force=a.force, only=a.window, since=since, run=frames.runs()[0] if frames else None)
    todo = [k for k, v in pl.items() if v["build"]]
    if live and not a.force and not a.window:
        todo = [k for k in todo if not skip_failed(store, product, k, pl[k]["digest"], now=now)]
    print(f"{len(files)} files, {len(pl)} windows in reach, {len(todo)} to build: {' '.join(todo) if todo else '-'}")

    build_id = stamp(now)
    chunks = {c["window"]: c for c in pointer.get("chunks", []) if isinstance(c, dict) and "window" in c}
    stats_all, built, failed_windows = {}, [], []
    needed = {}
    for k in todo:
        for f in pl[k]["files"]:
            needed[f.name] = f
    tmp = tempfile.TemporaryDirectory()
    try:
        paths, dl_failed = download_all(list(needed.values()), tmp.name, get=get)
        for name, err in sorted(dl_failed.items()):
            print(f"download failed: {name}: {err}")
        read_cache, unreadable = {}, {}
        for k in todo:
            w = pl[k]
            w0, w1 = w["t0"], w["t0"] + timedelta(seconds=product.window_s)
            read, missing = {}, []
            for f in w["files"]:
                if f.name not in paths:
                    missing.append(f"{f.name} ({dl_failed.get(f.name, 'not downloaded')})")
                    continue
                try:
                    if f.name not in read_cache:
                        rd = (readers or {}).get(f.token) or (read_jason3_ogdr if f.token == "j3_ogdr" else product.reader)
                        read_cache[f.name] = rd(paths[f.name])
                    d = read_cache[f.name]
                    sel = in_window(d["t"], w0, w1)
                    read[f.name] = (f.token, {kk: vv[sel] for kk, vv in d.items()})
                except Exception as exc:                               # noqa: BLE001
                    if f.name not in unreadable:
                        unreadable[f.name] = f"{exc.__class__.__name__}: {exc}"
                        _summary(f"WARNING: {f.name} unreadable: {unreadable[f.name]}")   # a refused file (a mean period, a wrong scale) is said out loud
                    missing.append(f"{f.name} ({unreadable[f.name]})")
            if missing:
                # a file that failed is dropped from this build and the window recorded (inputs null: tried again next run;
                # after FAILED_MAX_ATTEMPTS on the same digest, every FAILED_RETRY_AFTER_H)
                msg = f"{len(missing)} of {len(w['files'])} files missing: {'; '.join(missing[:4])}"
                print(f"{k}: {msg}")
                if live:
                    record_failure(store, product, k, w["digest"], msg)
                failed_windows.append(k)
                if not read:
                    continue
            try:
                missions = mission_index({tok for tok, _d in read.values()}, pointer.get("missions"), product.missions)
                body, record, st = product.build(k, w0, w1, read, missions, frames, build_id, None if missing else w["digest"], log=print)
            except Exception as exc:                                   # noqa: BLE001
                print(f"{k}: build failed: {exc.__class__.__name__}: {exc}")
                if live:
                    record_failure(store, product, k, w["digest"], f"build: {exc}")
                if k not in failed_windows:
                    failed_windows.append(k)
                continue
            if len(body) > F.WARN_CHUNK_BYTES:
                _summary(f"WARNING: {k} chunk is {len(body) / 1e6:.2f} MB")
            key = product.chunk_key(k, build_id)
            record["key"] = key
            put_object(store if live else None, a.local, key, body, BIN)
            chunks[k] = record
            stats_all[k] = st
            built.append(k)
            print(f"{k}: {record['n']} readings, {len(body) / 1e3:.0f} KB, missions {record['missions']}")
    finally:
        tmp.cleanup()

    # the pointer: every window in reach, newest last, dropping windows out of reach; stats sidecar first; pointer last
    in_reach = [k for k in sorted(chunks) if parse_window(k) + timedelta(seconds=product.window_s) > since]
    while len(in_reach) > MAX_CHUNKS:
        in_reach.pop(0)
    chunk_list = [chunks[k] for k in in_reach]
    newest = max((c["newest_utc"] for c in chunk_list if c.get("newest_utc")), default=pointer.get("newest_utc"))
    newest_by = mission_newest(chunk_list, pointer.get("missions"))
    missions_all = mission_index({m for c in chunk_list for m in c.get("missions", [])} | set(newest_by), pointer.get("missions"), product.missions)
    for m in missions_all:
        if m["id"] in newest_by:
            m["newest_utc"] = newest_by[m["id"]]
    changed = bool(built)
    model = None
    if product.collocate and not a.no_model and frames:
        model = {"name": "NOAA GFS-Wave", "field": HS_FIELD, "hs": [frames.lo, frames.hi], "runs": [P.run_key(r) for r in frames.runs()], "rule": MODEL_RULE}
    new_pointer = {"format": product.format, "build": build_id if changed else pointer.get("build"),
                   "published_utc": utc(now) if changed else pointer.get("published_utc"), "checked_utc": utc(now),
                   "newest_utc": newest, "window_s": product.window_s, "keep_h": a.keep_h, "encoding": "deflate",
                   "magic": product.magic.decode("ascii"), "missions": missions_all, "chunks": chunk_list, "model": model,
                   "columns": [list(c) for c in product.columns], "source": product.source, "datasets": datasets, "datasets_utc": datasets_utc,
                   "stats": product.stats_key(build_id) if changed else pointer.get("stats"),
                   "superseded": superseded_list(pointer, chunk_list, now, pointer.get("superseded"))}
    if changed:
        sbody = json.dumps({"build": build_id, "windows": stats_all, "built": built, "failed": failed_windows}, separators=(",", ":"), sort_keys=True, allow_nan=False).encode()
        put_object(store if live else None, a.local, product.stats_key(build_id), sbody, JSON)
    if live or a.local is not None:
        write_pointer(store if live else None, product, new_pointer, local=a.local)
    if live:
        try:
            print("pruned:", len(prune(store, product, new_pointer, now=now)))
        except Exception as exc:                                       # noqa: BLE001
            print(f"prune failed: {exc.__class__.__name__}: {exc}")
    for tok, h in mission_silence(newest_by, now):
        _summary(f"WARNING: mission {tok} silent for {h} h")
    tot = sum(c["n"] for c in chunk_list)
    _summary(f"### {product.label}: {len(built)} windows built ({' '.join(built) if built else '-'}), {len(chunk_list)} in the pointer, "
             f"{tot} readings, newest {newest or '-'}, {len(files)} files, {len(datasets)} datasets ({how})" + (f", frames fetched {frames.fetched}" if frames else ""))
    for k in built:
        st = stats_all[k]
        d = st["obs_minus_model_open"] if "obs_minus_model_open" in st else {}
        print(f"  {k}: {st['n']} readings {st.get('missions')}" + (f", obs - model open ocean: n {d['n']} median {d['median']} |median| {d['median_abs']} p90 {d['p90_abs']}" if d.get("n") else ""))
    if failed_windows and not built:
        return quiet("every window failed to build")
    return stale("nothing new to publish") if scheduled and not built else 0


if __name__ == "__main__":
    sys.exit(main())
