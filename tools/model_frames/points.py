"""Forecast-point job: every sea cell's wave partitions and wind for the newest COMPLETE GFS-Wave run, as block
tiles on R2 (format: pointfmt.py). The site reads one tile to build a forecast table for any ocean point.

python tools/model_frames/points.py         # env: R2_ACCOUNT_ID R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_BUCKET
Options:
  --dry-run          build everything, upload nothing
  --local DIR        publish into a directory instead of R2 (development; same layout)
  --steps 0,3,6      subset (testing): only with --dry-run or --local, and the pointer is never flipped for it
  --run YYYYMMDDHH   build this cycle instead of the newest complete one (testing): only with --dry-run or --local
  --force            rebuild even if the run is current or older than the live run
  --keep N           complete runs to retain (default 2)
  --workers N        decode processes (default 4; 0 = in this process)
  --work DIR         where the build's scratch files go (default: a fresh temporary directory)
Exit codes (as run.py): 0 published / already current / newer run live / pointer repaired; 3 no complete run
available or a needed object vanished mid-run; 1 build or publish failure; 2 bad arguments, or a refusal to
rewrite a run published in another format.
"No complete run" is not an error and leaves no record: if NOAA renames its files, every tick ends that way and
the only signal is the age of the run latest.json names.

Layout (PREFIX = gfswave/points/v1; its own prefix, so the frames job's pruning never sees it and this job's
never sees the frames; the version segment changes whenever the format does):
  <PREFIX>/<RUN>/<grid>/<tr>_<tc>.bin        one tile                    (immutable)
  <PREFIX>/<RUN>/<grid>/mask.bin             the grid's sea mask         (immutable)
  <PREFIX>/<RUN>/stats-<published>.json      per step / per tile numbers (immutable)
  <PREFIX>/<RUN>/manifest-<published>.json   one per publish             (immutable)
  <PREFIX>/latest.json -> {run, manifest, complete: true, ...}           (max-age=300, written LAST)
  <PREFIX>/failed/<RUN>.json, <PREFIX>/notready/<RUN>.json               attempt records
The pointer only ever names a COMPLETE run: every tile and mask is stored before the manifest, the manifest
before the pointer.

How a build runs: each step's three files (one per grid) are downloaded whole and decoded in worker PROCESSES
(eccodes is not thread-safe); every worker writes its step's values for the sea cells straight into a scratch
file on disk ([step, field, cell], cells in tile order: a whole run is ~5.5 GB, more than fits in memory).
Then the tiles are cut from that file one tile row at a time, compressed in threads and uploaded.
The sea cells of a grid are the cells with a wave height at the build's FIRST step (NOAA's land and ice mask is
the same at every step of a run). A later step that disagrees is counted: a cell that lost its waves is stored as
missing there, one that gained waves is not in the product; the manifest says how many ("sea_cells"), the summary
warns, and beyond DRIFT_FAIL of a grid's cells the build fails instead of publishing.
Each grid's rows with waves must be the ones pointfmt.GRIDS expects ("data"): NOAA's files span more rows than
they hold waves for, and a change of those bands would open a hole between the grids.
"""
import argparse
import hashlib
import io
import json
import os
import shutil
import sys
import tempfile
import time
import zlib
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import fetch as F        # noqa: E402
import publish as P      # noqa: E402
import pointfmt as PF    # noqa: E402

PREFIX = "gfswave/points/v1"
LATEST_KEY = f"{PREFIX}/latest.json"
BINARY = "application/octet-stream"
WORKERS = 4                              # decode processes
COMPRESS_WORKERS = 4                     # xz threads (lzma releases the GIL)
UPLOAD_WORKERS = 8
UPLOAD_BACKLOG = 64                      # tile uploads allowed to trail the build: bounds memory
FAILED_RETRY_AFTER_S = 3 * 3600
FAILED_MAX_ATTEMPTS = 3
NOTREADY_WARN = 6
FILE_TIMEOUT_S = 180                     # one whole gridded file: 5-12 MB
FILE_TRIES = 3                           # whole downloads of one file (each already retries its request 4 times)
SCRATCH_SLACK = 64 << 20                 # free space wanted beyond the scratch files themselves
DRIFT_FAIL = 0.01                        # share of a grid's sea cells that may differ from the first step's at any step
# what a decoded field may hold at most; anything beyond is a record that is not what its identity says
PLAUSIBLE_MAX = {"height": 40.0, "period": 60.0, "direction": 360.0001, "speed": 150.0}

MODEL = {
    "name": "NOAA/NCEP GFS-Wave (WAVEWATCH III)",
    "attribution": ("Source: NOAA/NCEP GFS-Wave (WAVEWATCH III) via NOAA Open Data Dissemination; "
                    "processed by Allshore Surf. Not an official NWS product."),
    "definition": ("Per model cell and step: significant height of combined wind waves and swell; the wind sea and "
                   "up to three swell partitions (significant height, PEAK period, mean direction the waves come "
                   "FROM); the 10 m wind the wave model was forced with (speed, direction it blows FROM)."),
}


def _summary(line):
    print(line)
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a") as fh:
            fh.write(line + "\n")


def format_key():
    """What a run's objects were built with; a published run is never rewritten under another one."""
    spec = {"format": PF.FORMAT, "missing": PF.MISSING, "kinds": PF.KINDS, "codec": "xz", "steps": F.STEP_SCHEDULE,
            "fields": [[f[0], f[1], list(f[3])] for f in PF.FIELDS],
            "grids": [[g["name"], g["tag"], g["ni"], g["nj"], g["lat0"], g["per_deg"], list(g["rows"]), g["tile"]] for g in PF.GRIDS]}
    return hashlib.sha256(json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]


# ------------------------------- NOAA side -------------------------------------------

def _grid_prefix(run_dt, grid):
    d, h = run_dt.strftime("%Y%m%d"), run_dt.strftime("%H")
    return f"gfs.{d}/{h}/wave/gridded/gfswave.t{h}z.{grid['tag']}.f"


def grid_url(run_dt, grid, step):
    return f"{F.S3}/{_grid_prefix(run_dt, grid)}{step:03d}.grib2"


def needed_keys(run_dt, steps=None):
    return [f"{_grid_prefix(run_dt, g)}{s:03d}.grib2" for s in (steps or F.STEPS) for g in PF.GRIDS]


def missing_objects(run_dt):
    present = set()
    for g in PF.GRIDS:
        present |= set(F.list_keys(_grid_prefix(run_dt, g)))
    return [k for k in needed_keys(run_dt) if k not in present]


def run_is_complete(run_dt):
    return not missing_objects(run_dt)


def latest_complete_run(now=None, lookback_cycles=8):
    """Newest cycle whose every gridded file (all grids, all steps) is on the bucket. Transport failures raise."""
    for cand in F.candidate_runs(now, lookback_cycles):
        if run_is_complete(cand):
            return cand
    return None


def messages(buf):
    """A GRIB2 file -> its messages (pure). Raises unless it is a whole number of edition-2 messages, which also
    catches a truncated download."""
    out, pos, n = [], 0, len(buf)
    while pos < n:
        if pos + 16 > n or buf[pos:pos + 4] != b"GRIB" or buf[pos + 7] != 2:
            raise ValueError(f"not a GRIB2 message at byte {pos}")
        total = int.from_bytes(buf[pos + 8:pos + 16], "big")
        if total < 20 or pos + total > n or buf[pos + total - 4:pos + total] != b"7777":
            raise ValueError(f"truncated GRIB2 message at byte {pos}")
        out.append(bytes(buf[pos:pos + total]))
        pos += total
    if not out:
        raise ValueError("empty GRIB2 file")
    return out


def fetch_file(run_dt, grid, step):
    """One whole gridded file as its list of messages. 404 -> F.NotReady (at once). A transport failure or a
    damaged body is downloaded again, FILE_TRIES times in all, then -> F.TransportError: one bad download among
    the 627 of a run must not cost the run."""
    url = grid_url(run_dt, grid, step)
    last = None
    for attempt in range(FILE_TRIES):
        if attempt:
            time.sleep(5 * attempt)
        try:
            _status, body = F._request(url, timeout=FILE_TIMEOUT_S)
            return messages(body)
        except F.TransportError as exc:
            last = str(exc)
        except ValueError as exc:
            last = f"{url}: {exc}"
    raise F.TransportError(f"{last} ({FILE_TRIES} downloads)")


_LONG_KEYS = ("discipline", "parameterCategory", "parameterNumber", "typeOfFirstFixedSurface",
              "scaleFactorOfFirstFixedSurface", "scaledValueOfFirstFixedSurface", "productDefinitionTemplateNumber",
              "forecastTime", "indicatorOfUnitOfTimeRange", "dataDate", "dataTime", "Ni", "Nj",
              "jScansPositively", "iScansNegatively", "jPointsAreConsecutive", "alternativeRowScanning")
_DOUBLE_KEYS = ("latitudeOfFirstGridPointInDegrees", "latitudeOfLastGridPointInDegrees",
                "longitudeOfFirstGridPointInDegrees", "longitudeOfLastGridPointInDegrees",
                "iDirectionIncrementInDegrees", "jDirectionIncrementInDegrees", "missingValue")
_IDENTITY = {f[3]: f[0] for f in PF.FIELDS}


def identify(meta):
    """The field a message holds (by its GRIB2 code numbers, which no eccodes version renames), or None for a
    record the product does not use. A swell partition is told apart by its sequence number."""
    base = (meta["discipline"], meta["parameterCategory"], meta["parameterNumber"], meta["typeOfFirstFixedSurface"])
    if meta["typeOfFirstFixedSurface"] == 241:
        if meta["scaleFactorOfFirstFixedSurface"] != 0:
            return None
        return _IDENTITY.get(base + (meta["scaledValueOfFirstFixedSurface"],))
    return _IDENTITY.get(base + (None,))


def check_time(meta, run_dt, step):
    """Raise unless the message is an instantaneous forecast for exactly this cycle and hour."""
    if meta["dataDate"] != int(run_dt.strftime("%Y%m%d")) or meta["dataTime"] != run_dt.hour * 100:
        raise ValueError(f"cycle {meta['dataDate']}/{meta['dataTime']} != {run_dt:%Y%m%d/%H00}")
    if meta["productDefinitionTemplateNumber"] != 0 or meta["indicatorOfUnitOfTimeRange"] != 1 or meta["forecastTime"] != step:
        raise ValueError(f"forecast time {meta['forecastTime']} (unit {meta['indicatorOfUnitOfTimeRange']}, "
                         f"template {meta['productDefinitionTemplateNumber']}) != +{step} h")


def check_geometry(meta, grid):
    """Raise unless the message is on exactly the grid the product stores (rows north to south, columns from
    0 E eastward, row-major)."""
    step = 1.0 / grid["per_deg"]
    last_lat = grid["lat0"] - (grid["nj"] - 1) * step
    last_lon = (grid["ni"] - 1) * step
    if meta.get("gridType") != "regular_ll" or (meta["Ni"], meta["Nj"]) != (grid["ni"], grid["nj"]):
        raise ValueError(f"unexpected grid {meta.get('gridType')} {meta['Ni']}x{meta['Nj']} for {grid['tag']}")
    if meta["jScansPositively"] or meta["iScansNegatively"] or meta["jPointsAreConsecutive"] or meta["alternativeRowScanning"]:
        raise ValueError("expected rows north to south, columns eastward, row-major")
    if abs(meta["latitudeOfFirstGridPointInDegrees"] - grid["lat0"]) > 1e-4 or abs(meta["latitudeOfLastGridPointInDegrees"] - last_lat) > 1e-3:
        raise ValueError("unexpected latitudes")
    if abs(meta["longitudeOfFirstGridPointInDegrees"]) > 1e-4 or abs(meta["longitudeOfLastGridPointInDegrees"] - last_lon) > 1e-3:
        raise ValueError("unexpected longitudes")
    if abs(meta["iDirectionIncrementInDegrees"] - step) > 1e-5 or abs(meta["jDirectionIncrementInDegrees"] - step) > 1e-5:
        raise ValueError("unexpected spacing")


def read_message(msg, wanted):
    """GRIB message bytes -> (meta, values or None): the values are decoded only when wanted(meta) says so."""
    import eccodes
    h = eccodes.codes_new_from_message(msg)
    try:
        meta = {k: eccodes.codes_get_long(h, k) for k in _LONG_KEYS}
        meta.update({k: eccodes.codes_get_double(h, k) for k in _DOUBLE_KEYS})
        meta["gridType"] = eccodes.codes_get_string(h, "gridType")
        vals = eccodes.codes_get_values(h) if wanted(meta) else None
    finally:
        eccodes.codes_release(h)
    return meta, vals


def decode_file(msgs, grid, run_dt, step):
    """A gridded file's messages -> {field: float64 [nj, ni], NaN = missing} for the product's 15 fields. Every
    used record is checked for its cycle, hour, grid and a plausible range; a missing or duplicated one raises."""
    out = {}
    for msg in msgs:
        meta, vals = read_message(msg, lambda m: identify(m) is not None)
        name = identify(meta)
        if name is None:
            continue
        if name in out:
            raise ValueError(f"{grid['tag']} f{step:03d}: two records for {name}")
        check_time(meta, run_dt, step)
        check_geometry(meta, grid)
        arr = np.asarray(vals, dtype=np.float64)
        if arr.size != grid["ni"] * grid["nj"]:
            raise ValueError(f"{name}: {arr.size} values for a {grid['ni']}x{grid['nj']} grid")
        arr = np.where(arr == meta["missingValue"], np.nan, arr)
        ok = arr[np.isfinite(arr)]
        if ok.size != np.count_nonzero(~np.isnan(arr)):
            raise ValueError(f"{name}: non-finite values")
        if ok.size and (ok.min() < 0 or ok.max() > PLAUSIBLE_MAX[PF.FIELD_KIND[name]]):
            raise ValueError(f"{grid['tag']} f{step:03d} {name}: values {ok.min():.3f}..{ok.max():.3f} are not a {PF.FIELD_KIND[name]}")
        out[name] = arr.reshape(grid["nj"], grid["ni"])               # float64: the quantiser's ties (pointfmt.TIE)
    lack = [n for n in PF.FIELD_NAMES if n not in out]
    if lack:
        raise ValueError(f"{grid['tag']} f{step:03d}: records missing: {lack}")
    return out


# ------------------------------- the build's scratch files ---------------------------

def sea_mask(hs, grid):
    """The grid's sea cells: a wave height present, inside the rows the product stores."""
    mask = np.isfinite(hs)
    r0, r1 = grid["rows"]
    mask[:r0] = False
    mask[r1:] = False
    return mask


def check_band(mask, grid):
    """-> (first, last) row with sea cells. Raises unless they are the rows the grid's "data" names (None = not
    checked: sea ice decides): the bands NOAA's files hold waves for are narrower than the files, the other grids
    are stored up to them, and a change would open a hole between the grids (or hide an overlap)."""
    rows = np.flatnonzero(mask.any(axis=1))
    got = (int(rows[0]), int(rows[-1]))
    for have, want in zip(got, grid["data"]):
        if want is not None and have != want:
            raise ValueError(f"{grid['tag']}: waves on rows {got[0]}..{got[1]}, expected {grid['data'][0]}..{grid['data'][1]}: "
                             f"NOAA's grid bands have changed (pointfmt.GRIDS)")
    return got


def step_record(fields, cells, grid):
    """One step of one grid -> (uint16 [field, cell], numbers for the stats)."""
    rec = np.empty((len(PF.FIELDS), cells.size), dtype="<u2")
    for i, (name, kind, _noaa, _ident) in enumerate(PF.FIELDS):
        rec[i] = PF.quantise(fields[name].ravel()[cells], kind)
    r0, r1 = grid["rows"]
    hs = fields["hs"]
    here = rec[PF.FIELD_NAMES.index("hs")] != PF.MISSING
    ragged = 0
    for part in PF.PARTITIONS:
        present = np.stack([rec[PF.FIELD_NAMES.index(n)] != PF.MISSING for n in part])
        ragged += int(np.count_nonzero(present.any(0) & ~present.all(0)))
    stat = {"hs_lost": int(cells.size - np.count_nonzero(here)),                        # sea cells with no height at this step
            "hs_extra": int(np.count_nonzero(np.isfinite(hs[r0:r1])) - np.count_nonzero(here)),   # heights outside the sea cells
            "ragged": ragged,                                                           # a partition with only some of its three values
            "wind_missing": int(np.count_nonzero(rec[PF.FIELD_NAMES.index("wind")] == PF.MISSING)),
            "hs_max": float(np.nanmax(hs[r0:r1])) if np.isfinite(hs[r0:r1]).any() else 0.0,
            "crc": zlib.crc32(rec.tobytes()) & 0xFFFFFFFF}                              # of every stored value of this step
    return rec, stat


def run_digest(step_stats):
    """One short hash over every stored value of the build (each grid-step's crc, in a fixed order): two builds of
    the same NOAA files agree on it exactly when they stored the same values, whatever decoded them."""
    h = hashlib.sha256()
    for s in sorted(step_stats, key=lambda s: (s["grid"], s["step"])):
        h.update(f"{s['grid']}:{s['step']}:{s['crc']:08x};".encode())
    return h.hexdigest()[:16]


def _paths(work, gname):
    return os.path.join(work, f"{gname}.cells.npy"), os.path.join(work, f"{gname}.u16")


_CELLS = {}


def _cells(work, gname):
    key = (work, gname)
    if key not in _CELLS:
        if any(k[0] != work for k in _CELLS):
            _CELLS.clear()                                         # one build at a time per process
        _CELLS[key] = np.load(_paths(work, gname)[0])
    return _CELLS[key]


def _write_step(work, gname, nsteps, si, rec):
    mm = np.memmap(_paths(work, gname)[1], dtype="<u2", mode="r+", shape=(nsteps, rec.shape[0], rec.shape[1]))
    mm[si] = rec
    mm.flush()
    del mm


def _step_task(job):
    """One (grid, step): download, decode, write its values. Runs in a worker process (or inline)."""
    run, gname, step, si, nsteps, work = job
    grid = PF.GRID_BY_NAME[gname]
    run_dt = datetime.strptime(run, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    t = time.time()
    fields = decode_file(fetch_file(run_dt, grid, step), grid, run_dt, step)
    rec, stat = step_record(fields, _cells(work, gname), grid)
    _write_step(work, gname, nsteps, si, rec)
    stat.update(grid=gname, step=step, seconds=round(time.time() - t, 2))
    return stat


class Inline:
    """The executor used with --workers 0 and in tests: everything in this process, in order."""

    def map(self, fn, jobs):
        return map(fn, jobs)

    def shutdown(self, **kw):
        pass


def decode_run(run_dt, steps, work, executor, log=print):
    """Fill the scratch files for every grid and step.
    -> ({grid name: (mask, cells, tiles, (first, last) row with sea)}, step stats)."""
    run = P.run_key(run_dt)
    layouts, stats, first = {}, [], []
    for grid in PF.GRIDS:                                          # the first step, here: it fixes the sea cells
        t = time.time()
        fields = decode_file(fetch_file(run_dt, grid, steps[0]), grid, run_dt, steps[0])
        mask = sea_mask(fields["hs"], grid)
        cells, tiles = PF.layout(mask, grid["tile"])
        if not cells.size:
            raise ValueError(f"{grid['tag']}: no sea cells at f{steps[0]:03d}")
        data_rows = check_band(mask, grid)
        rec, stat = step_record(fields, cells, grid)
        stat.update(grid=grid["name"], step=steps[0], seconds=round(time.time() - t, 2))
        first.append((grid, mask, cells, tiles, data_rows, rec, stat))
        del fields
    # the scratch files are sparse when created, so the space is checked for all of them together, before any
    need = sum(len(steps) * len(PF.FIELDS) * f[2].size * 2 for f in first)
    free = shutil.disk_usage(work).free
    if free < need * 1.05 + SCRATCH_SLACK:
        raise RuntimeError(f"{need / 1e9:.2f} GB of scratch space needed, {free / 1e9:.2f} GB free in {work}")
    for grid, mask, cells, tiles, data_rows, rec, stat in first:
        cells_path, mm_path = _paths(work, grid["name"])
        np.save(cells_path, cells)
        _CELLS[(work, grid["name"])] = cells                       # never a list left by an earlier build in this process
        np.memmap(mm_path, dtype="<u2", mode="w+", shape=(len(steps), len(PF.FIELDS), cells.size)).flush()
        _write_step(work, grid["name"], len(steps), 0, rec)
        stats.append(stat)
        layouts[grid["name"]] = (mask, cells, tiles, data_rows)
        log(f"{grid['name']} f{steps[0]:03d}: {cells.size} sea cells in {len(tiles)} tiles, rows {data_rows[0]}..{data_rows[1]}")
    del first
    jobs = [(run, g["name"], s, si, len(steps), work) for si, s in enumerate(steps) if si for g in PF.GRIDS]
    for n, stat in enumerate(_results(executor, _step_task, jobs), 1):
        stats.append(stat)
        if n % len(PF.GRIDS) == 0:
            log(f"{n + len(PF.GRIDS)}/{len(jobs) + len(PF.GRIDS)} files decoded (last: {stat['grid']} f{stat['step']:03d})")
    return layouts, stats


def _results(executor, fn, jobs):
    """Each job's result as it FINISHES; the first failure is raised at once and the jobs not yet started are
    dropped (a file that is not there is then seen after seconds, not after everything queued before it)."""
    if not hasattr(executor, "submit"):
        yield from executor.map(fn, jobs)
        return
    futures = [executor.submit(fn, job) for job in jobs]
    try:
        for f in as_completed(futures):
            yield f.result()
    finally:
        for f in futures:
            f.cancel()


def drift_summary(layouts, step_stats):
    """How far the steps' sea cells are from the first step's. Raises past DRIFT_FAIL of a grid's cells at any
    step: that is a broken file or a mask that moves within a run, and the product's rule no longer holds."""
    out = {"rule": "the cells with a wave height at the first step; a later step without one is stored as missing",
           "grid_steps": 0, "lost_max": 0, "extra_max": 0}
    for s in step_stats:
        n = s["hs_lost"] + s["hs_extra"]
        if not n:
            continue
        out["grid_steps"] += 1
        out["lost_max"], out["extra_max"] = max(out["lost_max"], s["hs_lost"]), max(out["extra_max"], s["hs_extra"])
        cells = layouts[s["grid"]][1].size
        if n > DRIFT_FAIL * cells:
            raise ValueError(f"{s['grid']} f{s['step']:03d}: {s['hs_lost']} sea cells without waves and {s['hs_extra']} "
                             f"extra, of {cells}: the sea cells are not the first step's any more")
    return out


# ------------------------------- R2 side ---------------------------------------------

def tile_key(run, gname, tr, tc):
    return f"{PREFIX}/{run}/{gname}/{tr}_{tc}.bin"


def mask_key(run, gname):
    return f"{PREFIX}/{run}/{gname}/mask.bin"


def files_template(run):
    return f"{PREFIX}/{run}/{{grid}}/{{tr}}_{{tc}}.bin"


def _stamp(published_utc):
    return published_utc.replace("-", "").replace(":", "")


def manifest_key(run, published_utc, complete=True):
    return f"{PREFIX}/{run}/{'manifest' if complete else 'partial'}-{_stamp(published_utc)}.json"


def stats_key(run, published_utc):
    return f"{PREFIX}/{run}/stats-{_stamp(published_utc)}.json"


def failed_key(run):
    return f"{PREFIX}/failed/{run}.json"


def notready_key(run):
    return f"{PREFIX}/notready/{run}.json"


class Uploads:
    """Uploads that run behind the build. The first failure is raised from drain() (the build stops); at most
    `backlog` trail it. Each future is asked done() ONCE and then either checked or kept: asking twice would drop a
    failure that completed between the two questions, and the pointer would flip over a hole (run.py, G12 P0-1)."""

    def __init__(self, store, workers=UPLOAD_WORKERS, backlog=UPLOAD_BACKLOG):
        self.store, self.backlog, self.pending = store, backlog, []
        self.pool = ThreadPoolExecutor(workers) if store is not None else None
        self.count = self.bytes = 0

    def put(self, key, body):
        self.count += 1
        self.bytes += len(body)
        if self.pool is not None:
            self.pending.append(self.pool.submit(self.store.put, key, body, BINARY, P.IMMUTABLE))
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


def publish_grid(run, grid, mask, cells, tiles, work, nsteps, uploads, compress_pool, log=print):
    """Cut one grid's tiles from its scratch file (a tile row at a time), compress and hand them to `uploads`.
    -> {"tr_tc": bytes} for the stats."""
    gname, t = grid["name"], grid["tile"]
    mm = np.memmap(_paths(work, gname)[1], dtype="<u2", mode="r", shape=(nsteps, len(PF.FIELDS), cells.size))
    sizes = {}

    def build(row, band, a):
        tr, tc, start, count = (int(x) for x in row)
        bitmap = PF.tile_bitmap(mask, t, tr, tc)
        header = PF.tile_header(run, grid, tr, tc, bitmap.shape[0], bitmap.shape[1], count, nsteps)
        planes = np.ascontiguousarray(band[:, :, start - a:start - a + count].transpose(1, 0, 2))
        return tr, tc, PF.encode_tile(header, bitmap, planes)

    try:
        for tr in np.unique(tiles[:, 0]):
            rows = tiles[tiles[:, 0] == tr]
            a, b = int(rows[0, 2]), int(rows[-1, 2] + rows[-1, 3])
            band = np.array(mm[:, :, a:b])                          # [step, field, cell] of this tile row
            for tr_, tc, blob in compress_pool.map(lambda r, band=band, a=a: build(r, band, a), rows):
                sizes[f"{tr_}_{tc}"] = len(blob)
                uploads.put(tile_key(run, gname, tr_, tc), blob)
        uploads.put(mask_key(run, gname), PF.encode_mask(run, grid, mask))
    finally:
        del mm
    log(f"{gname}: {len(sizes)} tiles, {sum(sizes.values()) / 1e6:.1f} MB")
    return sizes


def build_and_publish(store, run_dt, steps, work, executor, upload=True, log=print, force=False):
    run = P.run_key(run_dt)
    t0 = time.time()
    layouts, step_stats = decode_run(run_dt, steps, work, executor, log)
    drift = drift_summary(layouts, step_stats)
    t1 = time.time()
    uploads = Uploads(store if upload else None)
    compress_pool = ThreadPoolExecutor(COMPRESS_WORKERS)
    grids, tile_sizes = [], {}
    try:
        for grid in PF.GRIDS:
            mask, cells, tiles, data_rows = layouts[grid["name"]]
            sizes = publish_grid(run, grid, mask, cells, tiles, work, len(steps), uploads, compress_pool, log)
            tile_sizes[grid["name"]] = sizes
            r0, r1 = grid["rows"]
            grids.append({"name": grid["name"], "source": f"gfswave {grid['tag']}", "ni": grid["ni"], "nj": grid["nj"],
                          "lat0": grid["lat0"], "lon0": 0.0, "per_deg": grid["per_deg"], "rows": [r0, r1],
                          # the rows that hold sea cells in this run, and their latitudes (not the file's bounds)
                          "data_rows": list(data_rows),
                          "lat_north": PF.grid_lat(grid, data_rows[0]), "lat_south": PF.grid_lat(grid, data_rows[1]),
                          "lon_periodic": True, "registration": "center", "tile": grid["tile"],
                          "sea_cells": int(cells.size), "tiles": len(sizes), "bytes": int(sum(sizes.values())),
                          "mask": mask_key(run, grid["name"])})
        uploads.drain(0)                                            # every tile and mask is stored before the manifest
    except BaseException:
        uploads.close()
        raise
    finally:
        compress_pool.shutdown(wait=False, cancel_futures=True)
    uploads.close()
    complete = steps == F.STEPS
    manifest = {
        "schema": 1, "format": PF.FORMAT, "format_key": format_key(), "run": run,
        "run_utc": run_dt.strftime("%Y-%m-%dT%H:%M:%SZ"), "model": MODEL,
        "steps": list(steps), "step_schedule": F.STEP_SCHEDULE, "expected_steps": len(F.STEPS),
        "missing": PF.MISSING, "dtype": "<u2", "order": "field,step,cell", "codec": "xz",
        "bitmap": "row-major, 8 cells to a byte, the first cell in the most significant bit",
        "nearest": "a point takes the nearest sea cell of any grid; on a tie, the earlier grid in `grids`",
        "sea_cells": drift,
        "fields": [{"name": n, "kind": k, "grib": noaa, "scale": PF.KINDS[k]["scale"], "units": PF.KINDS[k]["units"]}
                   for n, k, noaa, _ident in PF.FIELDS],
        "grids": grids, "files": {"template": files_template(run)}, "digest": run_digest(step_stats),
        "complete": complete, "published_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "decode_seconds": round(t1 - t0, 1), "build_seconds": round(time.time() - t0, 1),
    }
    stats = {"run": run, "steps": step_stats, "tiles": tile_sizes}
    pointed = False
    if upload:
        _mkey, pointed = publish_manifest(store, run, manifest, stats, force=force)
    manifest["_pointed"] = pointed                                  # in memory only, like the two below
    manifest["_stats"] = stats
    manifest["_objects"] = {"count": uploads.count, "bytes": uploads.bytes}
    return manifest


def _json(obj):
    return json.dumps(obj, separators=(",", ":"), sort_keys=True, allow_nan=False).encode()


def publish_manifest(store, run, manifest, stats, force=False):
    """The stats sidecar, then the manifest under a fresh immutable key; the pointer ONLY for a complete run, and
    (without --force) only if no NEWER run went live while this one was building. -> (manifest key, pointed)."""
    skey = stats_key(run, manifest["published_utc"])
    store.put(skey, _json(stats), "application/json", P.IMMUTABLE)
    manifest = dict(manifest, stats=skey)
    mkey = manifest_key(run, manifest["published_utc"], complete=manifest["complete"])
    store.put(mkey, _json(manifest), "application/json", P.IMMUTABLE)
    if not manifest["complete"]:
        return mkey, False
    live = (store.get_json(LATEST_KEY) or {}).get("run")
    if live and live > run and not force:
        _summary(f"WARNING: run {live} went live while {run} was building; the pointer stays on {live}")
        return mkey, False
    point_to(store, run, mkey, manifest)
    return mkey, True


def point_to(store, run, mkey, manifest):
    if not manifest.get("complete"):
        raise ValueError("the pointer only ever names a complete run")
    pointer = {"run": run, "manifest": mkey, "complete": True, "format": manifest["format"],
               "format_key": manifest["format_key"], "published_utc": manifest["published_utc"],
               "steps": len(manifest["steps"])}
    store.put(LATEST_KEY, json.dumps(pointer, separators=(",", ":"), sort_keys=True).encode(), "application/json", P.POINTER)
    return pointer


def newest_manifest(store, run):
    """(key, parsed JSON) of the newest COMPLETE manifest of `run`, or (None, None). An unreadable one raises."""
    keys = sorted(store.list_keys(f"{PREFIX}/{run}/manifest-"))
    if not keys:
        return None, None
    body = store.get_json(keys[-1])
    if not isinstance(body, dict):
        raise ValueError(f"{keys[-1]} is not a manifest object")
    return keys[-1], body


def list_runs(store):
    """{run: has a complete manifest} for every run prefix under PREFIX."""
    runs = {}
    for k in store.list_keys(PREFIX + "/"):
        rest = k[len(PREFIX) + 1:]
        name = rest.split("/", 1)[0]
        if name.isdigit() and len(name) == 10:
            runs.setdefault(name, False)
            if rest.startswith(f"{name}/manifest-"):
                runs[name] = True
    return runs


def prune(store, keep=2):
    """Keep the newest `keep` COMPLETE runs (plus whatever the pointer names); delete every other run prefix,
    except manifest-less ones no older than the newest complete run (a build that may still be uploading)."""
    if keep < 1:
        raise ValueError("keep must be >= 1")
    latest = store.get_json(LATEST_KEY) or {}
    runs = list_runs(store)
    complete = sorted(r for r, ok in runs.items() if ok)
    protect = set(complete[-keep:]) | ({latest["run"]} if latest.get("run") else set())
    newest = complete[-1] if complete else None
    deleted = []
    for run in sorted(runs):
        if run in protect or (not runs[run] and newest and run >= newest):
            continue
        store.delete_prefix(f"{PREFIX}/{run}/")
        deleted.append(run)
    return deleted


def _record(store, key, run, counter, stamp, message):
    prev = store.get_json(key) or {"run": run, counter: 0}
    prev[counter] += 1
    prev[stamp] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    prev["last_error"] = P.redact(message, store.bucket)
    store.put(key, json.dumps(prev, sort_keys=True).encode(), "application/json", P.POINTER)
    return prev


def record_failure(store, run, message):
    return _record(store, failed_key(run), run, "attempts", "last_attempt_utc", message)


def record_notready(store, run, message):
    return _record(store, notready_key(run), run, "count", "last_utc", message)


def _skip_failed(store, run):
    rec = store.get_json(failed_key(run))
    if not rec:
        return False
    if rec.get("attempts", 0) >= FAILED_MAX_ATTEMPTS:
        return True
    last = datetime.strptime(rec["last_attempt_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - last).total_seconds() < FAILED_RETRY_AFTER_S


def existing_guard(store, run, live, force):
    """None to go ahead and build, else the exit code. A run that already has a complete manifest is not rebuilt
    on a normal tick: if the pointer does not name it (the pointer write failed, or latest.json was lost) and it
    is newer than the live run, the pointer is repaired to that manifest. One published in another format is
    never rewritten, not even with --force: its keys are immutable (a format change takes a new PREFIX version)."""
    try:
        mkey, man = newest_manifest(store, run)
    except ValueError as exc:
        msg = f"run {run}: its published manifest is not a valid manifest ({exc.__class__.__name__}); not rewriting its tiles"
        _summary("WARNING: " + msg)
        return 2
    except Exception as exc:                                        # noqa: BLE001  transport: the next tick retries
        print(f"run {run}: could not read its published manifest ({exc.__class__.__name__}); retrying next tick")
        return 1
    if mkey is None:
        return None
    if man.get("format_key") != format_key():
        msg = f"run {run} was published in format {man.get('format')}/{man.get('format_key')}; refusing to rewrite its tiles as {PF.FORMAT}/{format_key()}"
        _summary("WARNING: " + msg)
        return 2
    if force:
        return None
    usable = (man.get("run") == run and man.get("complete") is True and isinstance(man.get("steps"), list)
              and len(man["steps"]) == len(F.STEPS) and isinstance(man.get("published_utc"), str))
    if usable and (not live or run > live):
        point_to(store, run, mkey, man)
        msg = f"run {run} already has a complete manifest; pointer repaired to {mkey}"
        _summary(msg)
        return 0
    print(f"run {run} already has a manifest that cannot be pointed to; rebuilding")
    return None


class LocalClient:
    """A directory with the calls publish.Store makes (for --local: development and inspection)."""

    def __init__(self, root):
        self.root = os.path.abspath(root)

    def _path(self, key):
        path = os.path.abspath(os.path.join(self.root, *key.split("/")))
        if os.path.commonpath([path, self.root]) != self.root:
            raise ValueError(key)
        return path

    def put_object(self, Bucket, Key, Body, ContentType, CacheControl):
        path = self._path(Key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path + ".part", "wb") as fh:
            fh.write(Body)
        os.replace(path + ".part", path)

    def get_object(self, Bucket, Key):
        try:
            with open(self._path(Key), "rb") as fh:
                return {"Body": io.BytesIO(fh.read())}
        except FileNotFoundError:
            raise KeyError(Key) from None

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None, **kw):
        keys = []
        for base, _dirs, files in os.walk(self.root):
            for name in files:
                key = os.path.relpath(os.path.join(base, name), self.root).replace(os.sep, "/")
                if key.startswith(Prefix) and not key.endswith(".part"):
                    keys.append(key)
        return {"Contents": [{"Key": k} for k in sorted(keys)], "IsTruncated": False}

    def delete_objects(self, Bucket, Delete):
        for o in Delete["Objects"]:
            try:
                os.remove(self._path(o["Key"]))
            except FileNotFoundError:
                pass
        return {}


def _store_from_env():
    return P.Store(P.r2_client(os.environ["R2_ACCOUNT_ID"], os.environ["R2_ACCESS_KEY_ID"],
                               os.environ["R2_SECRET_ACCESS_KEY"]), os.environ["R2_BUCKET"])


def _executor(workers):
    if workers <= 0:
        return Inline()
    import multiprocessing
    return ProcessPoolExecutor(workers, mp_context=multiprocessing.get_context("spawn"))


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--local", default=None)
    ap.add_argument("--steps", default=None, help=f"comma list of forecast hours, default all {len(F.STEPS)}")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--keep", type=int, default=2)
    ap.add_argument("--workers", type=int, default=WORKERS)
    ap.add_argument("--work", default=None)
    ap.add_argument("--run", default=None)
    a = ap.parse_args(argv)
    if a.keep < 1:
        print("--keep must be >= 1")
        return 2
    if a.dry_run and a.local:
        print("--dry-run and --local are two different things; give one")
        return 2
    try:
        steps = [int(s) for s in a.steps.split(",")] if a.steps else F.STEPS
    except ValueError:
        steps = []
    if not steps or any(s not in F.STEPS for s in steps) or steps != sorted(set(steps)):
        print(f"--steps must be increasing forecast hours of the model output: hourly {F.STEP_SCHEDULE[0][0]}..{F.STEP_SCHEDULE[0][1]}, "
              f"then every {F.STEP_SCHEDULE[1][2]} h to {F.STEP_SCHEDULE[1][1]}")
        return 2
    partial = steps != F.STEPS
    if partial and not (a.dry_run or a.local):
        print("a subset of steps is never published to the bucket; add --dry-run or --local DIR")
        return 2

    if a.run is not None:
        try:
            run_dt = datetime.strptime(a.run, "%Y%m%d%H").replace(tzinfo=timezone.utc)
        except ValueError:
            run_dt = None
        if run_dt is None or len(a.run) != 10 or run_dt.hour % 6 or not (a.dry_run or a.local):
            print("--run takes a cycle as YYYYMMDDHH (00, 06, 12 or 18 UTC) and only goes with --dry-run or --local DIR")
            return 2
    else:
        try:
            run_dt = latest_complete_run()
        except F.TransportError as exc:
            print(f"NOAA listing failed: {exc}")
            return 1
        if run_dt is None:
            print("no complete run on S3 in the last 48 h")
            return 3
    run = P.run_key(run_dt)

    store = None
    if not a.dry_run:
        store = P.Store(LocalClient(a.local), "local") if a.local else _store_from_env()
        latest = store.get_json(LATEST_KEY) or {}
        live = latest.get("run")
        if live == run and latest.get("complete") and not a.force and not partial:
            print(f"run {run} already live; nothing to do")
            return 0
        if live and run < live and not a.force:
            print(f"newer run {live} is live; not regressing to {run}")
            return 0
        if not partial:                                             # before the back-off: a run whose tiles and manifest
            rc = existing_guard(store, run, live, a.force)          # are stored needs one PUT, not a 3 h wait
            if rc is not None:
                return rc
        if not partial and not a.force and _skip_failed(store, run):
            print(f"run {run} failed recently; waiting before retrying")
            return 0
    print(f"building run {run} ({len(steps)} steps){' [dry-run]' if a.dry_run else ''}{' [partial]' if partial else ''}")

    work = a.work or tempfile.mkdtemp(prefix="points-", dir=os.environ.get("RUNNER_TEMP") or None)
    os.makedirs(work, exist_ok=True)
    executor = _executor(a.workers)
    try:
        manifest = build_and_publish(store, run_dt, steps, work, executor, upload=not a.dry_run, force=a.force)
    except F.NotReady as exc:
        print(f"object vanished/not ready mid-run: {exc}")
        if store is not None and not partial:
            rec = record_notready(store, run, exc)
            print(f"not-ready count for {run}: {rec['count']}")
            if rec["count"] >= NOTREADY_WARN:
                _summary(f"WARNING: points run {run} not ready {rec['count']} times in a row: {exc}")
        return 3
    except Exception as exc:                                        # noqa: BLE001
        if store is not None and not partial:
            rec = record_failure(store, run, exc)
            print(f"build failed (attempt {rec['attempts']}): {exc!r}")
        raise
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
        if not a.work:
            shutil.rmtree(work, ignore_errors=True)
    print(json.dumps({k: manifest[k] for k in ("run", "complete", "decode_seconds", "build_seconds")}))
    if store is not None and manifest["complete"]:
        print("pruned:", prune(store, keep=a.keep))
    published = datetime.strptime(manifest["published_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    lag_h = (published - run_dt).total_seconds() / 3600
    obj = manifest["_objects"]
    _summary(f"### points run {run}: {len(manifest['steps'])} steps, complete={manifest['complete']}, "
             f"{obj['count']} objects, {obj['bytes'] / 1e6:.1f} MB, decode {manifest['decode_seconds']} s, "
             f"total {manifest['build_seconds']} s, published {lag_h:.1f} h after the cycle, digest {manifest['digest']}")
    _summary("grids: " + ", ".join(f"{g['name']} {g['sea_cells']} sea cells in {g['tiles']} tiles ({g['bytes'] / 1e6:.0f} MB)" for g in manifest["grids"]))
    if store is not None and manifest["complete"] and not manifest["_pointed"]:
        _summary(f"note: run {run} is stored but latest.json does not name it")
    st = manifest["_stats"]["steps"]
    drift = [s for s in st if s["hs_lost"] or s["hs_extra"]]
    if drift:
        worst = max(drift, key=lambda s: s["hs_lost"] + s["hs_extra"])
        _summary(f"WARNING: the sea cells differ from the first step's at {len(drift)} grid-steps "
                 f"(worst {worst['grid']} f{worst['step']:03d}: {worst['hs_lost']} without a height, {worst['hs_extra']} extra)")
    ragged = sum(s["ragged"] for s in st)
    if ragged:
        _summary(f"note: {ragged} cell-steps have a partition with only some of its three values")
    return 0


if __name__ == "__main__":
    sys.exit(main())
