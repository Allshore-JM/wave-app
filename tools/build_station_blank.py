"""Find the forecast stations whose GFS-Wave bulletin is blank, and take them off the station list (plan section 52 C).

NOAA publishes a bulletin for every station it lists, also for stations inside sheltered water the 0.25-degree wave
model has no sea for (Long Island Sound, Chesapeake Bay, the Potomac at Alexandria: 44061, 44062 ...). Such a bulletin
has its 385 rows but no height in any of them, and the site showed a table of blank cells with only the wind column.
Owner (2026-10-24): they are removed from the list; an old link still opens, with a note.

The rule (the parser's own cells, app._parse_bull_text): a bulletin is BLANK when no data row of the whole run has a
combined height above zero (the Hst cell: NOAA writes a literal 0 there for sheltered points, and empty swell cells) or
a swell group with its three numbers. A missing bulletin (404) or a failed download is ABSENT and never counts.

A station that may be blank because of SEA ICE is never removed (owner, 2026-10-24): the model masks iced water, so an
Arctic or a frozen-sea point is blank in winter and carries waves again when the ice goes. Such a blank station is
recorded under "ice" instead of "blank" when it lies in a sea-ice region: poleward of ICE_LAT in either hemisphere, or
inside ICE_BOXES (the lower-latitude seas that freeze in winter).

This tool reads the bulletins of one run for the listed stations, the ones it removed before and the ones waiting to
come back (so every one of them is checked every time: G35 F1, re-check R1-1), records which were blank, and writes
station_blank.json:
  {"rule": ..., "runs": {"YYYYMMDDHH": {"blank": [...], "ice": [...], "absent": [...], "back": [...], "scanned": N},
   ...}, "removed": [...], "restore": [...]}
where "back" = the removed stations found WITH data in that run, "removed" = the ids blank in BOTH of the two newest
recorded runs, plus the ids removed before that are not back in both (one cycle's hiccup neither removes nor restores;
the runs are at least 6 h apart by their names; a missing bulletin never counts), and "restore" = the ids that left
"removed" and are not on the list yet: they wait there, scanned every run, whatever the order of the plain scans and
the --apply runs, and one that is blank in both newest runs again goes back to "removed". `--apply` then rewrites
station_list.json from the listed ids minus "removed" plus "restore" (each at its old place: station_coords.json's
order, the list's original order; named on the console) and empties "restore". The coordinates and time-zone tables
stay whole (the point time-zone rule reads every coordinate; section 32).

Run (network: the run's bulletin tar from NOAA's AWS copy (one ~50 MB file holding every station's bulletin), else the
per-station files: AWS first, NOMADS second, paced):
  python tools/build_station_blank.py                 # the newest complete run
  python tools/build_station_blank.py --run 2026102406
  python tools/build_station_blank.py --apply         # scan, then rewrite station_list.json from "removed"
"""
import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
LIST_PATH = os.path.join(ROOT, "station_list.json")
OUT_PATH = os.path.join(ROOT, "station_blank.json")
AWS = "https://noaa-gfs-bdp-pds.s3.amazonaws.com"
NOMADS = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"
PROBE_STATION = "51201"
KEEP_RUNS = 4                 # runs remembered in the file
NOMADS_PACE_S = 0.6           # NOMADS: 120 requests a minute per address
# sea-ice regions: |lat| >= ICE_LAT, or inside a box (south, north, west, east) of a lower-latitude sea that freezes
ICE_LAT = 50.0
ICE_BOXES = {
    "Bohai and the northern Yellow Sea": (37.0, 41.5, 117.0, 125.5),
    "Sea of Okhotsk, Tatar Strait, Hokkaido": (41.5, 50.0, 130.0, 160.0),
    "Gulf of St Lawrence, Newfoundland, Labrador": (43.0, 50.0, -70.0, -50.0),
    "Sea of Azov": (45.0, 47.5, 34.5, 39.5),
}
RULE = ("blank = no data row of the run has a combined height (Hst) above zero or a swell group with three numbers; "
        "a blank station in a sea-ice region (|lat| >= 50 or a freezing sea's box) is recorded as ice and never removed; "
        "removed = blank in both of the two newest recorded runs, or removed before and not back (with data) in both; "
        "restore = left removed, not on the list yet: scanned every run until --apply puts it back; "
        "a missing bulletin never counts")
ROW_RE = re.compile(r"^\s*\|\s*\d{1,2}\s+\d{1,2}\s*\|")
NUM_RE = re.compile(r"^-?\d+(\.\d+)?$")


def bull_path(run, station):
    """The bulletin's path under either host for run 'YYYYMMDDHH'."""
    return f"gfs.{run[:8]}/{run[8:]}/wave/station/bulls.t{run[8:]}z/gfswave.{station}.bull"


def classify(text):
    """'blank' | 'data' for a bulletin's text, by the parser's cells; None when the text has no data rows at all."""
    rows = 0
    for line in text.splitlines():
        if not ROW_RE.match(line):
            continue
        rows += 1
        parts = [p.strip() for p in line.strip().strip("|").split("|")]
        if len(parts) < 2:
            continue
        toks = [t.replace("*", "") for t in parts[1].split()]
        if toks and NUM_RE.match(toks[0]) and float(toks[0]) > 0:
            return "data"
        for cell in parts[2:]:
            t = [x.replace("*", "") for x in cell.split() if x.replace("*", "")]
            if len(t) >= 3 and all(NUM_RE.match(x) for x in t[:3]):
                return "data"
    return "blank" if rows else None


def ice_region(lat, lon):
    """True where winter sea ice can blank a GFS-Wave point (the model masks iced water)."""
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError):
        return False
    if abs(lat) >= ICE_LAT:
        return True
    lon = (lon + 180.0) % 360.0 - 180.0
    return any(s <= lat <= n and w <= lon <= e for s, n, w, e in ICE_BOXES.values())


def split_ice(result, coords):
    """Move the blank stations of a sea-ice region from result["blank"] to result["ice"] (kept on the list).
    coords: {id: {"lat", "lon"}}; a station without coordinates stays blank."""
    blank, ice = [], []
    for s in result.get("blank", []):
        c = coords.get(s) or {}
        (ice if ice_region(c.get("lat"), c.get("lon")) else blank).append(s)
    out = dict(result)
    out["blank"], out["ice"] = sorted(blank), sorted(set(result.get("ice", [])) | set(ice))
    return out


def removed_ids(runs, before=()):
    """The ids blank in both of the two newest recorded runs (by run name), plus the ids of `before` (removed earlier)
    that are not back (scanned with data) in both; with fewer than two runs only `before`."""
    names = sorted(runs)[-2:]
    if len(names) < 2:
        return sorted(set(before))
    a, b = (set(runs[n].get("blank", [])) for n in names)
    back = set(runs[names[0]].get("back", [])) & set(runs[names[1]].get("back", []))
    return sorted((a & b) | (set(before) - back))


def scan_ids(listed, before):
    """The ids one run's scan reads: the listed ones, then the removed ones not on the list."""
    have = set(listed)
    return list(listed) + [s for s in before if s not in have]


def came_back(result, before):
    """The removed ids found with data in a scan: read (not absent, not failed) and not blank."""
    seen = set(result.get("blank", [])) | set(result.get("absent", [])) | set(result.get("failed", []))
    return sorted(s for s in before if s not in seen)


def merge(doc, run, result, listed=()):
    """Record one run's scan into the file's document (the newest KEEP_RUNS kept) and recompute 'removed' (the ids
    removed before stay unless back in both of the two newest runs) and 'restore': the ids that left 'removed' and are
    not on the list yet. They wait there, scanned every run, until --apply puts them back (re-check R1-1: a comeback
    completed by a plain scan was on neither list and never scanned again); one that is blank in both newest runs again
    is removed again."""
    runs = dict(doc.get("runs") or {})
    runs[run] = result
    runs = {k: runs[k] for k in sorted(runs)[-KEEP_RUNS:]}
    before = set(doc.get("removed") or ())
    removed = removed_ids(runs, before)
    restore = (set(doc.get("restore") or ()) | (before - set(removed))) - set(removed) - set(listed)
    return {"rule": RULE, "runs": runs, "removed": removed, "restore": sorted(restore)}


def apply_removal(ids, removed, restore=(), order=()):
    """The station list without the removed ids, order kept; the ids of `restore` (back on the list) inserted at their
    place in `order` (station_coords.json's keys: the list's original order), else at the end."""
    gone = set(removed)
    out = [str(s) for s in ids if str(s) not in gone]
    pos = {s: i for i, s in enumerate(order)}
    for s in restore:
        if s in gone or s in out:
            continue
        p = pos.get(s)
        at = next((i for i, t in enumerate(out) if p is not None and pos.get(t, -1) > p), len(out))
        out.insert(at, s)
    return out


# ---- network (never under test: the functions above are the rule)
def _session():
    import requests
    s = requests.Session()
    s.headers["User-Agent"] = "allshoresurf.com station-blank scan (https://allshoresurf.com)"
    return s


def newest_run(http):
    now = datetime.now(timezone.utc)
    for d in (0, 1):
        day = (now - timedelta(days=d)).strftime("%Y%m%d")
        for hh in ("18", "12", "06", "00"):
            run = day + hh
            for base in (AWS, NOMADS):
                try:
                    r = http.head(f"{base}/{bull_path(run, PROBE_STATION)}", timeout=15)
                    if r.status_code == 200:
                        return run
                except Exception:
                    pass
                if base == NOMADS:
                    time.sleep(NOMADS_PACE_S)
    raise SystemExit("no GFS-Wave run found on either host")


def fetch(http, run, station, state):
    """(station, 'blank' | 'data' | 'absent' | 'failed', note). AWS first (no limit, in parallel), NOMADS second (paced)."""
    try:
        r = http.get(f"{AWS}/{bull_path(run, station)}", timeout=20)
        if r.status_code == 200 and r.text:
            c = classify(r.text)
            return station, c or "absent", "aws"
        aws = f"aws {r.status_code}"
    except Exception as exc:
        aws = f"aws {type(exc).__name__}"
    with state["lock"]:
        wait = state["next"] - time.time()
        if wait > 0:
            time.sleep(wait)
        state["next"] = time.time() + NOMADS_PACE_S
    try:
        r = http.get(f"{NOMADS}/{bull_path(run, station)}", timeout=25)
    except Exception as exc:
        return station, "failed", f"{aws}; nomads {type(exc).__name__}"
    if r.status_code == 200 and r.text:
        c = classify(r.text)
        return station, c or "absent", f"{aws}; nomads"
    if r.status_code == 404:
        return station, "absent", f"{aws}; nomads 404"
    return station, "failed", f"{aws}; nomads {r.status_code}"


def tar_scan(http, run, ids):
    """The scan from NOAA's AWS tar of the run's bulletins (gfswave.tHHz.bull_tar); None when the tar is not there."""
    import tarfile
    import tempfile
    url = f"{AWS}/gfs.{run[:8]}/{run[8:]}/wave/station/gfswave.t{run[8:]}z.bull_tar"
    try:
        r = http.get(url, timeout=(20, 300), stream=True)
    except Exception as exc:
        print(f"  tar: {type(exc).__name__}", file=sys.stderr)
        return None
    if r.status_code != 200:
        print(f"  tar: HTTP {r.status_code}", file=sys.stderr)
        return None
    with tempfile.NamedTemporaryFile(suffix=".tar", delete=False) as tmp:
        for chunk in r.iter_content(1 << 20):
            tmp.write(chunk)
        name = tmp.name
    out = {"blank": [], "absent": [], "failed": [], "scanned": 0, "scanned_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "source": "aws tar"}
    try:
        with tarfile.open(name) as tar:
            members = {os.path.basename(m.name): m for m in tar.getmembers() if m.isfile()}
            for station in ids:
                out["scanned"] += 1
                m = members.get(f"gfswave.{station}.bull")
                if m is None:
                    out["absent"].append(station)
                    continue
                text = tar.extractfile(m).read().decode("ascii", "replace")
                c = classify(text)
                if c == "blank":
                    out["blank"].append(station)
                elif c is None:
                    out["absent"].append(station)
    finally:
        os.unlink(name)
    for k in ("blank", "absent", "failed"):
        out[k].sort()
    return out


def scan(http, run, ids, workers=8):
    import threading
    state = {"lock": threading.Lock(), "next": 0.0}
    out = {"blank": [], "absent": [], "failed": [], "scanned": 0, "scanned_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for station, kind, note in pool.map(lambda s: fetch(http, run, s, state), ids):
            out["scanned"] += 1
            if kind in ("blank", "absent", "failed"):
                out[kind].append(station)
                if kind != "blank":
                    print(f"  {station}: {kind} ({note})", file=sys.stderr)
    for k in ("blank", "absent", "failed"):
        out[k].sort()
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--run", help="YYYYMMDDHH (default: the newest run on NOAA)")
    ap.add_argument("--apply", action="store_true", help="rewrite station_list.json without the removed ids")
    ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args(argv)
    with open(LIST_PATH, encoding="utf-8") as f:
        ids = [str(s) for s in json.load(f)]
    with open(os.path.join(ROOT, "station_coords.json"), encoding="utf-8") as f:
        coords = json.load(f)
    doc = {}
    if os.path.exists(OUT_PATH):
        with open(OUT_PATH, encoding="utf-8") as f:
            doc = json.load(f)
    before = [str(s) for s in doc.get("removed") or []]
    waiting = [str(s) for s in doc.get("restore") or [] if str(s) not in set(ids)]
    http = _session()
    run = a.run or newest_run(http)
    if not re.fullmatch(r"\d{10}", run):
        raise SystemExit("--run must be YYYYMMDDHH")
    todo = scan_ids(ids, before + waiting)
    print(f"scanning {len(todo)} stations of run {run} ({len(ids)} listed, {len(before)} removed, "
          f"{len(waiting)} waiting to come back)", file=sys.stderr)
    result = split_ice(tar_scan(http, run, todo) or scan(http, run, todo, a.workers), coords)
    result["back"] = came_back(result, before)
    doc = merge(doc, run, result, ids)
    print(f"run {run}: blank {len(result['blank'])}, ice (kept) {len(result['ice'])}, absent {len(result['absent'])}, failed {len(result['failed'])} "
          f"of {result['scanned']}; removed (two newest runs {sorted(doc['runs'])[-2:]}): {len(doc['removed'])}", file=sys.stderr)
    print(" ".join(result["blank"]), file=sys.stderr)
    if result["back"]:
        print("removed before, with data in this run: " + " ".join(result["back"]), file=sys.stderr)
    if a.apply:
        restore = doc["restore"]
        kept = apply_removal(ids, doc["removed"], restore, list(coords))
        if restore:
            print("back on the list: " + " ".join(restore), file=sys.stderr)
        with open(LIST_PATH, "w", encoding="utf-8") as f:
            json.dump(kept, f, indent=2)
            f.write("\n")
        print(f"station_list.json: {len(ids)} -> {len(kept)}", file=sys.stderr)
        doc["restore"] = sorted(set(restore) - set(kept))
    elif doc["restore"]:
        print("waiting to come back (run with --apply): " + " ".join(doc["restore"]), file=sys.stderr)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=1)
        f.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
