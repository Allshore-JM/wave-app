"""GFS-Wave station bulletins from NOAA's run archive on AWS (plan section 53 A).

From the 2026-10-22 18Z run on, NOAA's AWS copy (noaa-gfs-bdp-pds) keeps only run archives under wave/station/; the
per-station .bull files answer 404 there. The bulletin archive, gfswave.tHHz.bull_tar, is a POSIX tar of 918 members
`./gfswave.<id>.bull`, each preceded by a small pax header (atime and ctime only) and its own 512-byte ustar header.
The layout is the same in every run seen (member order, names, sizes: 52,063 B each), so a committed index
(gfs_bull_index.json, built by tools/build_bull_index.py) serves every run, and one bulletin is ONE byte-range read of
[offset - 512, offset + size): the member's own ustar header and its text. The header proves the layout still holds
(name, size, checksum); the text proves it is the station's bulletin of that run (Location, Cycle, rows).

This module does no network itself: the caller passes `get_range(url, start, end) -> bytes`. Nothing at import but
reading the index file (a damaged or missing file = an empty index: the site then reads NOMADS, and says so).
"""
import json
import os
import re
import threading
import time

INDEX_FORMAT = "bull-index-v1"
INDEX_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gfs_bull_index.json")
BUCKET = "https://noaa-gfs-bdp-pds.s3.amazonaws.com"
HEADER = 512
MIN_ROWS = 100                                   # a GFS bulletin has 385; a cut or foreign text has far fewer
MAX_SIZE = 256 * 1024
NAME_RE = re.compile(r"\./gfswave\.([A-Za-z0-9_-]{1,32})\.bull")
ROW_RE = re.compile(r"^\s*\|\s*\d{1,2}\s+\d{1,2}\s*\|", re.M)
CYCLE_RE = re.compile(r"Cycle\s*:\s*(\d{8})\s+(\d{1,2})\s*UTC")
LOC_RE = re.compile(r"Location\s*:\s*(\S+)")

REASONS = ("ok", "unknown", "missing", "layout", "content", "transport")


def archive_url(date_str: str, run_str: str, base: str = BUCKET) -> str:
    return f"{base}/gfs.{date_str}/{run_str}/wave/station/gfswave.t{run_str}z.bull_tar"


def parse_index(doc):
    """{sid: (offset, size)} from an index document, or ValueError. `archive_bytes` and `stride` are kept as given."""
    if not isinstance(doc, dict) or doc.get("format") != INDEX_FORMAT:
        raise ValueError("not a bull-index-v1 document")
    members = doc.get("members")
    total = doc.get("archive_bytes")
    if not isinstance(members, list) or not members or not isinstance(total, int) or total <= 0:
        raise ValueError("index without members or archive size")
    out, end = {}, 0
    for m in members:
        if (not isinstance(m, list) or len(m) != 3 or not isinstance(m[0], str)
                or not all(isinstance(v, int) and not isinstance(v, bool) for v in m[1:])):
            raise ValueError(f"bad member {m!r}")
        sid, off, size = m
        if not NAME_RE.fullmatch(f"./gfswave.{sid}.bull") or sid in out:
            raise ValueError(f"bad or repeated station {sid!r}")
        if off < end + HEADER or off % HEADER or not 0 < size <= MAX_SIZE or off + size > total:
            raise ValueError(f"member {sid} out of order or out of the archive")
        out[sid] = (off, size)
        end = off + size
    return out


def load_index(path: str = INDEX_PATH):
    """(index, archive_bytes, error): an empty index and the error text when the file is missing or damaged."""
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
        return parse_index(doc), doc["archive_bytes"], None
    except Exception as exc:                                     # the site carries on from NOMADS
        return {}, None, f"{type(exc).__name__}: {exc}"


INDEX, ARCHIVE_BYTES, INDEX_ERROR = load_index()


def _octal(field: bytes) -> int:
    text = field.split(b"\0")[0].strip()
    if not text or any(c not in b"01234567" for c in text):
        raise ValueError("not an octal field")
    return int(text, 8)


def check_header(block: bytes, sid: str, size: int):
    """None when `block` is the ustar header of ./gfswave.<sid>.bull of `size` bytes, else what differs."""
    if len(block) != HEADER:
        return "short header"
    if block[257:263] not in (b"ustar\x00", b"ustar "):
        return "no ustar magic"
    if block[156:157] not in (b"0", b"\x00"):
        return "not a regular file"
    name = block[:100].split(b"\0")[0]
    if block[345:500].split(b"\0")[0]:                           # a ustar prefix: the name would be longer
        return "name with a prefix"
    if name != f"./gfswave.{sid}.bull".encode():
        return f"another member ({name[:40]!r})"
    try:
        if _octal(block[124:136]) != size:
            return "another size"
        want = _octal(block[148:156])
    except ValueError:
        return "bad number field"
    have = sum(block[:148]) + 8 * 32 + sum(block[156:])         # the checksum field counts as spaces
    if have != want:
        return "bad checksum"
    return None


def check_text(text: str, sid: str, date_str: str, run_str: str, min_rows: int = MIN_ROWS):
    """None when `text` is station `sid`'s bulletin of that run, else what is wrong (the GLWU check_bulletin rule).
    NOMADS's per-station copies are checked with min_rows=0: a short body there is a short read, caught by the fetch,
    and the parser also reads the older "Hr" layout, whose rows have no bars."""
    loc = LOC_RE.search(text)
    if not loc or loc.group(1) != sid:
        return "another station's text"
    cyc = CYCLE_RE.search(text)
    if not cyc or cyc.group(1) != date_str or int(cyc.group(2)) != int(run_str):
        return "another run's text"
    if min_rows and len(ROW_RE.findall(text)) < min_rows:
        return "too few rows"
    return None


class Stats:
    """What the archive reads did, for /healthz and the log. Its own lock (re-created after a fork by the site).
    `counts` are member reads by outcome; the run probe's view of the archive's size is kept apart (G36 A P3-1):
    `layout_suspect` is the last probe's verdict (None when the archive had the index's size), `probe_layouts` how
    many probes saw another size, `last_layout_read` the last member refused by its header, with its UTC time."""

    def __init__(self):
        self.lock = threading.Lock()
        self.reset()

    def reset(self):
        self.counts = {r: 0 for r in REASONS}
        self.nomads = 0
        self.last_error = None
        self.last_run = None
        self.layout_suspect = None
        self.probe_layouts = 0
        self.last_layout_read = None

    def note(self, reason, detail=None, run=None):
        with self.lock:
            self.counts[reason] = self.counts.get(reason, 0) + 1
            if reason == "ok":
                self.last_run = run
            elif detail and reason != "unknown":                 # an id typed into the address is no error of ours
                self.last_error = f"{reason}: {detail}"
            if reason == "layout":
                self.last_layout_read = f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {detail}"

    def probe_layout(self, detail):
        """The run probe saw the archive: `detail` names another size than the index's, None = the index's size."""
        with self.lock:
            self.layout_suspect = detail
            if detail:
                self.probe_layouts += 1

    def nomads_used(self):
        with self.lock:
            self.nomads += 1

    def snapshot(self):
        with self.lock:
            return {"reads": dict(self.counts), "nomads_fallbacks": self.nomads, "last_error": self.last_error,
                    "last_run": self.last_run, "layout_suspect": self.layout_suspect,
                    "probe_layouts": self.probe_layouts, "last_layout_read": self.last_layout_read}


STATS = Stats()


def archive_bulletin(date_str, run_str, sid, get_range, index=None, stats=None, base=BUCKET):
    """(text, reason, detail): the bulletin from the run's archive, or (None, reason, detail) with reason one of
    unknown / missing / layout / content / transport. `get_range(url, start, end)` returns exactly the bytes
    start..end inclusive or raises; an exception whose `status` is 404 (or 403, which S3 answers for a missing key
    when the bucket does not list publicly; NOAA's answers 404) means the archive is not there: counted as `missing`.
    The caller asks NOMADS after `missing` and `transport` alike, so that is a counter only; the run probe takes an
    archive 403 as a refusal on purpose (app._detect_latest_run, fresh check F4)."""
    index = INDEX if index is None else index
    stats = STATS if stats is None else stats
    where = index.get(sid)
    if where is None:
        stats.note("unknown", sid)
        return None, "unknown", sid
    off, size = where
    try:
        body = get_range(archive_url(date_str, run_str, base), off - HEADER, off + size - 1)
    except Exception as exc:
        status = getattr(exc, "status", None)
        reason = "missing" if status in (403, 404) else "transport"
        detail = f"{date_str} {run_str}Z {sid}: {exc}"
        stats.note(reason, detail)
        return None, reason, detail
    if len(body) != HEADER + size:
        detail = f"{sid}: {len(body)} bytes, expected {HEADER + size}"
        stats.note("transport", detail)
        return None, "transport", detail
    bad = check_header(body[:HEADER], sid, size)
    if bad:
        detail = f"{date_str} {run_str}Z {sid}: {bad}"
        stats.note("layout", detail)
        return None, "layout", detail
    text = body[HEADER:].decode("ascii", errors="replace")
    bad = check_text(text, sid, date_str, run_str)
    if bad:
        detail = f"{date_str} {run_str}Z {sid}: {bad}"
        stats.note("content", detail)
        return None, "content", detail
    stats.note("ok", run=f"{date_str}{run_str}")
    return text, "ok", None
