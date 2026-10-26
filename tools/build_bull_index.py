"""Build or check gfs_bull_index.json, the layout of NOAA's GFS-Wave bulletin archive (plan section 53 A).

NOAA's AWS copy keeps one archive of the run's station bulletins (gfs.YYYYMMDD/HH/wave/station/gfswave.tHHz.bull_tar,
~49 MB, 918 members). Its layout has been the same in every run seen, so the site reads a bulletin with one byte-range
request at the offset this index gives, and checks the member's own ustar header on the way (gfs_bulletins.py).

  python tools/build_bull_index.py 2026102512            # stream that run's archive, write gfs_bull_index.json
  python tools/build_bull_index.py --check 2026102506 2026102500 ...
                                                         # read six sampled members of each run by range: same layout?
When NOAA changes the archive (the site's /healthz then shows gfs.bulletins.layout_suspect, or members refused by
their header under reads.layout / last_layout_read, served from NOMADS's copies), build the index from a new run, check
it against a few others, run the tests and commit the file.

The build streams (tarfile mode "r|": no seeks, nothing kept but the index). It refuses members that are not regular
files named ./gfswave.<id>.bull of 1 B to 256 KB, and pax headers carrying anything but times (a path or a size in a
pax header would move the member away from its ustar header).
"""
import argparse
import json
import os
import sys
import tarfile
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import gfs_bulletins as GB  # noqa: E402

UA = {"User-Agent": "allshoresurf.com bulletin index (https://allshoresurf.com)"}
PAX_OK = {"atime", "ctime", "mtime"}


def index_from_tar(fileobj, run, total=None):
    """The index document of one archive stream (mode "r|"), or ValueError. `total` = the archive's length (default:
    the bytes read so far)."""
    members = []
    tf = tarfile.open(fileobj=fileobj, mode="r|")
    for m in tf:
        extra = set(m.pax_headers) - PAX_OK
        if extra:
            raise ValueError(f"{m.name}: pax header carries {sorted(extra)}")
        if not m.isreg():
            raise ValueError(f"{m.name}: not a regular file")
        hit = GB.NAME_RE.fullmatch(m.name)
        if not hit:
            raise ValueError(f"{m.name}: not ./gfswave.<id>.bull")
        if not 0 < m.size <= GB.MAX_SIZE:
            raise ValueError(f"{m.name}: size {m.size}")
        members.append([hit.group(1), m.offset_data, m.size])
    tf.close()
    if total is None:
        total = getattr(fileobj, "bytes_read", None) or fileobj.tell()
    if not members:
        raise ValueError("an empty archive")
    sizes = sorted({s for _, _, s in members})
    strides = sorted({b[1] - a[1] for a, b in zip(members, members[1:])})
    doc = {"format": GB.INDEX_FORMAT, "run": run, "archive_bytes": total, "member_size": sizes[0] if len(sizes) == 1
           else None, "stride": strides[0] if len(strides) == 1 else None, "members": members}
    GB.parse_index(doc)                                          # the site's own rules
    return doc


class _Counting:
    """A read-through file that counts bytes (the archive's length, for the run probe's check)."""

    def __init__(self, raw):
        self.raw, self.bytes_read = raw, 0

    def read(self, n=-1):
        b = self.raw.read(n)
        self.bytes_read += len(b)
        return b


def _drain(src):
    while src.read(1 << 20):
        pass


def build(run, out):
    url = GB.archive_url(run[:8], run[8:])
    t0 = time.time()
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=180) as r:
        length = int(r.headers.get("Content-Length") or 0)
        src = _Counting(r)
        doc = index_from_tar(src, run)
        _drain(src)                                              # the end-of-archive blocks: the full length
        doc["archive_bytes"] = src.bytes_read
    if length and length != doc["archive_bytes"]:
        raise SystemExit(f"read {doc['archive_bytes']} bytes of {length}")
    GB.parse_index(doc)
    with open(out, "w", encoding="utf-8", newline="\n") as f:
        json.dump(doc, f, separators=(",", ":"))
        f.write("\n")
    print(f"{run}: {len(doc['members'])} members, archive {doc['archive_bytes']} B, member size {doc['member_size']}, "
          f"stride {doc['stride']}, {time.time() - t0:.1f} s -> {out}")


def _get_range(url, start, end):
    req = urllib.request.Request(url, headers=dict(UA, Range=f"bytes={start}-{end}"))
    with urllib.request.urlopen(req, timeout=30) as r:
        if r.status != 206:
            raise IOError(f"HTTP {r.status}, not a range answer")
        body = r.read()
    if len(body) != end - start + 1:
        raise IOError(f"{len(body)} bytes for {end - start + 1}")
    return body


def _head_length(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA, method="HEAD"), timeout=30) as r:
        return int(r.headers.get("Content-Length") or 0)


def check(runs, path, head_length=_head_length, get_range=None):
    index, total, err = GB.load_index(path)
    if err:
        raise SystemExit(f"{path}: {err}")
    get_range = get_range or _get_range
    sids = list(index)
    samples = [sids[i] for i in sorted({0, 1, 100, 400, 700, len(sids) - 1}) if i < len(sids)]
    bad = 0
    for run in runs:
        url = GB.archive_url(run[:8], run[8:])
        try:
            length = head_length(url)
        except (urllib.error.URLError, OSError) as exc:             # a run NOAA has not written (yet): one line (G36 A P3-5)
            print(f"{run}: not readable on AWS ({getattr(exc, 'code', None) or exc})")
            bad += 1
            continue
        ok = 0
        for sid in samples:
            off, size = index[sid]
            try:
                body = get_range(url, off - GB.HEADER, off + size - 1)
            except (urllib.error.URLError, OSError) as exc:
                print(f"  {run} {sid}: not readable ({getattr(exc, 'code', None) or exc})")
                continue
            problem = GB.check_header(body[:GB.HEADER], sid, size) or GB.check_text(
                body[GB.HEADER:].decode("ascii", "replace"), sid, run[:8], run[8:])
            if problem:
                print(f"  {run} {sid}: {problem}")
            else:
                ok += 1
        same = length == total
        bad += (ok != len(samples)) + (not same)
        print(f"{run}: {ok} of {len(samples)} sampled members as indexed; archive {length} B "
              f"({'= index' if same else 'DIFFERS from ' + str(total)})")
    if bad:
        raise SystemExit(f"{bad} problem(s): the index does not fit every run")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("runs", nargs="+", help="YYYYMMDDHH")
    ap.add_argument("--check", action="store_true", help="check the runs against the index instead of building")
    ap.add_argument("--out", default=GB.INDEX_PATH)
    a = ap.parse_args(argv)
    for run in a.runs:
        if len(run) != 10 or not run.isdigit() or run[8:] not in ("00", "06", "12", "18"):
            raise SystemExit(f"{run}: not YYYYMMDDHH of a GFS cycle")
    if a.check:
        check(a.runs, a.out)
    else:
        if len(a.runs) != 1:
            raise SystemExit("build from one run")
        build(a.runs[0], a.out)


if __name__ == "__main__":
    main()
