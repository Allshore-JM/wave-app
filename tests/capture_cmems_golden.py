"""Capture the CMEMS station-list golden from the UNCHANGED CopernicusProvider._fetch_stations.

Run ONCE on the pre-change commit with the real index downloaded to the path in RAW_INDEX:
    python tests/capture_cmems_golden.py <path-to-index_latest.txt>
Builds tests/fixtures/cmems_index_sample.txt (a deterministic slice of the real index that
exercises every branch of the row filter) and tests/fixtures/cmems_golden.json (the station
list + _file_by_id the old code produces from that slice under a frozen clock).
"""
import csv
import io
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import buoy_sources as B  # noqa: E402
import fake_buoy_providers as F  # noqa: E402

SAMPLE = os.path.join(HERE, "fixtures", "cmems_index_sample.txt")
GOLDEN = os.path.join(HERE, "fixtures", "cmems_golden.json")
# Frozen "now" for LIVE_MAX_AGE: 2026-09-10T17:00:00Z (index captured 2026-09-10T16:32Z).
NOW_EPOCH = 1_789_059_600.0


class _Resp:
    def __init__(self, text):
        self.text = text
        self.content = text.encode("utf-8")
        self.status_code = 200

    # streaming interface used by the new code (byte chunks / decoded lines)
    def iter_lines(self, decode_unicode=False, chunk_size=None):
        for line in self.text.splitlines():
            yield line if decode_unicode else line.encode("utf-8")

    def iter_content(self, chunk_size=65536):
        b = self.content
        for i in range(0, len(b), chunk_size):
            yield b[i:i + chunk_size]

    def raise_for_status(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


class FakeHTTP:
    def __init__(self, text):
        self.text = text
        self.calls = []

    def get(self, url, **kw):
        self.calls.append((url, kw))
        return _Resp(self.text)


def build_sample(raw_path):
    """Slice the real index: keep header comments, then rows chosen so the fixture contains
    in-bbox wave platforms (with the duplicate-date rows the index naturally carries), rows
    outside the bbox, rows without wave parameters, stale rows, and a few malformed rows."""
    with open(raw_path, encoding="utf-8") as fh:
        lines = fh.read().splitlines()
    header = [l for l in lines if l.startswith("#")]
    rows = [l for l in lines if not l.startswith("#") and l.strip()]
    p = B.CopernicusProvider(http=None)
    latmin, latmax, lonmin, lonmax = p.BBOX
    inb, outb, nowave, bad = [], [], [], []
    for l in rows:
        r = next(csv.reader([l]))
        if len(r) < 8:
            bad.append(l); continue
        try:
            la = (float(r[2]) + float(r[3])) / 2.0
            lo = (float(r[4]) + float(r[5])) / 2.0
        except ValueError:
            bad.append(l); continue
        wave = "VHM0" in r[-1] or "VAVH" in r[-1]
        if not wave:
            nowave.append(l)
        elif latmin <= la <= latmax and lonmin <= lo <= lonmax:
            inb.append(l)
        else:
            outb.append(l)
    # deterministic picks: every 7th in-bbox wave row (keeps multi-date duplicates of the same
    # platform), 40 out-of-bbox, 40 no-wave, all malformed (usually few), plus hand-made edge rows
    sample = header + inb[::7][:260] + outb[:40] + nowave[:40] + bad[:10]
    tmpl = inb[0]
    sample.append(tmpl.replace(tmpl.split(",")[2], "abc", 1))            # malformed lat
    sample.append(",".join(tmpl.split(",")[:6]))                          # short row (<8 cols)
    sample.append("")                                                     # blank line
    old = next(csv.reader([tmpl]))
    old[7] = "2020-01-01T00:00:00Z"                                       # stale platform
    sample.append(",".join(old))
    return "\n".join(sample) + "\n"


def run_old_parser(sample_text):
    """Exercise the provider exactly as the route does, under a frozen clock."""
    B.time = F.FrozenTime(NOW_EPOCH)
    p = B.CopernicusProvider(http=FakeHTTP(sample_text))
    out = p._fetch_stations()
    return {"stations": out, "file_by_id": dict(p._file_by_id), "http_calls": len(p.http.calls)}


if __name__ == "__main__":
    raw = sys.argv[1]
    text = build_sample(raw)
    with open(SAMPLE, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)
    rec = run_old_parser(text)
    with open(GOLDEN, "w", encoding="utf-8") as fh:
        json.dump(rec, fh, indent=1, sort_keys=True)
    print("sample lines", text.count("\n"), "| stations", len(rec["stations"]),
          "| file_by_id", len(rec["file_by_id"]), "| wrote", SAMPLE, GOLDEN)
