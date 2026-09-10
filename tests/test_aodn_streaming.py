"""AODN: the WFS map layer is streamed and pruned to the rows the output uses; output unchanged.

Golden: tests/fixtures/aodn_golden.json (capture_aodn_golden.py, unchanged code) over
tests/fixtures/aodn_wfs_sample.csv -- station list, _latest_by_id and _recent_by_id for the
sample, an XML error body, a non-200 and a header-only body. The old provider body is kept
below verbatim as the reference for the large-feed equivalence + memory test.
"""
import csv
import io
import json
import os
import re
import sys
import tracemalloc
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import buoy_sources as B  # noqa: E402
import capture_aodn_golden as G  # noqa: E402

GOLDEN = json.load(open(G.GOLDEN, encoding="utf-8"))
SAMPLE = open(G.SAMPLE, encoding="utf-8", newline="").read()


def _old_fetch_stations(p, text):
    """Pre-change AODNProvider._fetch_stations + _wfs_rows, verbatim (whole-body parse)."""
    if text.lstrip().startswith("<"):
        hdr, rows = [], []
    else:
        allrows = list(csv.reader(io.StringIO(text)))
        hdr, rows = ((allrows[0] if allrows else []), []) if len(allrows) < 2 else (allrows[0], allrows[1:])
    if not hdr:
        return [], {}, {}
    ix = {c: i for i, c in enumerate(hdr)}
    if any(c not in ix for c in ("site_name", "time_end", "TIME", "geom")):
        return [], {}, {}
    bysite = {}
    for row in rows:
        if len(row) <= ix["geom"]:
            continue
        s = row[ix["site_name"]]
        if s:
            bysite.setdefault(s, []).append(row)
    out, latest_by, recent_by = [], {}, {}
    for s, srows in bysite.items():
        srows.sort(key=lambda r: r[ix["TIME"]])
        last = srows[-1]
        m = p._POINT.search(last[ix["geom"]] or "")
        if not m:
            continue
        lon, lat = float(m.group(1)), float(m.group(2))
        end_utc = B._parse_naive(last[ix["time_end"]])
        max_t = B._parse_naive(last[ix["TIME"]])
        offset = (max_t - end_utc) if (end_utc and max_t) else None
        obs = []
        for r in srows:
            tl = B._parse_naive(r[ix["TIME"]])
            if tl is not None and offset is not None:
                tu = (tl - offset).strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                tu = B._z(r[ix["time_end"]])
            obs.append(p._obs(r, ix, tu))
        latest_by[s] = obs[-1]
        recent_by[s] = B._recent_window(obs)
        inst = last[ix["institution"]] if "institution" in ix else ""
        st = {"local_id": s, "name": ("%s - %s" % (s, inst)) if inst else s,
              "lat": lat, "lon": lon, "latest_time": obs[-1]["time_utc"]}
        if p._spectra_code(s):
            st["capabilities"] = B._caps(bulk=True, recent_history=True, directional=True,
                                         spectra=True, partitions=True)
        out.append(st)
    return out, latest_by, recent_by


def _dump(o):
    return json.dumps(o, sort_keys=True)


def test_golden_scenarios_identical():
    rec = G.run_scenarios(SAMPLE)
    for name in GOLDEN:
        for k in ("stations", "latest_by_id", "recent_by_id"):
            assert _dump(rec[name][k]) == _dump(GOLDEN[name][k]), (name, k)
        assert rec[name]["http_calls"] == GOLDEN[name]["http_calls"]


def test_streams_and_closes():
    http = G.FakeHTTP(SAMPLE)
    B.AODNProvider(http=http)._fetch_stations()
    assert http.calls[0][1].get("stream") is True


def test_bytes_lines_and_blank_leading_lines():
    class R(G._Resp):
        def iter_lines(self, decode_unicode=False, chunk_size=None):
            yield b""                                    # blank line before the header
            for line in self.text.splitlines():
                yield line.encode("utf-8")

    class H:
        def get(self, url, **kw):
            return R(SAMPLE)
    p = B.AODNProvider(http=H())
    out = p._fetch_stations()
    # A blank first line made the old code's header row [] -> "no header" -> []. Same now.
    ref = _old_fetch_stations(B.AODNProvider(http=None), "\n" + SAMPLE)
    assert _dump(out) == _dump(ref[0]) and out == []
    assert _dump(p._recent_by_id) == _dump(ref[2])


def _big_feed():
    """~4 MB feed like the real one: the six sample sites stretched to ~8 days of 30-min rows,
    plus the edge rows, in shuffled-but-deterministic order."""
    rows = list(csv.reader(io.StringIO(SAMPLE)))
    hdr, data = rows[0], rows[1:]
    ix = {c: i for i, c in enumerate(hdr)}
    out = [hdr]
    for r in data:
        out.append(r)
        t = B._parse_naive(r[ix["TIME"]])
        if t is None or not r[ix["site_name"]] or len(r) <= ix["geom"]:
            continue
        for k in range(1, 12):                          # 11 older copies, 30 min apart... days back
            rr = list(r)
            rr[ix["TIME"]] = (t - timedelta(days=k, minutes=7 * k)).strftime("%Y-%m-%dT%H:%M:%S")
            rr[ix["significant_wave_height"]] = "%.3f" % (0.5 + (k % 5) / 3.0)
            out.append(rr)
    buf = io.StringIO()
    csv.writer(buf, lineterminator="\n").writerows(out)
    return buf.getvalue()


def test_large_feed_same_output_smaller_peak():
    text = _big_feed()
    assert len(text) > 2_000_000
    p_old = B.AODNProvider(http=None)
    tracemalloc.start(); tracemalloc.reset_peak()
    old = _old_fetch_stations(p_old, text)
    old_peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()

    class ChunkResp(G._Resp):
        def iter_lines(self, decode_unicode=False, chunk_size=None):
            pending = ""
            for chunk in self.iter_content(65536):
                pending += chunk.decode("utf-8")
                parts = pending.split("\n")
                pending = parts.pop()
                yield from parts
            if pending:
                yield pending
    prebuilt = ChunkResp(text)

    class H:
        def get(self, url, **kw):
            return prebuilt
    p_new = B.AODNProvider(http=H())
    tracemalloc.start(); tracemalloc.reset_peak()
    new_stations = p_new._fetch_stations()
    new_peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    assert _dump(new_stations) == _dump(old[0])
    assert _dump(p_new._latest_by_id) == _dump(old[1])
    assert _dump(p_new._recent_by_id) == _dump(old[2])
    assert all(len(v) > 0 for v in old[2].values() if v)     # window exercised
    assert new_peak < old_peak / 3, (new_peak, old_peak)
