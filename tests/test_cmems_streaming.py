"""Release G1: the Copernicus index is streamed instead of held whole; output is unchanged.

Golden: tests/fixtures/cmems_golden.json, captured by tests/capture_cmems_golden.py from the
UNCHANGED parser over tests/fixtures/cmems_index_sample.txt (a slice of the real index).
The old parser is also kept below, verbatim, as an independent reference for the large-input
equivalence + memory test.
"""
import csv
import io
import json
import os
import sys
import time
import tracemalloc

import pytest
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import buoy_sources as B  # noqa: E402
import capture_cmems_golden as G  # noqa: E402
import fake_buoy_providers as F  # noqa: E402

GOLDEN = json.load(open(G.GOLDEN, encoding="utf-8"))
SAMPLE = open(G.SAMPLE, encoding="utf-8").read()


@pytest.fixture(autouse=True)
def _frozen(monkeypatch):
    monkeypatch.setattr(B, "time", F.FrozenTime(G.NOW_EPOCH))
    yield


def _old_parser(p, idx_text):
    """The pre-G1 body of CopernicusProvider._fetch_stations, verbatim (whole-string parse)."""
    latmin, latmax, lonmin, lonmax = p.BBOX
    now = B.time.time()
    best = {}
    for r in csv.reader(io.StringIO(idx_text)):
        if not r or r[0].startswith("#") or len(r) < 8:
            continue
        if "VHM0" not in r[-1] and "VAVH" not in r[-1]:
            continue
        try:
            la = (float(r[2]) + float(r[3])) / 2.0
            lo = (float(r[4]) + float(r[5])) / 2.0
        except (ValueError, IndexError):
            continue
        if not (latmin <= la <= latmax and lonmin <= lo <= lonmax):
            continue
        tend = r[7].strip()
        tz = tend if tend.endswith("Z") else tend + "Z"
        ep = B._z_epoch({"time_utc": tz})
        if ep is None or (now - ep) > p.LIVE_MAX_AGE:
            continue
        fn = r[1]
        pid = fn.split("/")[-1].rsplit("_", 1)[0]
        if pid not in best or tend > best[pid][0]:
            best[pid] = (tend, fn, la, lo, tz)
    out, file_by_id = [], {}
    for pid, (tend, fn, la, lo, tz) in best.items():
        file_by_id[pid] = fn
        name = pid.split("_")[-1].replace("-", " ") if "_" in pid else pid
        out.append({"local_id": pid, "name": name, "lat": la, "lon": lo, "latest_time": tz})
    return out, file_by_id


def test_golden_sample_identical():
    rec = G.run_old_parser(SAMPLE)          # drives the CURRENT provider through the fake http
    assert json.dumps(rec, sort_keys=True) == json.dumps(GOLDEN, sort_keys=True)


def test_streams_instead_of_reading_text():
    http = G.FakeHTTP(SAMPLE)
    p = B.CopernicusProvider(http=http)
    p._fetch_stations()
    assert http.calls and http.calls[0][1].get("stream") is True


def test_request_exception_on_get_and_mid_body_are_fail_soft():
    class Boom:
        def get(self, url, **kw):
            raise requests.ConnectionError("down")
    assert B.CopernicusProvider(http=Boom())._fetch_stations() == []

    class MidBody(G._Resp):
        def iter_lines(self, decode_unicode=False, chunk_size=None):
            yield from list(super().iter_lines(decode_unicode))[:50]
            raise requests.exceptions.ChunkedEncodingError("cut")

    class H:
        def get(self, url, **kw):
            return MidBody(SAMPLE)
    assert B.CopernicusProvider(http=H())._fetch_stations() == []


def test_bytes_lines_are_decoded():
    class BytesResp(G._Resp):
        def iter_lines(self, decode_unicode=False, chunk_size=None):
            for line in self.text.splitlines():
                yield line.encode("utf-8")          # what requests yields with no charset

    class H:
        def get(self, url, **kw):
            return BytesResp(SAMPLE)
    p = B.CopernicusProvider(http=H())
    out = p._fetch_stations()
    assert json.dumps(out, sort_keys=True) == json.dumps(GOLDEN["stations"], sort_keys=True)


def test_large_index_same_output_small_peak():
    """A ~40 MB synthetic index (the real one's size): new == old byte-for-byte, and the new
    parser's heap peak stays far below the old whole-string peak."""
    header = SAMPLE.split("\n")[:6]
    body = [l for l in SAMPLE.split("\n")[6:] if l]
    big_lines = header[:]
    n = 0
    while sum(len(l) + 1 for l in big_lines) < 40_000_000:
        for l in body:
            # vary the platform id so the index is not just repeats of one station
            big_lines.append(l.replace("_20260", "_%03d_20260" % (n % 500)))
            n += 1
    big = "\n".join(big_lines) + "\n"
    p_old = B.CopernicusProvider(http=None)
    tracemalloc.start(); tracemalloc.reset_peak()
    old_out, old_map = _old_parser(p_old, big)
    old_peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()

    class ChunkResp(G._Resp):
        def iter_lines(self, decode_unicode=False, chunk_size=None):
            # emulate requests: decode chunk by chunk, split on newlines
            pending = ""
            for chunk in self.iter_content(65536):
                pending += chunk.decode("utf-8")
                parts = pending.split("\n")
                pending = parts.pop()
                for part in parts:
                    yield part
            if pending:
                yield pending

    prebuilt = ChunkResp(big)                 # its 40 MB .content copy is not the parser's doing

    class H:
        def get(self, url, **kw):
            return prebuilt
    p_new = B.CopernicusProvider(http=H())
    tracemalloc.start(); tracemalloc.reset_peak()
    new_out = p_new._fetch_stations()
    new_peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    assert json.dumps(new_out, sort_keys=True) == json.dumps(old_out, sort_keys=True)
    assert p_new._file_by_id == old_map
    assert len(new_out) > 1000
    # ChunkResp holds `big` (40 MB) itself, so measure the parser's own allocations: the old
    # path must allocate at least a whole extra copy of the index; the new path must not.
    assert old_peak > 40_000_000, old_peak
    assert new_peak < 15_000_000, new_peak
