"""Capture the AODN station-list golden from the UNCHANGED AODNProvider._fetch_stations.

Run ONCE on the pre-change commit with the real WFS CSV downloaded:
    python tests/capture_aodn_golden.py <path-to-aodn_wfs.csv>
Builds tests/fixtures/aodn_wfs_sample.csv (six full site series from the real feed plus
hand-made edge rows) and tests/fixtures/aodn_golden.json holding EVERYTHING the provider
derives: the station list, _latest_by_id and _recent_by_id, for the sample and for the
XML-error and non-200 responses.
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

SAMPLE = os.path.join(HERE, "fixtures", "aodn_wfs_sample.csv")
GOLDEN = os.path.join(HERE, "fixtures", "aodn_golden.json")
KEEP_SITES = ["Lakes Entrance", "Augusta Offshore", "Apollo Bay", "Bengello", "Albany", "Bob"]


class _Resp:
    def __init__(self, text, status=200):
        self.text = text
        self.content = text.encode("utf-8")
        self.status_code = status

    def iter_lines(self, decode_unicode=False, chunk_size=None):
        for line in self.text.splitlines():
            yield line if decode_unicode else line.encode("utf-8")

    def iter_content(self, chunk_size=65536):
        for i in range(0, len(self.content), chunk_size):
            yield self.content[i:i + chunk_size]

    def close(self):
        pass


class FakeHTTP:
    def __init__(self, text, status=200):
        self.text, self.status, self.calls = text, status, []

    def get(self, url, **kw):
        self.calls.append((url, kw))
        return _Resp(self.text, self.status)


def build_sample(raw_path):
    with open(raw_path, encoding="utf-8", newline="") as fh:
        rows = list(csv.reader(fh))
    hdr, data = rows[0], rows[1:]
    ix = {c: i for i, c in enumerate(hdr)}
    keep = [r for r in data if len(r) > ix["geom"] and r[ix["site_name"]] in KEEP_SITES]
    tmpl = next(r for r in keep if r[ix["site_name"]] == "Bob")
    edge = []
    r = list(tmpl); r[ix["site_name"]] = ""; edge.append(r)                       # blank site -> skipped
    edge.append(tmpl[:ix["geom"]])                                                # short row -> skipped
    r = list(tmpl); r[ix["site_name"]] = "Odd Site"; r[ix["TIME"]] = "2026-09-10T17:40:00Z"; edge.append(r)   # unparseable TIME
    r = list(tmpl); r[ix["site_name"]] = "Odd Site"; r[ix["TIME"]] = "2026-09-09T17:40:00"; edge.append(r)
    r = list(tmpl); r[ix["site_name"]] = "Odd Site"; r[ix["TIME"]] = "2026-09-08T17:40:00"; edge.append(r)
    r = list(tmpl); r[ix["site_name"]] = "No Geom"; r[ix["geom"]] = "n/a"; edge.append(r)   # no POINT -> skipped
    r = list(tmpl); r[ix["site_name"]] = "Bad End"; r[ix["time_end"]] = "soon"; edge.append(r)   # unparseable time_end
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\n")
    w.writerow(hdr)
    # interleave edge rows so pruning sees them mid-stream
    for i, row in enumerate(keep):
        w.writerow(row)
        if i == 40:
            for e in edge:
                w.writerow(e)
    return out.getvalue()


def run_scenarios(text):
    out = {}
    for name, http in (("sample", FakeHTTP(text)),
                       ("xml_error", FakeHTTP("<ows:ExceptionReport>boom</ows:ExceptionReport>")),
                       ("http_500", FakeHTTP(text, 500)),
                       ("header_only", FakeHTTP(text.split("\n")[0] + "\n"))):
        p = B.AODNProvider(http=http)
        stations = p._fetch_stations()
        out[name] = {"stations": stations, "latest_by_id": p._latest_by_id,
                     "recent_by_id": p._recent_by_id, "http_calls": len(http.calls)}
    return out


if __name__ == "__main__":
    text = build_sample(sys.argv[1])
    with open(SAMPLE, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)
    rec = run_scenarios(text)
    json.dump(rec, open(GOLDEN, "w", encoding="utf-8"), indent=1, sort_keys=True)
    s = rec["sample"]
    print("sample rows", text.count("\n"), "| stations", len(s["stations"]),
          "| recent lens", {k: len(v) for k, v in s["recent_by_id"].items()}, "| wrote", SAMPLE, GOLDEN)
