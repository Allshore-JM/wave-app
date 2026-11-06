"""Real radar frames for tests/overlay/radar.test.js: one slot's Hawaii and Guam pictures from the published store, the
pointer's fields the client checks, the job's cell tables, and the code at sampled nodes read back with the job's own format
rules (tools/model_frames/radar.py). Run by hand when the format changes:  python tests/capture_radar_client_fixtures.py
Writes tests/fixtures/radar_client/ and needs network access to models.allshoresurf.com."""
import io
import json
import os
import sys
import urllib.request
import zlib
import struct

ROOT = "https://models.allshoresurf.com/radar/v1"
HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "fixtures", "radar_client")
sys.path.insert(0, os.path.join(HERE, "..", "tools", "model_frames"))
import radar as R  # noqa: E402


def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "allshore-tests"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def grey(png):
    """The PNG's rows, by the plain PNG rules (independent of the client's decoder)."""
    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    at, idat, w, h = 8, b"", 0, 0
    while at < len(png):
        n, kind = struct.unpack(">I4s", png[at:at + 8])
        body = png[at + 8:at + 8 + n]
        if kind == b"IHDR":
            w, h, depth, colour = struct.unpack(">IIBB", body[:10])
            assert depth == 8 and colour == 0
        elif kind == b"IDAT":
            idat += body
        at += 12 + n
    raw = zlib.decompress(idat)
    out, prev, i = [], bytearray(w), 0
    for _ in range(h):
        f = raw[i]
        line = bytearray(raw[i + 1:i + 1 + w])
        i += 1 + w
        for c in range(w):
            a = line[c - 1] if c else 0
            b = prev[c]
            p = prev[c - 1] if c else 0
            if f == 1:
                line[c] = (line[c] + a) & 255
            elif f == 2:
                line[c] = (line[c] + b) & 255
            elif f == 3:
                line[c] = (line[c] + (a + b) // 2) & 255
            elif f == 4:
                pa, pb, pc = abs(b - p), abs(a - p), abs(a + b - 2 * p)
                line[c] = (line[c] + (a if pa <= pb and pa <= pc else b if pb <= pc else p)) & 255
        out.append(bytes(line))
        prev = line
    return w, h, out


def main():
    os.makedirs(OUT, exist_ok=True)
    ptr = json.loads(get(ROOT + "/latest.json"))
    # the newest slot where Hawaii has t1 and t2 cells; else the newest with t1
    pick = None
    for rec in reversed(ptr["slots"]):
        e = rec["r"].get("hawaii")
        if e and e.get("t2") and e.get("t1"):
            pick = rec
            break
    assert pick, "no slot with Hawaii t1 + t2 cells held now"
    slot = pick["t"]
    names = {"slot": slot, "t0": {}, "t1": {}, "t2": {}}
    expect = {"slot": slot, "o": {}, "t0": {}, "t1": {}, "t2": {}}
    for region in ("hawaii", "guam"):
        e = pick["r"].get(region)
        if not e:
            continue
        expect["o"][region] = e["o"]
        body = get("%s/%s/%s/t0.png" % (ROOT, slot, region))
        with open(os.path.join(OUT, "%s_t0.png" % region), "wb") as f:
            f.write(body)
        w, h, rows = grey(body)
        s, wlon, n, elon = ptr["regions"][region]["box"]
        assert (w, h) == ((elon - wlon) * 10, (n - s) * 10)
        cov = [[r, c, rows[r][c]] for r in range(0, h, 7) for c in range(0, w, 11)]
        echo = [[r, c, rows[r][c]] for r in range(h) for c in range(w) if rows[r][c] >= 2][:40]
        expect["t0"][region] = {"cols": w, "rows": h, "samples": cov, "echo": echo}
        names["t0"][region] = True
    e = pick["r"]["hawaii"]
    for tier, cell in (("t1", 20), ("t2", 5)):
        cells = cell_names("hawaii", cell)
        hex_ = e[tier]
        listed = [nm for i, nm in enumerate(cells) if bit(hex_, i)]
        nm = listed[0]
        body = get("%s/%s/hawaii/%s/%s.png" % (ROOT, slot, tier, nm))
        with open(os.path.join(OUT, "hawaii_%s_%s.png" % (tier, nm)), "wb") as f:
            f.write(body)
        w, h, rows = grey(body)
        echo = [[r, c, rows[r][c]] for r in range(1, h - 1) for c in range(1, w - 1) if rows[r][c] >= 2]
        expect[tier] = {"name": nm, "cols": w, "rows": h, "listed": listed, "echo": echo[:: max(1, len(echo) // 40)][:40]}
    # the pointer with the picked slot and two neighbours, every field the client validates
    keep = dict(ptr)
    i = [k for k, r in enumerate(ptr["slots"]) if r["t"] == slot][0]
    keep["slots"] = ptr["slots"][max(0, i - 2):i + 1]
    with io.open(os.path.join(OUT, "pointer.json"), "w", encoding="utf-8", newline="\n") as f:
        json.dump(keep, f, indent=0, sort_keys=True)
    tables = {}
    for region in ptr["regions"]:
        tables[region] = {"t1": cell_names(region, 20), "t2": cell_names(region, 5)}
    with io.open(os.path.join(OUT, "cell_tables.json"), "w", encoding="utf-8", newline="\n") as f:
        json.dump(tables, f, sort_keys=True)
    with io.open(os.path.join(OUT, "expect.json"), "w", encoding="utf-8", newline="\n") as f:
        json.dump(expect, f, sort_keys=True)
    print("slot", slot, "t1", expect["t1"]["name"], "t2", expect["t2"]["name"])


def cell_names(region, cell):
    """The job's cell_table, by name."""
    return ["%d_%d" % (lat, lon) for lat, lon in R.cell_table(region, cell)]


def bit(hex_, i):
    ch = i >> 2
    return ch < len(hex_) and (int(hex_[ch], 16) >> (i & 3)) & 1 == 1


if __name__ == "__main__":
    main()
