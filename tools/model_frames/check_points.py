"""The live forecast-point product against NOAA's point bulletins of the SAME run (a check to run by hand:
after a change to the points job or to point_forecast.py, and when NOAA changes its wave products).

python tools/model_frames/check_points.py [bucket root, default https://models.allshoresurf.com] [station ids ...]

For each station that has a bulletin: its model cell (through point_forecast.PointSource, as the site reads it), the
combined height at every step, and for every bulletin partition of at least 0.3 m the product's partition of the
same hour with the nearest peak period and direction (within 1.5 s and 30 degrees = matched). The bulletin is made
from the model's spectrum AT the station and has up to six partitions; the product is the gridded cell and has the
wind sea and three swells: open-ocean stations match on height to a few centimetres, coastal stations (whose cell
is not their water) less, and bulletin partitions beyond the product's four are not found. Then that every row
the site builds is in rank order (height squared x period). Exit code 1 if the product and the bulletins are not
the same waves (thresholds at the end). Read-only: anonymous GETs.
"""
import os
import re
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import point_forecast as PFC   # noqa: E402

STATIONS = ("51201 51001 51002 51003 51004 51101 51209 51000 46001 46006 46026 46042 46059 46035 46066 46075 46005 46002 "
            "41001 41002 41010 41048 41040 44008 44011 42001 42002 42058 32012 31201 21004 22101 23092 13130 62001 62081 62105 "
            "64045 55020 56005 52200 31052 32487 46246 44137").split()
S3 = "https://noaa-gfs-bdp-pds.s3.amazonaws.com"


def http(url, max_bytes=8 << 20):
    req = urllib.request.Request(url, headers={"User-Agent": "allshore-check"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read(max_bytes + 1)[:max_bytes]


def parse_bulletin(text, cycle_hour):
    """-> (lat, lon, rows): rows[i] = (combined height m, [(height m, peak period s, direction deg FROM), ...]) for
    hour i of the run. The bulletin gives the direction the waves travel TO."""
    m = re.search(r"Location\s*:\s*\S+\s*\(\s*([\d.]+)([NS])\s+([\d.]+)([EW])\)", text)
    if not m:
        return None
    lat = float(m.group(1)) * (1 if m.group(2) == "N" else -1)
    lon = float(m.group(3)) * (1 if m.group(4) == "E" else -1)
    rows = []
    for line in text.splitlines():
        cells = line.split("|")
        if len(cells) < 9 or not re.fullmatch(r"\s*\d+\s+\d+\s*", cells[1]):
            continue
        if int(cells[1].split()[1]) != (cycle_hour + len(rows)) % 24:
            raise ValueError(f"bulletin rows are not hourly at row {len(rows)}: {line[:30]!r}")
        parts = []
        for c in cells[3:9]:
            c = c.replace("*", " ").split()
            if len(c) == 3:
                parts.append((float(c[0]), float(c[1]), (float(c[2]) + 180.0) % 360.0))
        rows.append((float(cells[2].split()[0]), parts))
    return lat, lon, rows


def turn(a, b):
    return abs((a - b + 180.0) % 360.0 - 180.0)


def main(argv):
    root = argv[1].rstrip("/") if len(argv) > 1 else "https://models.allshoresurf.com"
    stations = argv[2:] or STATIONS
    src = PFC.PointSource(lambda: root, http)
    man = src.manifest()
    run, steps = man["run"], man["steps"]
    PF = PFC.pointfmt()
    ihs = PF.FIELD_NAMES.index("hs")
    print(f"product run {run}, {len(steps)} steps, grids {[g['name'] for g in man['grids']]}")
    print(f"{'id':6s} {'grid':4s} {'km':>5s} | combined Hs mean / max |d| (m) | bulletin partitions >= 0.3 m: n, matched, |dHs| |dTp| |dDir| | matched after +120 h")
    tot = {"st": 0, "hs_n": 0, "hs": 0.0, "bp": 0, "hit": 0, "dh": 0.0, "dt": 0.0, "dd": 0.0}
    cont = {"rows": 0, "ranked": 0}
    ocean = []                                               # per open-ocean station on a native grid: mean |d| of combined Hs
    for sid in stations:
        try:
            text = http(f"{S3}/gfs.{run[:8]}/{run[8:]}/wave/station/bulls.t{run[8:]}z/gfswave.{sid}.bull").decode("latin-1")
        except Exception:                                    # noqa: BLE001  no bulletin for this id in this run
            continue
        b = parse_bulletin(text, int(run[8:]))
        if not b:
            continue
        lat, lon, brows = b
        cell, why = src.locate(man, lat, lon)
        if cell is None:
            print(f"{sid:6s} no forecast at {lat}, {lon}: {why}")
            continue
        codes = src.series(man, cell)
        parts = [PFC.partitions_at(codes, si) for si in range(len(steps))]
        s = {"hs": [], "bp": 0, "hit": 0, "dh": 0.0, "dt": 0.0, "dd": 0.0, "bp_late": 0, "hit_late": 0}
        for si, hour in enumerate(steps):
            if hour >= len(brows):
                break
            hst, bparts = brows[hour]
            if int(codes[ihs][si]) != PF.MISSING:
                s["hs"].append(abs(int(codes[ihs][si]) / 100.0 - hst))
            for bp in bparts:
                if bp[0] < 0.3:
                    continue
                s["bp"] += 1
                s["bp_late"] += hour > 120
                near = [p for p in parts[si] if abs(p[1] - bp[1]) <= 1.5 and turn(p[2], bp[2]) <= 30]
                if near:
                    p = min(near, key=lambda p: abs(p[1] - bp[1]) / 1.5 + turn(p[2], bp[2]) / 30)
                    s["hit"] += 1
                    s["hit_late"] += hour > 120
                    s["dh"] += abs(p[0] - bp[0])
                    s["dt"] += abs(p[1] - bp[1])
                    s["dd"] += turn(p[2], bp[2])
        rows = PFC.point_rows(codes, steps, man["run_dt"], __import__("pytz").utc)
        for r in rows:                                               # every row in rank order, packed from the left
            live = [g for g in range(6) if r[2 + 3 * g] is not None]
            power = [PFC.swell_power(r[2 + 3 * g], r[3 + 3 * g]) for g in live]
            cont["rows"] += 1
            cont["ranked"] += live == list(range(len(live))) and power == sorted(power, reverse=True)
        hit = s["hit"] or 1
        mean_hs = sum(s["hs"]) / max(1, len(s["hs"]))
        print(f"{sid:6s} {cell['grid']:4s} {cell['km']:5.1f} |      {mean_hs:6.3f} / {max(s['hs'] or [0]):5.2f}        | {s['bp']:5d} {100 * s['hit'] / max(1, s['bp']):6.1f} %  "
              f"{s['dh'] / hit:5.3f} {s['dt'] / hit:5.2f} {s['dd'] / hit:5.1f} | {100 * s['hit_late'] / max(1, s['bp_late']):6.1f} %")
        tot["st"] += 1
        tot["hs_n"] += len(s["hs"])
        tot["hs"] += sum(s["hs"])
        for k in ("bp", "hit", "dh", "dt", "dd"):
            tot[k] += s[k]
        if cell["grid"] != "n25" and cell["km"] < 10:
            ocean.append(mean_hs)
    if not tot["st"]:
        print("no station could be compared")
        return 1
    hit = tot["hit"] or 1
    share, dt, dd = tot["hit"] / max(1, tot["bp"]), tot["dt"] / hit, tot["dd"] / hit
    print(f"\n{tot['st']} stations: combined Hs mean |d| {tot['hs'] / max(1, tot['hs_n']):.3f} m over {tot['hs_n']} rows; bulletin partitions >= 0.3 m: "
          f"{tot['bp']}, matched {100 * share:.1f} %, mean |dHs| {tot['dh'] / hit:.3f} m, |dTp| {dt:.2f} s, |dDir| {dd:.1f} deg")
    print(f"rows in rank order (height squared x period, packed from the left): {cont['ranked']} of {cont['rows']}")
    med = sorted(ocean)[len(ocean) // 2] if ocean else None
    print(f"stations within 10 km of their cell on a native grid: {len(ocean)}, median of their mean |d| of combined Hs: {med if med is None else round(med, 3)} m")
    # the same waves: matched partitions agree on period and direction (the convention: FROM), most bulletin
    # partitions are found, the combined height agrees where the cell is the station's water, every row is ranked
    ok = share > 0.75 and dt < 0.15 and dd < 4.0 and (med is None or med < 0.03) and cont["ranked"] == cont["rows"]
    print("RESULT:", "the product and the bulletins are the same waves" if ok else "MISMATCH: look at the table above")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
