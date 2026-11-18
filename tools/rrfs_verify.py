"""The RRFS verification gate (plan section 99, step 5): RRFS station winds vs GFS station winds vs observations.

Reads only public data, read-only:
  - the RRFS job's station tables (rrfs/v1/latest.json lists every kept source: up to 3 days of hourly cycles and runs)
  - the site's GFS station winds (gfswave/winds/v1/<RUN>/, 8 runs kept, ~48 h)
  - NDBC real-time observations (www.ndbc.noaa.gov/data/realtime2/<id>.txt, the last 45 days), paced, cached on disk

Each forecast (source, lead) of a station is paired with the observation nearest its valid hour (within 10 min). The
report gives, per RRFS grid and per lead band, the pairs, and for RRFS and GFS on the SAME station-hours: speed MAE,
bias and RMSE, direction MAE (both speeds >= 3 m/s), and RRFS's gust MAE / bias against the observed gust.

Observation height: NDBC buoys measure wind at ~4 m, both models give 10 m. By default a buoy's speed and gust are
scaled to 10 m with the power law over the sea, (10 / 4.1) ** 0.11 = 1.103 (BUOY_TO_10M); C-MAN and coastal stations
(their heights vary, many near 10 m) are left as measured. --no-height turns the scaling off.

The gate: a grid passes with >= MIN_PAIRS matched pairs and RRFS's speed MAE not worse than GFS's (all leads to +84 h);
the owner decides any grid that does not.

  python tools/rrfs_verify.py --out DIR                    # report.md + report.json in DIR (NDBC cache in DIR/ndbc)
  python tools/rrfs_verify.py --out DIR --no-height        # observations as measured
"""
import argparse
import json
import math
import os
import struct
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)

BUCKET = "https://models.allshoresurf.com"
NDBC = "https://www.ndbc.noaa.gov/data/realtime2"
UA = {"User-Agent": "allshoresurf.com RRFS verification (https://allshoresurf.com)"}
MISSING = 65535
OBS_WINDOW_MIN = 10
MIN_PAIRS = 200
DIR_MIN_MS = 3.0
BUOY_TO_10M = (10 / 4.1) ** 0.11
LEADS = [(0, 6), (7, 18), (19, 48), (49, 84)]
PACE_S = 0.4
GFS_RUNS_BACK = 12                      # 6-hourly runs looked for (the product keeps 8)


# ------------------------------------------------------------------------------------------------------------- fetching

def http_get(url, tries=3):
    """Bytes, or None on 403/404 (not there)."""
    last = None
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=60) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code in (403, 404):
                return None
            last = e
        except Exception as e:                       # noqa: BLE001
            last = e
        time.sleep(2 * (attempt + 1))
    raise IOError(f"GET {url}: {last}")


# -------------------------------------------------------------------------------------------------------- the forecasts

def rrfs_forecasts(get, base=BUCKET):
    """({sid: grid}, [(sid, valid naive UTC, lead h, source cycle, speed, dir | None, gust | None)]) from every kept
    RRFS source's station table."""
    ptr = json.loads(get(f"{base}/rrfs/v1/latest.json"))
    grids, out = {}, []
    for src in ptr["sources"]:
        doc = json.loads(get(f"{base}/{src['stations']}"))
        body = get(f"{base}/{src['stations'][:-len('.json')]}.bin")
        if body is None or len(body) != doc["count"] * doc["record_bytes"]:
            print(f"RRFS {src['cycle']}: station table missing or damaged, left out")
            continue
        base_t = datetime.strptime(src["cycle"], "%Y%m%d%H")
        n = len(doc["steps"])
        for sid, (slot, grid, _j, _i) in doc["stations"].items():
            grids[sid] = grid
            vals = struct.unpack(f"<{3 * n}H", body[slot * doc["record_bytes"]:(slot + 1) * doc["record_bytes"]])
            for k, h in enumerate(doc["steps"]):
                s, d, g = vals[3 * k:3 * k + 3]
                if s == MISSING:
                    continue
                out.append((sid, base_t + timedelta(hours=h), int(h), src["cycle"], s / 100,
                            None if d == MISSING else d / 10, None if g == MISSING else g / 100))
    return grids, out


def gfs_forecasts(get, sids, now, base=BUCKET, back=GFS_RUNS_BACK, max_lead=84):
    """[(sid, valid, lead, run, speed, dir, None)] of the kept GFS winds runs for the stations `sids`, leads <= max_lead."""
    out = []
    t = now.replace(minute=0, second=0, microsecond=0)
    t = t.replace(hour=t.hour // 6 * 6)
    for k in range(back):
        run_dt = t - timedelta(hours=6 * k)
        run = f"{run_dt:%Y%m%d%H}"
        raw = get(f"{base}/gfswave/winds/v1/{run}/index.json")
        if raw is None:
            continue
        doc = json.loads(raw)
        body = get(f"{base}/gfswave/winds/v1/{run}/winds.bin")
        if body is None or len(body) != doc["count"] * doc["record_bytes"]:
            continue
        steps = doc["steps"]
        for sid in sids:
            slot = doc["stations"].get(sid)
            if slot is None:
                continue
            vals = struct.unpack(f"<{2 * len(steps)}H", body[slot * doc["record_bytes"]:(slot + 1) * doc["record_bytes"]])
            for i, h in enumerate(steps):
                if h > max_lead:
                    break
                s, d = vals[2 * i], vals[2 * i + 1]
                if s == MISSING:
                    continue
                out.append((sid, run_dt + timedelta(hours=h), int(h), run, s / 100, d / 10, None))
    return out


# ------------------------------------------------------------------------------------------------------ observations

def parse_ndbc(text):
    """{naive UTC datetime: (speed m/s, dir deg | None, gust m/s | None)} of an NDBC realtime2 standard met file."""
    out = {}
    lines = text.splitlines()
    if not lines:
        return out
    head = lines[0].lstrip("#").split()
    if not {"WDIR", "WSPD", "GST"} <= set(head):
        return out
    ix = {k: head.index(k) for k in ("YY", "MM", "DD", "hh", "mm", "WDIR", "WSPD", "GST")}
    for line in lines[1:]:
        if line.startswith("#"):
            continue
        p = line.split()
        if len(p) < len(head):
            continue
        try:
            t = datetime(int(p[ix["YY"]]), int(p[ix["MM"]]), int(p[ix["DD"]]), int(p[ix["hh"]]), int(p[ix["mm"]]))
        except ValueError:
            continue
        num = lambda v: None if v == "MM" else float(v)          # noqa: E731
        s = num(p[ix["WSPD"]])
        if s is None:
            continue
        out[t] = (s, num(p[ix["WDIR"]]), num(p[ix["GST"]]))
    return out


def is_buoy(sid):
    """NDBC moored buoys have 5-digit numeric ids (anemometer ~4 m); C-MAN / coastal stations have letters. A wind
    station's id carries its network ("ndbc:51003"): only NDBC's count."""
    net, _, bare = sid.rpartition(":")
    return net in ("", "ndbc") and len(bare) == 5 and bare.isdigit()


def observations(sids, cache_dir, get, pace=PACE_S, log=print):
    """{sid: {time: obs}} from NDBC realtime2 (cached for a day in cache_dir)."""
    os.makedirs(cache_dir, exist_ok=True)
    out = {}
    for sid in sorted(sids):
        path = os.path.join(cache_dir, f"{sid}.txt")
        if os.path.exists(path) and time.time() - os.path.getmtime(path) < 6 * 3600:
            with open(path, encoding="utf-8", errors="replace") as f:
                text = f.read()
        else:
            raw = get(f"{NDBC}/{sid}.txt")
            time.sleep(pace)
            text = raw.decode("utf-8", "replace") if raw else ""
            with open(path, "w", encoding="utf-8") as f:
                f.write(text)
        obs = parse_ndbc(text)
        if obs:
            out[sid] = obs
    log(f"observations: {len(out)} of {len(sids)} stations have NDBC wind")
    return out


def nearest_obs(series, t, window_min=OBS_WINDOW_MIN):
    """The observation nearest t within window_min minutes (the earlier one on a tie), or None."""
    for dm in range(window_min + 1):
        for t2 in (t - timedelta(minutes=dm), t + timedelta(minutes=dm)):
            hit = series.get(t2)
            if hit is not None:
                return hit
    return None


# ---------------------------------------------------------------------------------------------------------- statistics

def band_of(lead):
    for lo, hi in LEADS:
        if lo <= lead <= hi:
            return f"{lo}-{hi} h"
    return None


def angdiff(a, b):
    return abs((a - b + 180) % 360 - 180)


def pair(fc, obs, height=True):
    """{(sid, valid, band): [(speed err, dir err | None, gust err | None), ...]} of one model's forecasts."""
    out = defaultdict(list)
    for sid, t, lead, _src, s, d, g in fc:
        band = band_of(lead)
        series = obs.get(sid)
        if band is None or series is None:
            continue
        o = nearest_obs(series, t)
        if o is None:
            continue
        os_, od, og = o
        k = BUOY_TO_10M if height and is_buoy(sid) else 1.0
        os_ = os_ * k
        og = og * k if og is not None else None
        de = angdiff(d, od) if (d is not None and od is not None and s >= DIR_MIN_MS and os_ >= DIR_MIN_MS) else None
        ge = (g - og) if (g is not None and og is not None) else None
        out[(sid, t, band)].append((s - os_, de, ge))
    return out


def summarise(errs):
    """MAE, bias, RMSE of speed; direction MAE; gust MAE and bias; n."""
    sp = [e[0] for e in errs]
    dr = [e[1] for e in errs if e[1] is not None]
    gu = [e[2] for e in errs if e[2] is not None]
    r = lambda v: round(v, 2)                                      # noqa: E731
    out = {"n": len(sp)}
    if sp:
        out.update(mae=r(sum(map(abs, sp)) / len(sp)), bias=r(sum(sp) / len(sp)),
                   rmse=r(math.sqrt(sum(e * e for e in sp) / len(sp))))
    if dr:
        out.update(dir_mae=r(sum(dr) / len(dr)), dir_n=len(dr))
    if gu:
        out.update(gust_mae=r(sum(map(abs, gu)) / len(gu)), gust_bias=r(sum(gu) / len(gu)), gust_n=len(gu))
    return out


def compare(rr, gf, grids):
    """Per grid and band (and 'all'): RRFS and GFS summaries over the station-hours BOTH have (each model's forecasts
    at that station-hour and band averaged first, so a model with more sources does not weigh more)."""
    def mean(lst):
        n = len(lst)
        return (sum(e[0] for e in lst) / n,
                (lambda d: sum(d) / len(d) if d else None)([e[1] for e in lst if e[1] is not None]),
                (lambda g: sum(g) / len(g) if g else None)([e[2] for e in lst if e[2] is not None]))

    table = defaultdict(lambda: {"rrfs": [], "gfs": []})
    for key in set(rr) & set(gf):
        sid, _t, band = key
        grid = grids.get(sid)
        if grid is None:
            continue
        a, b = mean(rr[key]), mean(gf[key])
        for g_, bd in ((grid, band), (grid, "all"), ("all", band), ("all", "all")):
            table[(g_, bd)]["rrfs"].append(a)
            table[(g_, bd)]["gfs"].append(b)
    return {f"{g_}|{bd}": {"rrfs": summarise(v["rrfs"]), "gfs": summarise(v["gfs"])} for (g_, bd), v in table.items()}


def gate(result):
    """{grid: (verdict, reason)} on the 'all' band."""
    out = {}
    for key, v in sorted(result.items()):
        grid, band = key.split("|")
        if band != "all" or grid == "all":
            continue
        n, a, b = v["rrfs"]["n"], v["rrfs"].get("mae"), v["gfs"].get("mae")
        if n < MIN_PAIRS:
            out[grid] = ("not enough", f"{n} matched pairs (< {MIN_PAIRS})")
        elif a <= b:
            out[grid] = ("pass", f"RRFS MAE {a} <= GFS {b} m/s over {n} pairs")
        else:
            out[grid] = ("owner's call", f"RRFS MAE {a} > GFS {b} m/s over {n} pairs")
    return out


def table_md(result):
    lines = ["| Grid | Lead | Pairs | RRFS MAE | GFS MAE | RRFS bias | GFS bias | RRFS dir MAE | GFS dir MAE | RRFS gust MAE / bias |",
             "|---|---|---|---|---|---|---|---|---|---|"]
    order = [f"{lo}-{hi} h" for lo, hi in LEADS] + ["all"]
    for grid in ("hi", "pr", "conus", "ak", "all"):
        for band in order:
            v = result.get(f"{grid}|{band}")
            if not v:
                continue
            r, g = v["rrfs"], v["gfs"]
            gust = f"{r.get('gust_mae', '-')} / {r.get('gust_bias', '-')}"
            lines.append(f"| {grid} | {band} | {r['n']} | {r.get('mae', '-')} | {g.get('mae', '-')} | {r.get('bias', '-')} | "
                         f"{g.get('bias', '-')} | {r.get('dir_mae', '-')} | {g.get('dir_mae', '-')} | {gust} |")
    return lines


def verdicts_md(verdicts):
    return ["| Grid | Verdict | Why |", "|---|---|---|"] + [f"| {g} | {v} | {why} |" for g, (v, why) in verdicts.items()]


def report_md(result, verdicts, meta):
    lines = [f"# RRFS verification gate ({meta['built_utc']})", "",
             f"Sources: {meta['rrfs_sources']} RRFS sources ({meta['rrfs_first']} .. {meta['rrfs_last']}), "
             f"{meta['gfs_runs']} GFS runs; {meta['obs_stations']} NDBC stations with wind; observations "
             f"{'scaled to 10 m for buoys (x%.3f)' % BUOY_TO_10M if meta['height'] else 'as measured'}.", "",
             "## Forecast stations: gate (all leads to +84 h, matched station-hours)", ""]
    lines += verdicts_md(verdicts)
    lines += ["", "## Forecast stations by grid and lead (speed m/s; direction deg for speeds >= 3 m/s; RRFS gust vs "
              "observed gust)", ""]
    lines += table_md(result)
    st = meta.get("stations")
    if st:
        lines += ["", f"## Wind stations ({st['with_obs']} of {st['stations']} with observations: {st['networks']}; "
                  f"RRFS cells of the {st['rrfs_sources']} kept cell sources, GFS 0.25-deg 10-m wind of {st['gfs_runs']} "
                  f"runs; window {st['start']} .. {st['end']}; land stations as measured)", ""]
        lines += verdicts_md(st["gate"]) + [""] + table_md(st["result"])
    return "\n".join(lines) + "\n"


def main(argv=None, get=http_get, now=None, log=print):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", required=True, help="a directory for report.md, report.json and the NDBC cache")
    ap.add_argument("--no-height", action="store_true", help="observations as measured (no 4 m -> 10 m scaling)")
    ap.add_argument("--base", default=BUCKET)
    ap.add_argument("--stations", action="store_true",
                    help="also the site's wind stations: RRFS cells and GFS 10-m wind (eccodes) vs their observations")
    ap.add_argument("--station-grids", default="hi,pr",
                    help="with --stations: the grids whose wind stations are read (default hi,pr: the forecast "
                         "stations already give CONUS and Alaska plenty of pairs; 'all' for every grid)")
    a = ap.parse_args(argv)
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    os.makedirs(a.out, exist_ok=True)
    grids, rr = rrfs_forecasts(get, a.base)
    log(f"RRFS: {len(rr)} station forecasts of {len(grids)} stations")
    gf = gfs_forecasts(get, set(grids), now, a.base)
    log(f"GFS: {len(gf)} station forecasts")
    obs = observations(set(grids), os.path.join(a.out, "ndbc"), get, log=log)
    result = compare(pair(rr, obs, not a.no_height), pair(gf, obs, not a.no_height), grids)
    verdicts = gate(result)
    cycles = sorted({f[3] for f in rr})
    meta = {"built_utc": f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC", "rrfs_sources": len(cycles),
            "rrfs_first": cycles[0] if cycles else None, "rrfs_last": cycles[-1] if cycles else None,
            "gfs_runs": len({f[3] for f in gf}), "obs_stations": len(obs), "height": not a.no_height}
    if a.stations:
        meta["stations"] = station_gate(a, now, get, log)
    with open(os.path.join(a.out, "report.json"), "w", encoding="utf-8") as f:
        json.dump({"meta": meta, "gate": verdicts, "result": result}, f, indent=1, default=str)
    md = report_md(result, verdicts, meta)
    with open(os.path.join(a.out, "report.md"), "w", encoding="utf-8") as f:
        f.write(md)
    log(md)
    return 0


def _get_json(get):
    def f(url):
        raw = get(url)
        return json.loads(raw) if raw else None
    return f


def station_gate(a, now, get, log, stations_path=None, reader=None, fetch_mod=None, decode=None):
    """The wind-station section: {stations, with_obs, networks, rrfs_sources, gfs_runs, start, end, result, gate}."""
    import rrfs_grid as G
    import rrfs_verify_stations as S
    if reader is None:
        import rrfs_wind
        reader = rrfs_wind.RrfsWinds(lambda: a.base, lambda url, cap: get(url),
                                     lambda url, s, e: _ranged(url, s, e))
    if fetch_mod is None:
        sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
        import fetch as fetch_mod
    sel = getattr(a, "station_grids", "hi,pr")
    stations = S.load_stations(stations_path or os.path.join(ROOT, "wind_stations.json"), G.locate,
                               None if sel in (None, "", "all") else set(sel.split(",")))
    srcs = reader.sources(need_cells=True)
    if not srcs:
        log("no RRFS cell sources")
        return None
    start = min(datetime.strptime(s["cycle"], "%Y%m%d%H") for s in srcs)
    obs = S.observations(stations, start, now, _get_json(get), lambda url: (get(url) or b"").decode("utf-8", "replace"),
                         os.path.join(a.out, "stations"), log=log)
    have = [s for s in stations if s[0] in obs]
    rr = S.rrfs_at_stations(reader, have, G.locate, log=log)
    gf = S.gfs_at_stations(have, start, now, fetch_mod, decode or S.eccodes_decode, log=log)
    grids = {s[0]: G.locate(s[1], s[2])[0] for s in have}
    result = compare(pair(rr, obs, not a.no_height), pair(gf, obs, not a.no_height), grids)
    nets = {}
    for s in have:
        nets[s[3]] = nets.get(s[3], 0) + 1
    return {"stations": len(stations), "with_obs": len(have), "networks": ", ".join(f"{k} {v}" for k, v in sorted(nets.items())),
            "rrfs_sources": len(srcs), "gfs_runs": len({f[3] for f in gf}), "start": f"{start:%Y-%m-%d %H} UTC",
            "end": f"{now:%Y-%m-%d %H:%M} UTC", "result": result, "gate": gate(result)}


def _ranged(url, start, end):
    req = urllib.request.Request(url, headers=dict(UA, Range=f"bytes={start}-{end}"))
    with urllib.request.urlopen(req, timeout=60) as r:
        body = r.read()
        if getattr(r, "status", 206) != 206 or len(body) != end - start + 1:
            raise IOError(f"range {start}-{end} of {url}: {getattr(r, 'status', '?')} {len(body)} B")
        return body


if __name__ == "__main__":
    sys.exit(main())
