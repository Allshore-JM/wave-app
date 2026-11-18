"""The RRFS gate at the site's wind stations (plan section 99): RRFS's own cells and GFS's 10-m wind at each land /
coastal wind station inside RRFS's grids, against that station's observations (tools/rrfs_verify.py covers the
forecast stations; most Hawaii forecast stations are wave-only buoys, so this adds Hawaii's ~300 wind stations).

- Stations: wind_stations.json rows inside a fine grid (rrfs_grid.locate), networks NWS API, METAR, CO-OPS and NDBC
  (IEM road sensors and ECCC are left out).
- Observations, read-only and paced: NWS API /stations/{id}/observations (km/h), aviationweather.gov METARs (knots,
  bulk), CO-OPS wind (m/s), NDBC realtime2 (m/s; buoys scaled to 10 m like the forecast-station gate). Land stations
  are taken as measured (airports and most mesonets are near 10 m).
- RRFS: the cell series of every source the site keeps cell files for (the newest 2 runs and 3 hourly cycles), read
  like the site does (rrfs_wind, one ranged GET per station and source): short leads only, since older sources' cell
  files are pruned.
- GFS: the 0.25-deg 10-m U/V of the GFS runs whose steps fall in the observed window, from NOAA's S3 copy by byte range
  (tools/model_frames/fetch.py, the frames job's reader), bilinear at the station. eccodes needed.

Pairs and statistics are tools/rrfs_verify.py's (matched station-hours, lead bands). Called by rrfs_verify.py --stations.
"""
import json
import math
import os
import time
import urllib.parse
from datetime import datetime, timedelta, timezone

import rrfs_verify as V

NETWORKS = ("nws", "metar", "coops", "ndbc")
NWS_API = "https://api.weather.gov"
METAR_API = "https://aviationweather.gov/api/data/metar"
COOPS_API = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
KT = 0.514444
KMH = 1 / 3.6
METAR_BATCH = 80


def load_stations(path, locate, grids=None):
    """[(sid, lat, lon, src, net_id)] of wind_stations.json inside a fine grid (net_id: the id the network knows);
    `grids`: only those grids (None = all four)."""
    with open(path, encoding="utf-8-sig") as f:
        doc = json.load(f)
    out = []
    for row in doc["stations"]:
        sid, _name, lat, lon, _kind, src = row[:6]
        hit = locate(lat, lon) if src in NETWORKS else None
        if hit is None or (grids and hit[0] not in grids):
            continue
        out.append((sid, float(lat), float(lon), src, sid.split(":", 1)[1]))
    return out


# ------------------------------------------------------------------------------------------------------ observations

def _iso(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)


def parse_nws(doc):
    out = {}
    for f in doc.get("features") or []:
        p = f.get("properties") or {}
        sp = (p.get("windSpeed") or {}).get("value")
        if sp is None or not p.get("timestamp"):
            continue
        d = (p.get("windDirection") or {}).get("value")
        g = (p.get("windGust") or {}).get("value")
        out[_iso(p["timestamp"]).replace(second=0, microsecond=0)] = (sp * KMH, d, None if g is None else g * KMH)
    return out


def parse_metar(rows):
    out = {}
    for r in rows or []:
        if r.get("wspd") is None or r.get("obsTime") is None:
            continue
        t = datetime.fromtimestamp(int(r["obsTime"]), timezone.utc).replace(tzinfo=None, second=0)
        d = r.get("wdir")
        d = d if isinstance(d, (int, float)) else None                  # "VRB" = no direction
        g = r.get("wgst")
        out.setdefault(r.get("icaoId"), {})[t] = (r["wspd"] * KT, d, None if g is None else g * KT)
    return out


def parse_coops(doc):
    out = {}
    for r in doc.get("data") or []:
        try:
            t = datetime.strptime(r["t"], "%Y-%m-%d %H:%M")
            s = float(r["s"])
        except (KeyError, ValueError):
            continue
        num = lambda v: float(v) if v not in (None, "") else None     # noqa: E731
        out[t] = (s, num(r.get("d")), num(r.get("g")))
    return out


def observations(stations, start, now, get_json, get_text, cache_dir, log=print, pace=V.PACE_S):
    """{sid: {time: (speed m/s, dir, gust)}} from each network (NWS / CO-OPS per station, METAR in batches,
    NDBC via rrfs_verify.parse_ndbc; cached in cache_dir for 6 h)."""
    os.makedirs(cache_dir, exist_ok=True)
    out = {}

    def cached(name, fetch):
        path = os.path.join(cache_dir, name)
        if os.path.exists(path) and time.time() - os.path.getmtime(path) < 6 * 3600:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        val = fetch()
        with open(path, "w", encoding="utf-8") as f:
            json.dump(val, f)
        time.sleep(pace)
        return val

    by = {}
    for st in stations:
        by.setdefault(st[3], []).append(st)
    for sid, _la, _lo, _src, nid in by.get("nws", []):
        def fetch(nid=nid):
            url = f"{NWS_API}/stations/{urllib.parse.quote(nid)}/observations?start={start:%Y-%m-%dT%H:%M:%SZ}"
            feats = []
            for _page in range(6):
                doc = get_json(url) or {}
                feats += doc.get("features") or []
                nxt = (doc.get("pagination") or {}).get("next")
                if not nxt or not doc.get("features"):
                    break
                url = nxt
            return {"features": feats}
        obs = parse_nws(cached(f"nws_{nid}.json", fetch))
        if obs:
            out[sid] = obs
    metars = by.get("metar", [])
    hours = int((now - start).total_seconds() // 3600) + 1
    for k in range(0, len(metars), METAR_BATCH):
        ids = ",".join(m[4] for m in metars[k:k + METAR_BATCH])
        rows = cached(f"metar_{k}.json", lambda ids=ids: get_json(f"{METAR_API}?ids={ids}&hours={hours}&format=json") or [])
        got = parse_metar(rows)
        for sid, _la, _lo, _src, nid in metars[k:k + METAR_BATCH]:
            if got.get(nid):
                out[sid] = got[nid]
    for sid, _la, _lo, _src, nid in by.get("coops", []):
        url = (f"{COOPS_API}?product=wind&station={nid}&begin_date={start:%Y%m%d}&end_date={now:%Y%m%d}"
               f"&time_zone=gmt&units=metric&format=json&application=allshoresurf")
        obs = parse_coops(cached(f"coops_{nid}.json", lambda url=url: get_json(url) or {}))
        if obs:
            out[sid] = obs
    for sid, _la, _lo, _src, nid in by.get("ndbc", []):
        text = cached(f"ndbc_{nid}.json", lambda nid=nid: get_text(f"{V.NDBC}/{nid}.txt") or "")
        obs = V.parse_ndbc(text)
        if obs:
            out[sid] = obs
    log(f"wind stations: {len(out)} of {len(stations)} have observations "
        f"({', '.join(f'{n} {sum(1 for s in by.get(n, []) if s[0] in out)}' for n in NETWORKS)})")
    return out


# ---------------------------------------------------------------------------------------------------- the forecasts

def rrfs_at_stations(reader, stations, locate, log=print, workers=8):
    """[(sid, valid, lead, cycle, speed, dir, gust)] from every kept RRFS cell source at each station's cell (the
    ranged reads `workers` at a time)."""
    from concurrent.futures import ThreadPoolExecutor
    out = []
    srcs = reader.sources(need_cells=True)
    jobs = []
    for sid, lat, lon, _src, _nid in stations:
        hit = locate(lat, lon)
        if hit is None:
            continue
        grid, j, i = hit
        jobs += [(sid, src, grid, j, i) for src in srcs if grid in (src.get("cells") or {})]

    def read(job):
        sid, src, grid, j, i = job
        try:
            return job, reader._cell_series(src, grid, j, i)
        except Exception as exc:                     # noqa: BLE001
            log(f"RRFS {src['cycle']} {grid} at {sid}: {exc}")
            return job, None

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(read, jobs))
    for (sid, src, _grid, _j, _i), got in results:
        if got is None:
            continue
        steps, arr = got
        base = datetime.strptime(src["cycle"], "%Y%m%d%H")
        for h, r in zip(steps, arr):
            if r["spd"] == V.MISSING:
                continue
            out.append((sid, base + timedelta(hours=int(h)), int(h), src["cycle"], r["spd"] / 100,
                        None if r["dir"] == V.MISSING else r["dir"] / 10,
                        None if r["gust"] == V.MISSING else r["gust"] / 100))
    log(f"RRFS at wind stations: {len(out)} forecasts from {len(srcs)} sources")
    return out


def bilinear(grid, lat, lon):
    """Bilinear value of a 0.25-deg global field (721 x 1440, row 0 = 90 N, column 0 = 0 E) at lat/lon."""
    fy = (90.0 - lat) / 0.25
    fx = (lon % 360.0) / 0.25
    y0, x0 = int(math.floor(fy)), int(math.floor(fx))
    y1, x1 = min(y0 + 1, 720), (x0 + 1) % 1440
    wy, wx = fy - y0, fx - x0
    return ((1 - wy) * ((1 - wx) * grid[y0][x0] + wx * grid[y0][x1]) +
            wy * ((1 - wx) * grid[y1][x0] + wx * grid[y1][x1]))


def gfs_at_stations(stations, start, now, fetch_mod, decode, log=print, max_lead=84):
    """[(sid, valid, lead, run, speed, dir, None)] of GFS's 10-m wind at the stations: every 6-h run from start - 84 h on
    whose steps are valid in [start, now] (S3 by byte range; a step S3 does not have is left out)."""
    out = []
    keys = ["UGRD:10 m above ground", "VGRD:10 m above ground"]
    t = start - timedelta(hours=max_lead)
    t = t.replace(hour=t.hour // 6 * 6, minute=0, second=0, microsecond=0)
    runs = 0
    while t <= now:
        run_dt = t.replace(tzinfo=timezone.utc)
        for h in fetch_mod.STEPS:
            valid = t + timedelta(hours=h)
            if h > max_lead or valid > now:
                break
            if valid < start:
                continue
            try:
                rec = fetch_mod.fetch_records(fetch_mod.atmos_url(run_dt, h), keys)
            except Exception as exc:                 # noqa: BLE001 - a step not on S3: left out
                log(f"GFS {t:%Y%m%d%H} f{h:03d}: {exc.__class__.__name__}")
                continue
            u, v = decode(rec[keys[0]]), decode(rec[keys[1]])
            for sid, lat, lon, _src, _nid in stations:
                uu, vv = bilinear(u, lat, lon), bilinear(v, lat, lon)
                out.append((sid, valid, h, f"{t:%Y%m%d%H}", math.hypot(uu, vv),
                            (math.degrees(math.atan2(-uu, -vv)) + 360) % 360, None))
        runs += 1
        t += timedelta(hours=6)
    log(f"GFS at wind stations: {len(out)} forecasts from {runs} runs")
    return out


def eccodes_decode(msg):
    import eccodes as ec
    g = ec.codes_new_from_message(msg)
    try:
        ny, nx = ec.codes_get(g, "Nj"), ec.codes_get(g, "Ni")
        if (ny, nx) != (721, 1440) or ec.codes_get(g, "jScansPositively") != 0:
            raise ValueError("not the 0.25-deg global grid scanned north to south")
        vals = ec.codes_get_double_array(g, "values")
        return [vals[r * nx:(r + 1) * nx] for r in range(ny)]
    finally:
        ec.codes_release(g)
