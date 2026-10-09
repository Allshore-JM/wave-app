"""Build tide_stations.json: NOAA CO-OPS tide-prediction stations for the map's tide layer (plan section 38).

Per station: id, name, lat, lon, type (R harmonic / S subordinate), ref (a subordinate's reference station), tz (the
nearest civil zone, the live buoys' rule), obs (the station also reports its water level), oh / ol (a subordinate's
published time offsets in minutes for high and low tide: they pair its extremes with its reference's).

Sources (public domain): mdapi stations.json?type=tidepredictions, ?type=waterlevels, and one tidepredoffsets.json per
subordinate station (~2,250 small requests, four at a time; --offsets-cache keeps them between runs).

Run:  python tools/tides/fetch_stations.py            (writes ../../tide_stations.json)
NOAA's list changes rarely; the site never asks mdapi at runtime, so re-run it now and then and commit the file."""
import argparse
import json
import os
import re
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
OUT = os.path.join(ROOT, "tide_stations.json")
MD = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi"
FIELDS = ["id", "name", "lat", "lon", "type", "ref", "tz", "obs", "oh", "ol"]
KEEP_UPPER = {"ICWW", "ICW", "USCG", "NOAA", "US", "USS", "RR", "AFB", "NAS", "NAB", "MSF", "PGA", "GPS", "GNSS", "NERR",
              "ANVSA", "MARAD", "CBBT", "LAWMA", "II", "III", "IV", "SW", "SE", "NW", "NE"}
# NOAA's metadata has a few positions wrong (G27 A-F4: a longitude's sign, or a latitude): the station's real place,
# from which its time zone follows. Checked against charts / the place's known coordinates.
POSITION_FIX = {
    "TPT2891": (-19.03333, -169.91667),     # Niue Island: NOAA +169.92 (Vanuatu's waters); Niue is 169.9 W, UTC-11
    "TPT2893": (-18.65, -173.98333),        # Neiafu, Vava'u (Tonga): NOAA +6.017 (= 180 - 173.983)
    "TPT2897": (-20.26667, -174.79),        # Nomuka (Tonga): NOAA +174.79 (Fiji's waters)
    "TWC0279": (1.25, -78.833),             # San Lorenzo (Ecuador): NOAA +78.833 (the Indian Ocean)
    "6835001": (-6.1, 106.86667),           # Djakarta (Tanjung Priok, 6 06 S): NOAA -2.20 (430 km out in the Java Sea)
}
# Stations whose derived zone differs from NOAA's own `timezonecorr` by more than ZONE_TOL_H because NOAA's value is
# stale or on the other side of the date line (the same clock, a day apart): Apia, Kiribati, Tonga, Kanton, Raoul,
# Easter Island. Each is listed with the zone it MUST have, so a lost or wrong position (two are in POSITION_FIX) still
# stops the build (G27 re-check RC-12). Any OTHER station differing that much stops it too: look at its position first.
KNOWN_ZONE_DIFF = {
    "1778000": "Pacific/Apia", "1814060": "Pacific/Kiritimati", "TPT2743": "Pacific/Kiritimati",
    "TPT2819": "Pacific/Easter", "TPT2821": "Pacific/Kiritimati", "TPT2855": "Pacific/Kanton",
    "TPT2859": "Pacific/Apia", "TPT2893": "Pacific/Tongatapu", "TPT2895": "Pacific/Tongatapu",
    "TPT2897": "Pacific/Tongatapu", "TPT2899": "Pacific/Tongatapu", "TPT2905": "Pacific/Auckland",
}
ZONE_TOL_H = 3


def get(url, tries=3):
    for k in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                return json.load(r)
        except Exception:
            if k == tries - 1:
                raise
            time.sleep(1 + 2 * k)


def _caps_word(w):
    """A word written in capitals, without digits ("HONOLULU", "O'CONNOR,", "ST.CROIX", "(BREAKWATER")."""
    return any(c.isalpha() for c in w) and not any(c.islower() or c.isdigit() for c in w)


def _title(w):
    """Each run of capitals in a word capitalised ("O'CONNOR" -> "O'Connor", "ST.CROIX" -> "St.Croix"), abbreviations
    (KEEP_UPPER) and single letters kept, an "S" after an apostrophe lowered ("MARTHA'S" -> "Martha's")."""
    def one(m):
        run = m.group(0)
        if run in KEEP_UPPER:
            return run
        if len(run) == 1:
            return run.lower() if run == "S" and m.start() > 0 and w[m.start() - 1] == "'" else run
        return run.capitalize()
    return re.sub(r"[A-Z]+", one, w)


def clean_name(name):
    """NOAA writes the main stations' names in capitals, wholly ("HONOLULU", "CBBT, CHESAPEAKE CHANNEL") or their
    leading place ("PAGO PAGO Harbor, Tutuila Island", "CHUUK, Moen Island", "NEW YORK (The Battery)"): those capitals
    are converted (abbreviations kept: ICWW, USCG, CBBT ...). A capital word inside a mixed name is an abbreviation and
    stays ("Martha's Vineyard GPS Buoy", "PGA Boulevard Bridge", "Fort Eustis (MARAD)"). G27 A-F6."""
    name = " ".join(str(name or "").split())
    words = name.split(" ") if name else []
    if not any(c.islower() for c in name):
        n = len(words)                                         # the whole name is in capitals
    else:
        n = 0                                                  # the leading run of capital words: the place's name
        while n < len(words) and _caps_word(words[n]):
            n += 1
        ends_segment = n > 0 and (n == len(words) or words[n - 1][-1] in ",)" or words[n].startswith("("))
        if not (n >= 2 or ends_segment):
            n = 0                                              # one capital word inside a phrase: an abbreviation
    return " ".join([_title(w) for w in words[:n]] + words[n:])


def std_offset_h(zone):
    """A zone's standard UTC offset in hours (the smaller of January's and July's)."""
    z = ZoneInfo(zone)
    return min(z.utcoffset(datetime(2026, m, 15)).total_seconds() / 3600 for m in (1, 7))


def zone_mismatches(rows, corr):
    """Stations whose zone is wrong: a KNOWN_ZONE_DIFF station without the zone listed for it, any other station whose
    zone differs from NOAA's timezonecorr by more than ZONE_TOL_H: [(id, name, zone, corr)]. rows: the snapshot's rows;
    corr: {id: NOAA's timezonecorr}."""
    out = []
    for r in rows:
        sid, zone = r[0], r[6]
        if sid in KNOWN_ZONE_DIFF:
            if zone != KNOWN_ZONE_DIFF[sid]:
                out.append((sid, r[1], zone, KNOWN_ZONE_DIFF[sid]))
            continue
        try:
            c = float(corr.get(sid))
        except (TypeError, ValueError):
            continue
        if abs(std_offset_h(zone) - c) > ZONE_TOL_H:
            out.append((sid, r[1], zone, c))
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=OUT)
    ap.add_argument("--offsets-cache", default=None, help="JSON file keeping the per-station offsets between runs")
    args = ap.parse_args(argv)

    os.environ.setdefault("LIVE_BACKGROUND", "0")
    sys.path.insert(0, ROOT)
    import app as A                                            # the live buoys' nearest-civil-zone rule

    preds = get(MD + "/stations.json?type=tidepredictions")["stations"]
    gauges = {s["id"] for s in get(MD + "/stations.json?type=waterlevels")["stations"]}
    print(f"{len(preds)} prediction stations, {len(gauges)} water-level stations", flush=True)

    cache = {}
    if args.offsets_cache and os.path.exists(args.offsets_cache):
        cache = json.load(open(args.offsets_cache))
    todo = [s["id"] for s in preds if s.get("type") == "S" and s["id"] not in cache]

    def offsets(sid):
        d = get(f"{MD}/stations/{sid}/tidepredoffsets.json")
        time.sleep(0.1)
        return sid, [d.get("timeOffsetHighTide"), d.get("timeOffsetLowTide")]

    with ThreadPoolExecutor(4) as pool:
        for n, (sid, off) in enumerate(pool.map(offsets, todo), 1):
            cache[sid] = off
            if n % 200 == 0:
                print(f"  offsets {n}/{len(todo)}", flush=True)
                if args.offsets_cache:
                    json.dump(cache, open(args.offsets_cache, "w"))
    if args.offsets_cache:
        json.dump(cache, open(args.offsets_cache, "w"))

    ids = {s["id"] for s in preds}
    rows, dropped = [], []
    for s in sorted(preds, key=lambda s: s["id"]):
        if s.get("type") == "S" and (s.get("reference_id") in (None, "", s["id"]) or s.get("reference_id") not in ids):
            dropped.append(s["id"])                            # NOAA answers nothing for these (Malakal Harbor: its own ref)
            continue
        lat, lon = POSITION_FIX.get(s["id"], (round(float(s["lat"]), 5), round(float(s["lng"]), 5)))
        kind = "S" if s.get("type") == "S" else "R"
        oh, ol = cache.get(s["id"], [None, None]) if kind == "S" else (None, None)
        rows.append([s["id"], clean_name(s["name"]), lat, lon, kind, (s.get("reference_id") or None) if kind == "S" else None,
                     A._nearest_civil_tz(lat, lon), s["id"] in gauges, oh, ol])
    bad = zone_mismatches(rows, {s["id"]: s.get("timezonecorr") for s in preds})
    if bad:                                                    # a position NOAA has wrong (A-F4): fix it or list it
        print("ZONE CHECK FAILED (derived zone vs NOAA's timezonecorr, > %d h):" % ZONE_TOL_H, file=sys.stderr)
        for b in bad:
            print("  %s %s: %s vs %s" % (b[0], b[1], b[2], b[3] if isinstance(b[3], str) else "NOAA %+g h" % b[3]),
                  file=sys.stderr)
        return 2
    doc = {"source": "NOAA CO-OPS metadata API (tidesandcurrents.noaa.gov), public domain",
           "captured": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "fields": FIELDS, "stations": rows}
    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        f.write('{"source":%s,"captured":%s,"fields":%s,"stations":[\n' % (
            json.dumps(doc["source"]), json.dumps(doc["captured"]), json.dumps(FIELDS)))
        f.write(",\n".join(json.dumps(r, ensure_ascii=False, separators=(",", ":")) for r in rows))
        f.write("\n]}\n")
    r = sum(1 for x in rows if x[4] == "R")
    print(f"wrote {args.out}: {len(rows)} stations ({r} R, {len(rows) - r} S), "
          f"{sum(1 for x in rows if x[7])} with observations; left out (no valid reference): {dropped}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
