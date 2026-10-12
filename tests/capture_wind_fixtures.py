"""Capture answers of the three wind feeds for tests/test_wind_sources.py (plan section 39). Run once; the files
are committed. The whole-feed files are TRIMMED to a few stations (the real ones are 100 KB - 1 MB) with every row
shape the parsers must handle kept: a missing wind ("MM"), a calm and a variable METAR wind, a report without wind.

  latest_obs.txt        NDBC data/latest_obs/latest_obs.txt: the header lines + the rows of KEEP_NDBC
  realtime2_OOUH1.txt   NDBC data/realtime2/OOUH1.txt (Honolulu's relayed gauge): the header + the first 400 rows (~40 h)
  realtime2_51003.txt   NDBC data/realtime2/51003.txt (a buoy): the header + the first 60 rows
  coops_latest.json     CO-OPS product=wind&date=latest for 1612340
  coops_24.json         CO-OPS product=wind&range=24 for 1612340
  coops_error.json      CO-OPS's error answer (a station without wind data)
  metars.csv            the METAR cache's header + the rows of KEEP_METAR (+ one VRB and one calm report found)
  metar_history.json    api/data/metar?ids=PHNL&hours=24
  nws_obs.json          api.weather.gov stations/001HE/observations (ld+json), the newest 12 rows of the last 24 hours: a
                        gust-only row (the top of the hour), rows with speed + direction, rows with speed only
  nws_history.json      the same station's last 24 hours (limit 500), TRIMMED to its first 40 rows
  nws_stations.json     api.weather.gov stations?state=HI (ld+json), TRIMMED to one station per network
"""
import csv
import gzip
import io
import json
import os
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import wind_sources as W  # noqa: E402

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "wind")
KEEP_NDBC = ["51003", "51201", "OOUH1", "ILOH1", "HRRH1", "PLSF1", "46012", "15009"]
KEEP_METAR = ["PHNL", "PHTO", "PHJR", "PHSF", "KSFO", "EGLL"]


def get(url):
    req = urllib.request.Request(url, headers=W.HEADERS)
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read()


def save(name, data):
    with open(os.path.join(OUT, name), "wb") as f:
        f.write(data)
    print(name, len(data))


def main():
    os.makedirs(OUT, exist_ok=True)
    text = get(W.NDBC_LATEST_URL).decode("utf-8", "replace")
    lines = text.splitlines()
    head = [l for l in lines if l.startswith("#")]
    rows = [l for l in lines if l.split()[:1] and l.split()[0].upper() in KEEP_NDBC]
    save("latest_obs.txt", ("\n".join(head + rows) + "\n").encode())
    for sid, n in (("OOUH1", 400), ("51003", 60)):
        lines = get(W.NDBC_RT2_URL % sid).decode("utf-8", "replace").splitlines()
        save("realtime2_%s.txt" % sid, ("\n".join(lines[:2 + n]) + "\n").encode())
    save("coops_latest.json", get(W.coops_url("1612340", date="latest")))
    save("coops_24.json", get(W.coops_url("1612340", range=24)))
    save("coops_error.json", get(W.coops_url("9435373", date="latest")))
    raw = gzip.decompress(get(W.METAR_CACHE_URL)).decode("utf-8", "replace")
    rd = list(csv.reader(io.StringIO(raw)))
    head, body = rd[0], rd[1:]
    i_id, i_raw, i_spd = head.index("station_id"), head.index("raw_text"), head.index("wind_speed_kt")
    keep = [r for r in body if r[i_id] in KEEP_METAR]
    vrb = next((r for r in body if " VRB" in r[i_raw]), None)
    calm = next((r for r in body if " 00000KT" in r[i_raw] and r[i_id] not in KEEP_METAR), None)
    nowind = next((r for r in body if not r[i_spd].strip()), None)
    keep += [r for r in (vrb, calm, nowind) if r]
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(head)
    w.writerows(keep)
    save("metars.csv", buf.getvalue().encode())
    hist = get(W.METAR_API_URL % "PHNL")
    json.loads(hist)
    save("metar_history.json", hist)
    # NWS API (step 5c): ld+json (Accept header), the newest rows first
    since = W.iso_z(int(__import__("time").time()) - W.HISTORY_S)
    obs = json.loads(get_nws(W.NWS_OBS_URL % ("001HE", since, 12)))
    save("nws_obs.json", json.dumps(obs, indent=1).encode())
    hist = json.loads(get_nws(W.NWS_OBS_URL % ("001HE", since, W.NWS_HISTORY_LIMIT)))
    hist["@graph"] = hist["@graph"][:40]
    save("nws_history.json", json.dumps(hist, indent=1).encode())
    page = json.loads(get_nws(W.NWS_API + "/stations?state=HI&limit=500"))
    seen, keep = set(), []
    for st in page.get("@graph", []):
        key = (st.get("provider"), st.get("subProvider"), st.get("stationIdentifier", "")[-2:] == "HE")
        if key not in seen:
            seen.add(key)
            keep.append(st)
    page["@graph"] = keep
    page.pop("pagination", None)
    save("nws_stations.json", json.dumps(page, indent=1).encode())


def get_nws(url):
    req = urllib.request.Request(url, headers=W.NWS_HEADERS)
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read()


if __name__ == "__main__":
    main()
