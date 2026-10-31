"""Capture the Taiwan CWA fixtures of plan section 69 from CWA's keyless copies (no key needed; run by hand):

    python -I tests/capture_keyed_fixtures.py

tests/fixtures/keyed/cwa_wind.json        O-A0003-001 cut to a few stations, in the REST answer's wrapper (records)
tests/fixtures/keyed/cwa_buoy_stations.json  O-B0076-001 cut to a few stations (two with a wave sensor, one tide gauge)
tests/fixtures/keyed/cwa_buoy_obs.json    O-B0075-001 (the ZIP of one XML file per station) for two buoys, 30 hours,
                                          turned into the REST answer's JSON shape (records.SeaSurfaceObs.Location)
The CCO, KNMI and Frost answers need a key: their tests build answers from the documented shapes."""
import io
import json
import os
import sys
import urllib.request
import xml.etree.ElementTree as ET
import zipfile

S3 = "https://cwaopendata.s3.ap-northeast-1.amazonaws.com/Observation/"
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "keyed")
WIND_IDS = ("466940", "467410", "C0A520", "C0K330", "467490")
BUOY_IDS = ("46694A", "46761F")
NS = "{urn:cwa:gov:tw:cwacommon:0.1}"


def get(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "allshoresurf.com tests"}), timeout=60) as r:
        return r.read()


def strip(tag):
    return tag.split("}", 1)[-1]


def to_json(el):
    kids = list(el)
    if not kids:
        return el.text
    out = {}
    for k in kids:
        name, val = strip(k.tag), to_json(k)
        if name in out:
            out[name] = out[name] if isinstance(out[name], list) else [out[name]]
            out[name].append(val)
        else:
            out[name] = val
    return out


def main():
    os.makedirs(OUT, exist_ok=True)
    wind = json.loads(get(S3 + "O-A0003-001.json"))
    st = [s for s in wind["cwaopendata"]["dataset"]["Station"] if s["StationId"] in WIND_IDS]
    json.dump({"success": "true", "records": {"Station": st}}, open(os.path.join(OUT, "cwa_wind.json"), "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    sta = json.loads(get(S3 + "O-B0076-001.json"))
    locs = sta["cwaopendata"]["Resources"]["Resource"]["Data"]["SeaSurfaceObs"]["Location"]
    keep = [l for l in locs if l["Station"]["StationID"] in BUOY_IDS]
    keep += [l for l in locs if "WaveHeight" not in str((l.get("StationObsStatus") or {}).get("ObservedPropertyNames"))][:1]
    json.dump({"success": "true", "records": {"SeaSurfaceObs": {"Location": keep}}},
              open(os.path.join(OUT, "cwa_buoy_stations.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    z = zipfile.ZipFile(io.BytesIO(get(S3 + "O-B0075-001.zip")))
    out = []
    for sid in BUOY_IDS:
        root = ET.fromstring(z.read(sid + ".xml"))
        loc = root.find(".//" + NS + "Location")
        d = to_json(loc)
        times = d["StationObsTimes"]["StationObsTime"]
        d["StationObsTimes"]["StationObsTime"] = (times if isinstance(times, list) else [times])[:30]
        out.append(d)
    json.dump({"success": "true", "records": {"SeaSurfaceObs": {"Location": out}}},
              open(os.path.join(OUT, "cwa_buoy_obs.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print("wrote", OUT)
    return 0


if __name__ == "__main__":
    sys.exit(main())
