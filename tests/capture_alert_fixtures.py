"""Captures tests/fixtures/alerts/coastal_active.json: one real answer of the NWS coastal alerts (alert_sources.URL) cut
to one alert per product and office kind (geometry dropped, texts kept). Run by hand: python -I tests/capture_alert_fixtures.py
[--from saved.json]. Reaches api.weather.gov once without --from."""
import argparse
import json
import os
import sys
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import alert_sources  # noqa: E402

KEEP = ("@id", "id", "areaDesc", "geocode", "affectedZones", "references", "sent", "effective", "onset", "expires",
        "ends", "status", "messageType", "category", "severity", "event", "senderName", "headline", "description",
        "instruction", "parameters")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="src")
    a = ap.parse_args()
    if a.src:
        with open(a.src, encoding="utf-8") as f:
            doc = json.load(f)
    else:
        req = urllib.request.Request(alert_sources.URL, headers=alert_sources.HEADERS)
        doc = json.loads(urllib.request.urlopen(req, timeout=30).read())
    seen, feats = set(), []
    for f in doc["features"]:
        p = f["properties"]
        key = (p["event"], p["messageType"])
        if key in seen:
            continue
        seen.add(key)
        params = {k: v for k, v in (p.get("parameters") or {}).items() if k in ("NWSheadline",)}
        feats.append({"id": f.get("id"), "type": "Feature", "geometry": None,
                      "properties": dict({k: p.get(k) for k in KEEP if k in p}, parameters=params)})
    out = {"type": "FeatureCollection", "features": feats}
    path = os.path.join(HERE, "fixtures", "alerts", "coastal_active.json")
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(out, f, indent=1, sort_keys=True)
        f.write("\n")
    print(path, len(feats), "alerts", sorted(seen))


if __name__ == "__main__":
    main()
