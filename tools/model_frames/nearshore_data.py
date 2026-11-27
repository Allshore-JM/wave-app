"""Builds the nearshore job's data folder (plan section 109): tides.json and manifest.json.
The folder is committed to the data branch and the job checks it out at a pinned commit.

    python tools/model_frames/nearshore_data.py <out folder> <version> --tables <the table set's manifest.json>
                                                [--tides <an existing tides.json>]

- tides.json     NOAA CO-OPS predicted highs and lows at Haleiwa (1612668, MLLW), YEARS years from this year.
- manifest.json  version, the table set and the sha256 of ITS manifest (static/nearshore/<set>/manifest.json in the
                 bucket, written last by the nearshore-table workflow: it pins every region's table), and the sha256 of
                 tides.json. The job refuses a folder, a table manifest or a table that does not match.

The tables themselves are not in the folder: the job reads them from the bucket (tools/model_frames/nearshore.py)."""
import argparse
import hashlib
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

STATION = "1612668"
YEARS = 3
TABLES_FORMAT = "nearshore-tables-v1"
COOPS = ("https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?product=predictions&application=allshoresurf"
         "&begin_date={b}&end_date={e}&datum=MLLW&station={s}&time_zone=gmt&units=metric&interval=hilo&format=json")


def fetch_tides(start_year, years=YEARS, station=STATION, opener=urllib.request.urlopen):
    hilo = []
    for y in range(start_year, start_year + years):
        with opener(COOPS.format(b=f"{y}0101", e=f"{y}1231", s=station), timeout=60) as r:
            doc = json.load(r)
        for p in doc["predictions"]:
            t = datetime.strptime(p["t"], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp()
            hilo.append([int(t), round(float(p["v"]), 3)])
    hilo.sort()
    if len(hilo) < 1300 * years:
        raise ValueError(f"only {len(hilo)} highs and lows for {years} years")
    return {"station": station, "datum": "MLLW", "msl_minus_mllw": 0.2835, "source": "NOAA CO-OPS predictions (hilo)", "hilo": hilo}


def check_tables(raw):
    """The table set's manifest (bytes) -> (set, sha256, region ids); refuses anything that is not a whole one."""
    doc = json.loads(raw)
    regions = doc.get("regions")
    if doc.get("format") != TABLES_FORMAT or not isinstance(doc.get("set"), str) or not isinstance(regions, list) or not regions:
        raise ValueError("not a table set's manifest")
    for r in regions:
        if not isinstance(r.get("id"), str) or not isinstance(r.get("table"), str) or len(str(r.get("sha256", ""))) != 64:
            raise ValueError(f"region {r.get('id')!r}: no table or no sha256")
    return doc["set"], hashlib.sha256(raw).hexdigest(), [r["id"] for r in regions]


def build(out, version, tables_raw, tides, now=None):
    """Writes tides.json and manifest.json into out -> the manifest."""
    tset, sha, regions = check_tables(tables_raw)
    os.makedirs(out, exist_ok=True)
    body = json.dumps(tides, separators=(",", ":")).encode("utf-8")
    with open(os.path.join(out, "tides.json"), "wb") as f:
        f.write(body)
    man = {"version": version, "tables": {"set": tset, "manifest_sha256": sha, "regions": regions},
           "sha256": {"tides.json": hashlib.sha256(body).hexdigest()},
           "built_utc": (now or datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")}
    with open(os.path.join(out, "manifest.json"), "w", encoding="utf-8", newline="\n") as f:
        json.dump(man, f, indent=1)
    return man


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("out"); ap.add_argument("version")
    ap.add_argument("--tables", required=True, help="the table set's manifest.json, as the bucket holds it (byte for byte)")
    ap.add_argument("--tides", help="an existing tides.json to reuse instead of asking NOAA")
    a = ap.parse_args(argv)
    with open(a.tables, "rb") as f:
        raw = f.read()
    if a.tides:
        with open(a.tides, encoding="utf-8") as f:
            tides = json.load(f)
    else:
        tides = fetch_tides(datetime.now(timezone.utc).year)
    man = build(a.out, a.version, raw, tides)
    print(f"tides: {len(tides['hilo'])} highs and lows; tables: set {man['tables']['set']} ({', '.join(man['tables']['regions'])}) "
          f"manifest sha256 {man['tables']['manifest_sha256']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
