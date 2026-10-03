"""Capture one AODN buoy's real spectra answers for tests/test_aodn_spectra.py (run once, by hand):

    python tests/capture_aodn_spectra_fixture.py ["Wilsons Prom"]

Records the THREDDS answers AODNProvider.spectrum() asks for (.dds and the seven .ascii projections, keyed by the
part after the file name so a replay works in any month) and the buoy's own reported peak direction from the map
layer at the same times (the check: the spectra's peak-band direction must equal it)."""
import json
import os
import sys

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import buoy_sources as B  # noqa: E402

FIXTURE = os.path.join(HERE, "fixtures", "aodn_spectra.json")


def key(url):
    """'.dds' or '.ascii?<projection>' (the part after '..._monthly.nc')."""
    return url.split("_monthly.nc", 1)[1]


def main(site):
    real = requests.Session()
    real.headers["User-Agent"] = "allshore-wave-app/1.0 (test fixture)"
    bodies = {}

    class Recorder:
        def get(self, url, **kw):
            r = real.get(url, timeout=120)
            if url.startswith(B.AODNProvider.THREDDS):
                bodies[key(url)] = r.text
            return r

    p = B.AODNProvider(http=Recorder())
    spec = p.spectrum(site)
    if not spec:
        raise SystemExit("no spectra for %s" % site)
    ix, bysite = B.AODNProvider()._map_rows()
    times = {s["time_utc"][:16] for s in spec["steps"] if s["time_utc"]}
    bulk = {}
    for r in bysite[site]:
        t = r[ix["TIME"]][:16]
        if t in times and r[ix["peak_wave_direction"]] not in ("", "NaN"):
            bulk[t] = float(r[ix["peak_wave_direction"]])
    json.dump({"site": site, "bodies": bodies, "peak_direction_from": bulk}, open(FIXTURE, "w", encoding="utf-8"),
              indent=1, sort_keys=True)
    print("wrote", FIXTURE, len(bodies), "answers,", len(bulk), "of", len(times), "times with the buoy's peak direction")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "Wilsons Prom")
