"""One-off: the radar job's fixtures (plan section 85). Run by hand with eccodes and pyproj importable; the tests need neither.

  python -I tests/capture_radar_fixtures.py <folder holding HAWAII.grib2.gz>

Writes tests/fixtures/radar/HAWAII.grib2.gz (a real MRMS file) and expect.json: values at sampled points read by eccodes, an
independent GRIB decoder, and the OPERA grid's projection computed by pyproj, an independent projection library.
"""
import json
import os
import shutil
import sys

import eccodes
import numpy as np
import pyproj

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "tests", "fixtures", "radar")


def main(src):
    os.makedirs(OUT, exist_ok=True)
    gz = os.path.join(src, "HAWAII.grib2.gz")
    shutil.copyfile(gz, os.path.join(OUT, "HAWAII.grib2.gz"))
    import gzip
    g = eccodes.codes_new_from_message(gzip.decompress(open(gz, "rb").read()))
    ni, nj = eccodes.codes_get(g, "Ni"), eccodes.codes_get(g, "Nj")
    vals = eccodes.codes_get_values(g).reshape(nj, ni)
    eccodes.codes_release(g)
    rng = np.random.default_rng(85)
    echo = np.argwhere(vals >= 5)
    picks = [tuple(x) for x in echo[rng.integers(0, len(echo), 60)]] + [tuple(x) for x in rng.integers(0, [nj, ni], (60, 2))]
    samples = [[int(r), int(c), round(float(vals[r, c]), 1)] for r, c in picks]
    crs = pyproj.Transformer.from_crs("EPSG:4326", "+proj=laea +lat_0=55 +lon_0=10 +x_0=1950000 +y_0=-2100000 +units=m +ellps=WGS84", always_xy=True)
    pts = [(38.7, -9.1), (51.5, -0.1), (60.2, 25.0), (34.5, 24.0), (70.0, 25.0), (31.8, -10.4), (67.0, -39.5), (55.0, 10.0), (41.9, 12.5), (64.1, -21.9)]
    proj = [[lat, lon, *[round(v, 3) for v in crs.transform(lon, lat)]] for lat, lon in pts]
    with open(os.path.join(OUT, "expect.json"), "w", encoding="utf-8") as fh:
        json.dump({"hawaii_samples": samples, "opera_forward": proj, "stats": {"nocov": float((vals < -900).mean()), "ge5": float((vals >= 5).mean())}}, fh, indent=0)
    print("samples", len(samples), "echo", len(echo))


if __name__ == "__main__":
    main(sys.argv[1])
