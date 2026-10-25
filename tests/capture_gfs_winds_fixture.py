"""Capture tests/fixtures/gfswinds/gfswave_13130_member.spec: the first member of a real GFS-Wave spectra archive
(gfswave.tHHz.spec_tar.gz on NOAA's AWS copy), cut after its first 40 steps, for the parity test of the winds job's
parser with the site's (plan section 53). Reads only the archive's head (one member, ~8 MB inflated).

  python tests/capture_gfs_winds_fixture.py 2026102512
"""
import gzip
import os
import re
import sys
import tarfile
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "fixtures", "gfswinds", "gfswave_13130_member.spec")
STEPS = 40


def main(run):
    url = (f"https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.{run[:8]}/{run[8:]}/wave/station/"
           f"gfswave.t{run[8:]}z.spec_tar.gz")
    req = urllib.request.Request(url, headers={"User-Agent": "allshoresurf.com test fixture"})
    with urllib.request.urlopen(req, timeout=120) as r:
        tf = tarfile.open(fileobj=gzip.GzipFile(fileobj=r), mode="r|")
        m = next(iter(tf))
        assert m.name == "./gfswave.13130.spec", m.name
        data = tf.extractfile(m).read()
    times = [x.start() for x in re.finditer(rb"^\d{8} \d{6}\s*$", data, re.M)]
    cut = data[:times[STEPS]] if len(times) > STEPS else data
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "wb") as f:
        f.write(cut)
    print(f"{OUT}: {len(cut)} B, {min(len(times), STEPS)} steps of run {run}")


if __name__ == "__main__":
    main(sys.argv[1])
