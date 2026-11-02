"""Capture tests/fixtures/gfswinds/gfswave_13130_member.spec: the first member of a real GFS-Wave spectra archive
(gfswave.tHHz.spec_tar.gz on NOAA's AWS copy), cut after its first 40 steps, for the parity test of the winds job's
parser with the site's (plan section 53). Reads only the archive's head (one member, ~8 MB inflated).

  python tests/capture_gfs_winds_fixture.py 2026102512

Section 77 added two more fixtures:
  python tests/capture_gfs_winds_fixture.py 2026110112 --station 51201 --steps 6
      -> gfswave_51201_member6.spec: the header + 6 steps of that station's per-station .spec on NOMADS (one ranged
         read of 121,806 bytes: the header is 984 B, a step 20,137 B).
  python tests/capture_gfs_winds_fixture.py --expect
      -> gfsspec_expect.json: per fixture step the bulletin's Hst (NOAA's own bulletin, read from the AWS run archive
         by the site's gfs_bulletins reader) and the rows partition_v2 gives (a regression pin: re-made on purpose only).
"""
import gzip
import json
import os
import re
import sys
import tarfile
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
FIX = os.path.join(HERE, "fixtures", "gfswinds")
OUT = os.path.join(FIX, "gfswave_13130_member.spec")
STEPS = 40
SPEC_HEAD, SPEC_BLOCK = 984, 20137
NOMADS = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"
UA = {"User-Agent": "allshoresurf.com test fixture"}


def station_cut(run, sid, steps):
    url = f"{NOMADS}/gfs.{run[:8]}/{run[8:]}/wave/station/bulls.t{run[8:]}z/gfswave.{sid}.spec"
    req = urllib.request.Request(url, headers=dict(UA, Range=f"bytes=0-{SPEC_HEAD + steps * SPEC_BLOCK - 1}"))
    with urllib.request.urlopen(req, timeout=120) as r:
        data = r.read()
    out = os.path.join(FIX, f"gfswave_{sid}_member{steps}.spec")
    with open(out, "wb") as f:
        f.write(data)
    print(f"{out}: {len(data)} B")


# NOTE (G48a P3-4): the "parts" rows below are produced by the repo's own partition_v2, so the tests that compare a
# member's split with them are REGRESSION pins. The independent evidence that partition_v2 equals the scratch split
# (swell76/e76.py, the measured settings) is the G48a reviewer's run: 0 of 46 fixture hours and 0 of 300 perturbed
# real spectra differ (docs/reviews/gfs-spectral-G48-adversarial.md).
def expect():
    sys.path.insert(0, os.path.dirname(HERE))
    sys.path.insert(0, os.path.join(os.path.dirname(HERE), "tools", "model_frames"))
    import gfs_bulletins as GB  # noqa: E402
    import partition2d as P2  # noqa: E402
    import ww3spec  # noqa: E402
    import requests  # noqa: E402
    from app import _parse_bull_text  # noqa: E402
    sess = requests.Session()

    def get_range(url, start, end):
        r = sess.get(url, headers=dict(UA, Range=f"bytes={start}-{end}"), timeout=(5, 30))
        return r.status_code, r.content

    out = {}
    for name in sorted(os.listdir(FIX)):
        m = re.fullmatch(r"gfswave_(\w+)_member\d*\.spec", name)
        if not m:
            continue
        sid = m.group(1)
        with open(os.path.join(FIX, name), "rb") as f:
            sp = ww3spec.read(f.read())
        run = sp.steps[0][0].strftime("%Y%m%d%H")
        text, reason, detail = GB.archive_bulletin(run[:8], run[8:], sid, get_range)
        assert reason == "ok", (sid, run, reason, detail)
        hst = {}
        for row in _parse_bull_text(text, sid, "UTC", run[:8], run[8:], {})["rows"]:
            hst[row[0] + " " + row[1]] = row[-1]
        steps = []
        for when, E, u, ud in sp.steps:
            parts, _ = P2.partition_v2(sp.freqs, sp.dirs, E, wind=(u, ud))
            steps.append({"time": when.strftime("%Y%m%d%H"), "u10": u, "udir": ud,
                          "hst_bulletin": hst.get(when.strftime("%A, %B %d, %Y %I:%M %p").replace(" 0", " ")),
                          "hs": round(ww3spec.hs_of(sp.freqs, sp.dirs, E), 4),
                          "parts": [[round(p["hs"], 4), round(p["tp"], 3), round(p["dir"], 2), p["ws"]]
                                    for p in parts]})
        out[name] = {"station": sid, "run": run, "nf": len(sp.freqs), "nd": len(sp.dirs),
                     "dirs_first3": sp.dirs[:3].tolist(), "steps": steps}
    with open(os.path.join(FIX, "gfsspec_expect.json"), "w") as f:
        json.dump(out, f, indent=0)
    print("gfsspec_expect.json written")


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
    if "--expect" in sys.argv:
        expect()
    elif "--station" in sys.argv:
        station_cut(sys.argv[1], sys.argv[sys.argv.index("--station") + 1], int(sys.argv[sys.argv.index("--steps") + 1]))
    else:
        main(sys.argv[1])
