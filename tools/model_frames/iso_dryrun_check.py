"""TEMPORARY (plan section 46, step 1; removed before the job-only merge): build a few steps of the newest complete GFS
and ECMWF runs on the runner through the real job code into memory, then decode every sea-level pressure frame back
and compare it with eccodes' own values of the same GRIB records (Pa / 100, the GFS columns rolled here by hand, not by
the job's decode): |diff| <= half a quantum (+ float32 rounding) inside 900-1080 hPa, clamped codes outside, no cell
missing (pressure is defined everywhere), half-res frames only. Prints ::notice:: lines for the public annotations."""
import argparse
import io
import os
import sys
import time

import numpy as np
from PIL import Image

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import ecmwf as X          # noqa: E402
import ecmwf_fetch as EF   # noqa: E402
import encode as E         # noqa: E402
import fetch as F          # noqa: E402
import publish as P        # noqa: E402
import run as R            # noqa: E402


class Mem:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, ContentType, CacheControl):
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        raise KeyError(Key)

    def list_objects_v2(self, **kw):
        return {"Contents": []}


def raw(blob):
    import eccodes
    h = eccodes.codes_new_from_message(blob)
    try:
        nj, ni = eccodes.codes_get(h, "Nj"), eccodes.codes_get(h, "Ni")
        v = np.asarray(eccodes.codes_get_values(h), dtype=np.float64).reshape(nj, ni)
        meta = {k: eccodes.codes_get(h, k) for k in ("shortName", "typeOfLevel", "level", "units", "packingType",
                                                     "longitudeOfFirstGridPointInDegrees", "missingValue")}
    finally:
        eccodes.codes_release(h)
    return np.where(v == meta["missingValue"], np.nan, v), meta


def check(label, src, run_dt, steps, capture, key_of):
    blobs = {}

    def keep(*args):
        out = capture(*args)
        if isinstance(out, tuple):
            for part in out:
                blobs.update({(args[1], k): b for k, b in part.items()})
        else:
            blobs.update({(args[1], k): b for k, b in out.items()})
        return out
    c = Mem()
    t0 = time.time()
    orig = src.fetch
    src.fetch = lambda run_dt_, step: keep(run_dt_, step)
    try:
        m = R.build_and_publish(P.Store(c, "mem"), run_dt, steps, log=lambda *a: None, src=src)
    finally:
        src.fetch = orig
    secs = time.time() - t0
    f = E.FIELDS["pres"]
    lo, hi, half_q = f["lo"], f["hi"], (f["hi"] - f["lo"]) / 508
    run = P.run_key(run_dt)
    worst, n, sizes, lows, highs = 0.0, 0, [], [], []
    for step in steps:
        full = P.frame_key(run, "pres", step, half=False, prefix=src.prefix)
        half = P.frame_key(run, "pres", step, half=True, prefix=src.prefix)
        assert full not in c.objects, f"{label}: a full-resolution pressure frame was published"
        png = c.objects[half]
        sizes.append(len(png))
        q = np.array(Image.open(io.BytesIO(png)))
        assert q.shape == (361, 720) and q.dtype == np.uint8
        v, meta = raw(blobs[(step, key_of)])
        assert (meta["typeOfLevel"], meta["level"], meta["units"]) == ("meanSea", 0, "Pa"), meta
        if abs(meta["longitudeOfFirstGridPointInDegrees"]) < 1e-6:      # GFS: 0 E first; the site grid starts at -180
            v = np.roll(v, 720, axis=1)
        hpa = v / 100.0
        exp = hpa[::2, ::2]
        assert not np.isnan(exp).any() and (q > 0).all(), f"{label} f{step:03d}: a pressure cell is missing"
        back = E.dequantize(q, lo, hi)
        inside = (exp > lo) & (exp < hi)
        err = np.abs(back - exp)[inside]
        worst = max(worst, float(err.max()))
        assert err.max() <= half_q + 1e-3, f"{label} f{step:03d}: {err.max():.4f} hPa > half a quantum"
        assert (q[exp <= lo] == 1).all() and (q[exp >= hi] == 255).all(), f"{label} f{step:03d}: clamping"
        n += int(inside.sum())
        lows.append(float(np.nanmin(hpa)))
        highs.append(float(np.nanmax(hpa)))
    stats = m["_stats"]["frames"]
    old = {k for k in c.objects if "/pres/" not in k and k.endswith(".png")}
    line = (f"{label} run {run}: steps {','.join(map(str, steps))}; {n} half cells within {worst:.3f} hPa "
            f"(half a quantum {half_q:.3f}); pres PNG {min(sizes)}-{max(sizes)} B; range {min(lows):.1f}-{max(highs):.1f} hPa; "
            f"clamped low {sum(s['fields']['pres']['clamped_low'] for s in stats)} high "
            f"{sum(s['fields']['pres']['clamped_high'] for s in stats)}; other frames {len(old)}; build {secs:.1f} s")
    print(line)
    print(f"::notice title={label} pressure dry run::{line}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gfs-steps", default="0,120,384")
    ap.add_argument("--ecmwf-steps", default="0,150,360")
    a = ap.parse_args()
    import eccodes
    print("eccodes", eccodes.codes_get_api_version())
    gfs_run = F.latest_complete_run()
    if gfs_run is not None:
        check("GFS", R.GFS, gfs_run, [int(s) for s in a.gfs_steps.split(",")],
              lambda run_dt, step: (F.fetch_records(F.wave_url(run_dt, step), list(R.WAVE_KEYS.values())),
                                    F.fetch_records(F.atmos_url(run_dt, step), list(R.ATMOS_KEYS))),
              "PRMSL:mean sea level")
    else:
        print("::notice title=GFS pressure dry run::no complete GFS run")
    ec_run = EF.latest_complete_run()
    if ec_run is not None:
        check("ECMWF", X.SOURCE, ec_run, [int(s) for s in a.ecmwf_steps.split(",")],
              lambda run_dt, step: X.EcmwfSource.fetch(X.SOURCE, run_dt, step), "msl")
    else:
        print("::notice title=ECMWF pressure dry run::no complete ECMWF run")
    return 0


if __name__ == "__main__":
    sys.exit(main())
