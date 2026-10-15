"""TEMPORARY (plan section 43, step 1; removed before the job-only merge): build the newest complete ECMWF run on the
runner through the real job code into memory, then decode every PNG back and compare it with eccodes' own values of
the same GRIB records: |diff| <= half a quantum (+ float32 rounding), no model cell lost, half = full[::2, ::2], wave
direction present exactly where the wave height is. Prints the build time and a digest of the frames of chosen steps
(compared with a local build of the same records)."""
import argparse
import hashlib
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
        v = np.asarray(eccodes.codes_get_values(h), dtype=np.float64).reshape(eccodes.codes_get(h, "Nj"), eccodes.codes_get(h, "Ni"))
        mv = eccodes.codes_get(h, "missingValue")
        info = (eccodes.codes_get(h, "packingType"), eccodes.codes_get(h, "shortName"))
    finally:
        eccodes.codes_release(h)
    return np.where(v == mv, np.nan, v), info


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--steps", default="")
    ap.add_argument("--digest-steps", default="0,150,360")
    a = ap.parse_args()
    import eccodes
    print("eccodes", eccodes.codes_get_api_version())
    run_dt = EF.latest_complete_run()
    if run_dt is None:
        print("no complete ECMWF run")
        return 3
    steps = [int(s) for s in a.steps.split(",")] if a.steps else EF.STEPS
    blobs = {}
    real = EF.fetch_records

    def keep(run_dt_, step, stream, params):
        out = real(run_dt_, step, stream, params)
        blobs.update({(step, p): b for p, b in out.items()})
        return out
    EF.fetch_records = keep
    c = Mem()
    t0 = time.time()
    man = R.build_and_publish(P.Store(c, "b"), run_dt, steps, log=print, src=X.SOURCE)
    build_s = time.time() - t0
    run = P.run_key(run_dt)
    worst, packing = {}, set()
    digest = hashlib.sha256()
    want = {int(s) for s in a.digest_steps.split(",") if s}
    for step in steps:
        u, iu = raw(blobs[(step, "10u")])
        v, _ = raw(blobs[(step, "10v")])
        truth = {}
        for name, p in X.FIELD_PARAMS.items():
            truth[name], info = raw(blobs[(step, p)])
            packing.add(info)
        packing.add(iu)
        truth["wind"] = np.sqrt(u * u + v * v)
        truth["wdir"] = np.degrees(np.arctan2(-u, -v)) % 360
        assert np.array_equal(np.isnan(truth["hs"]), np.isnan(truth["pdir"])), f"mwd/swh masks differ at {step}"
        for name in ("hs", "tp", "pdir", "wind", "wdir"):
            f = E.FIELDS[name]
            res = "full" if "full" in f["resolutions"] else "half"
            key = f"{P.ECMWF_PREFIX}/{run}/{'' if res == 'full' else 'half/'}{name}/f{step:03d}.png"
            q = np.array(Image.open(io.BytesIO(c.objects[key])))
            if res == "full":
                qh = np.array(Image.open(io.BytesIO(c.objects[f"{P.ECMWF_PREFIX}/{run}/half/{name}/f{step:03d}.png"])))
                assert np.array_equal(qh, q[::2, ::2]), (name, step, "half")
            if step in want:
                digest.update(c.objects[key])
            t = truth[name] if res == "full" else truth[name][::2, ::2]
            model = ~np.isnan(t)
            assert np.all(q[model] > 0), (name, step, "a model cell was lost")
            inside = model & (t > f["lo"]) & (t < f["hi"])
            d = np.abs(E.dequantize(q, f["lo"], f["hi"])[inside] - t[inside])
            if f.get("circular"):
                d = np.minimum(d, 360 - d)
            lim = E.quantum(f["lo"], f["hi"]) / 2 + 1e-6 * max(abs(f["hi"]), 1)
            assert d.max() <= lim, (name, step, float(d.max()), lim)
            worst[name] = max(worst.get(name, 0.0), float(d.max()))
    st = man["_stats"]["frames"]
    print(f"run {run}: {len(steps)} steps built in {build_s:.0f} s ({build_s / len(steps):.1f} s a step); complete={man['complete']}")
    print("packing:", sorted(packing))
    print("max |decoded - eccodes| per field:", {k: round(v, 5) for k, v in worst.items()})
    print("pdir mismatch steps:", sum(1 for f in st if f["pdir_mask_mismatch"]), "filled hs:", st[0]["fields"]["hs"]["filled_points"])
    print("stored MB:", round(sum(len(b) for b in c.objects.values()) / 1e6, 1))
    print(f"DIGEST steps {sorted(want & set(steps))}: {digest.hexdigest()[:16]}")
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
