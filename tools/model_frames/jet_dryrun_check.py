"""TEMPORARY (plan section 106, step 3): on GitHub's runner (pinned eccodes 2.48, JPEG2000 + CCSDS), build a few steps of
the newest GFS and ECMWF runs in memory and check
  (a) jet / jdir / jhgt codes against eccodes-direct values at random nodes (error <= half a code),
  (b) every older field's CODES against the frames production published for the same run (unchanged data).
Prints one markdown line per check and fails on any miss. REMOVE before the job-only merge, with jet-dryrun.yml."""
import io
import os
import sys
import urllib.request

import numpy as np
from PIL import Image

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import decode as D        # noqa: E402
import encode as E        # noqa: E402
import fetch as F         # noqa: E402
import ecmwf_fetch as EF  # noqa: E402
import ecmwf as X         # noqa: E402
import publish as P       # noqa: E402
import run as R           # noqa: E402

BUCKET = "https://models.allshoresurf.com"
UA = {"User-Agent": "allshoresurf.com jet dry run (https://allshoresurf.com)"}
OLDER = ("hs", "tp", "wind", "pdir", "wdir", "pres")
rng = np.random.default_rng(106)


class Mem:
    """The smallest store build_and_publish needs (publish.Store over an in-memory client)."""
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, ContentType="", CacheControl="", **kw):
        self.objects[Key] = Body


def codes(png):
    return np.array(Image.open(io.BytesIO(png)))


def get(url):
    return urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=60).read()


def live_run(prefix):
    import json
    return json.loads(get(f"{BUCKET}/{P.latest_key(prefix)}"))["run"]


def check(src, steps, truth):
    from datetime import datetime, timezone
    run = live_run(src.prefix)
    run_dt = datetime.strptime(run, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    c = Mem()
    R.build_and_publish(P.Store(c, "b"), run_dt, steps, log=lambda *a: None, src=src)
    bad, lines, sizes = [], [], {}
    for s in steps:
        u, v, h = truth(run_dt, s)
        spd = np.hypot(u.astype(np.float64), v.astype(np.float64))
        dr = np.degrees(np.arctan2(-u.astype(np.float64), -v.astype(np.float64))) % 360
        worst = {}
        for name, true, half in (("jet", spd, False), ("jet", spd, True), ("jdir", dr, True), ("jhgt", h.astype(np.float64), True)):
            body = c.objects[P.frame_key(run, name, s, half=half, prefix=src.prefix)]
            sizes.setdefault(f"{'half/' if half else ''}{name}", []).append(len(body))
            q = codes(body)
            t = true[::2, ::2] if half else true
            r = rng.integers(0, q.shape[0], 500)
            cc = rng.integers(0, q.shape[1], 500)
            f = E.FIELDS[name]
            dec = E.dequantize(q[r, cc], f["lo"], f["hi"])
            if f.get("circular"):
                err = np.abs((dec - t[r, cc] + 180.0) % 360.0 - 180.0) / (360.0 / 508)
            else:
                err = np.abs(dec - np.clip(t[r, cc], f["lo"], f["hi"])) / (E.quantum(f["lo"], f["hi"]) / 2)
            worst[f"{'half/' if half else ''}{name}"] = round(float(err.max()), 3)
            if err.max() > 1.001:
                bad.append(f"{name} f{s:03d} {err.max():.3f}")
        same = []
        for name in OLDER:
            for half in (False, True):
                if ("half" if half else "full") not in E.FIELDS[name]["resolutions"]:
                    continue
                key = P.frame_key(run, name, s, half=half, prefix=src.prefix)
                ok = np.array_equal(codes(c.objects[key]), codes(get(f"{BUCKET}/{key}")))
                same.append(ok)
                if not ok:
                    bad.append(f"{key} differs from production")
        lines.append(f"- {src.name} {run} f{s:03d}: jet errors / half-code {worst}; older fields equal to production "
                     f"{sum(same)}/{len(same)}")
    lines.append(f"- {src.name} PNG KB per step: " + ", ".join(f"{k} {np.mean(v) / 1024:.0f}" for k, v in sorted(sizes.items())))
    return bad, lines


def gfs_truth(run_dt, s):
    recs = F.fetch_records(F.atmos_url(run_dt, s), list(R.JET_KEYS))
    return tuple(D.decode(recs[k], k, run_dt, s)[0] for k in R.JET_KEYS)


def ecmwf_truth(run_dt, s):
    recs = EF.fetch_records(run_dt, s, "oper", EF.JET_PARAMS)
    return tuple(D.decode_ecmwf(recs[p], p, run_dt, s)[0] for p in EF.JET_PARAMS)


if __name__ == "__main__":
    bad, lines = [], []
    for src, steps, truth in ((R.GFS, [0, 24, 120, 123, 240, 384], gfs_truth), (X.SOURCE, [0, 24, 144, 150, 360], ecmwf_truth)):
        b, l = check(src, steps, truth)
        bad += b
        lines += l
    print("\n".join(lines))
    if bad:
        print("FAILED: " + "; ".join(bad[:20]))
        sys.exit(1)
    print("all jet checks passed")
