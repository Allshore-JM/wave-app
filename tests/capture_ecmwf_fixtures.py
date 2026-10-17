"""Capture the ECMWF job's test fixtures (plan section 43) from one real open-data step (network + eccodes):

    python tests/capture_ecmwf_fixtures.py 2026101412 150
    python tests/capture_ecmwf_fixtures.py 2026101412 150 --params msl --host https://data.ecmwf.int/forecasts

tests/fixtures/ecmwf/wave_<step>.index, oper_<step>.index   the real .index files (JSON lines)
tests/fixtures/ecmwf/meta_<step>.json   per record: eccodes metadata (decode._KEYS + gridType), the index span, and the
                                        value at a few cells (sea, land/missing, both poles, both ends of a row, sea ice)

The second form (plan section 46) ADDS the named records to the existing meta_<step>.json of the same run and step and
leaves every other record and the .index files untouched (it checks the index is unchanged); data.ecmwf.int keeps runs
a few days longer than the AWS mirror.
"""
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "tools", "model_frames"))

import decode as D        # noqa: E402
import ecmwf_fetch as EF  # noqa: E402

CELLS = [(0, 0), (720, 1439), (300, 0), (300, 1439), (274, 88), (400, 720), (180, 300), (500, 1000), (284, 87),
         (22, 1329), (638, 630), (596, 1042)]    # G30a F1: Arctic and Antarctic pack ice (swh ~0, pp1d noise), waves under ice
UA = {"User-Agent": "allshoresurf.com fixtures (https://allshoresurf.com)"}


def main(run, step, only=None, host=None):
    run_dt = datetime.strptime(run, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    out = os.path.join(HERE, "fixtures", "ecmwf")
    os.makedirs(out, exist_ok=True)
    path = os.path.join(out, f"meta_{step}.json")
    meta = {"run": run, "step": step, "records": {}}
    if only:
        meta = json.load(open(path, encoding="utf-8"))
        assert meta["run"] == run and meta["step"] == step, "--params adds to the fixture of the same run and step"
    base = host or EF.S3
    import eccodes
    import numpy as np
    for stream, params in EF.STREAMS.items():
        if only:
            params = tuple(p for p in params if p in only)
            if not params:
                continue
        rel = EF.stem(run_dt, step, stream)
        text = urllib.request.urlopen(urllib.request.Request(f"{base}/{rel}.index", headers=UA), timeout=60).read().decode()
        ipath = os.path.join(out, f"{stream}_{step}.index")
        if only:
            with open(ipath, encoding="utf-8", newline="") as fh:
                assert fh.read() == text, "the index changed since the fixture was captured"
        else:
            with open(ipath, "w", newline="\n") as fh:
                fh.write(text)
        spans = EF.select(EF.parse_index(text), params, run_dt, step, stream)
        for p, (off, n) in spans.items():
            req = urllib.request.Request(f"{base}/{rel}.grib2", headers=dict(UA, Range=f"bytes={off}-{off + n - 1}"))
            blob = urllib.request.urlopen(req, timeout=120).read()
            h = eccodes.codes_new_from_message(blob)
            m = {k: eccodes.codes_get(h, k) for k in D._KEYS + ("gridType",)}
            vals = np.asarray(eccodes.codes_get_values(h)).reshape(m["Nj"], m["Ni"])
            eccodes.codes_release(h)
            m = {k: (v.item() if hasattr(v, "item") else v) for k, v in m.items()}
            cells = [[r, c, (None if vals[r, c] == m["missingValue"] else round(float(vals[r, c]), 6))] for r, c in CELLS]
            meta["records"][p] = {"meta": m, "span": [off, n], "cells": cells}
    with open(path, "w", newline="\n") as fh:
        json.dump(meta, fh, indent=1, sort_keys=True)
        fh.write("\n")


if __name__ == "__main__":
    args = sys.argv[1:]
    opt = {}
    for flag in ("--params", "--host"):
        if flag in args:
            i = args.index(flag)
            opt[flag] = args[i + 1]
            del args[i:i + 2]
    main(args[0], int(args[1]), only=opt["--params"].split(",") if "--params" in opt else None, host=opt.get("--host"))
