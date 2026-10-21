"""Capture the Great Lakes job's test fixtures (plan section 50) from one real GLWU run (network + eccodes):

    python tests/capture_glwu_fixtures.py 2026102119

tests/fixtures/glwu/run.idx        the run's .idx lines of steps 0, 1, 148 and 149 (19 records each, file order)
tests/fixtures/glwu/meta.json      per record of step 0: eccodes metadata (decode._GLWU_KEYS); the source grid's
                                   latitude / longitude at a few (col, row) nodes (eccodes' own coordinates, for the
                                   Lambert maths); the values at a few cells; the water-cell count; and every station's
                                   wind cell and its WIND / WDIR (the winds.json the job writes)
tests/fixtures/glwu/45007.bull     one real bulletin of the run
"""
import json
import os
import sys
from datetime import datetime, timezone

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "tools", "model_frames"))

import decode as D        # noqa: E402
import glwu as GL         # noqa: E402
import glwu_fetch as G    # noqa: E402

NODES = [(0, 0), (580, 0), (0, 360), (580, 360), (290, 180), (100, 250), (400, 60), (517, 112)]   # (col, row)
CELLS = [(290, 180), (100, 250), (400, 60), (1, 1)]                                                 # (col, row): water + land


def main(run):
    import eccodes
    run_dt = datetime.strptime(run, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    out = os.path.join(HERE, "fixtures", "glwu")
    os.makedirs(out, exist_ok=True)
    _status, body = G.request(G.grib_url(run_dt) + ".idx")
    keep = [ln for ln in body.decode("ascii").splitlines()
            if G._step_of(ln.split(":")[5] if ln.count(":") >= 6 else "") in (0, 1, 148, 149)]
    open(os.path.join(out, "run.idx"), "w", encoding="ascii", newline="\n").write("\n".join(keep) + "\n")
    recs = G.parse_idx(body.decode("ascii"))
    step0 = G.fetch_step(run_dt, recs, 0)
    meta = {"run": run, "records": {}}
    nx = D.GLWU_SRC["Nx"]
    for key, blob in step0.items():
        h = eccodes.codes_new_from_message(blob)
        try:
            m = {k: eccodes.codes_get(h, k) for k in D._GLWU_KEYS}
            vals = eccodes.codes_get_values(h)
            if key == "HTSGW:surface":
                lat, lon = eccodes.codes_get_array(h, "latitudes"), eccodes.codes_get_array(h, "longitudes")
                meta["nodes"] = [[c, r, float(lat[r * nx + c]), float(lon[r * nx + c])] for c, r in NODES]
                meta["water_cells"] = int(np.count_nonzero(vals != m["missingValue"]))
        finally:
            eccodes.codes_release(h)
        m = {k: (v if isinstance(v, (int, str)) else float(v)) for k, v in m.items()}
        m["cells"] = [[c, r, float(vals[r * nx + c])] for c, r in CELLS]
        meta["records"][key] = m
    ws, _ = D.decode_glwu(step0["WIND:surface"], "WIND:surface", run_dt, 0)
    wd, _ = D.decode_glwu(step0["WDIR:surface"], "WDIR:surface", run_dt, 0)
    cells = GL.station_cells(~np.isnan(ws))
    meta["stations"] = {sid: (None if i is None else [i, float(ws[i]), float(wd[i])]) for sid, i in cells.items()}
    json.dump(meta, open(os.path.join(out, "meta.json"), "w", encoding="utf-8"), indent=1, sort_keys=True)
    bull = G.fetch_bulletin(run_dt, "45007")
    open(os.path.join(out, "45007.bull"), "w", encoding="ascii", newline="\n").write(bull)
    print("wrote", out, len(keep), "idx lines")


if __name__ == "__main__":
    main(sys.argv[1])
