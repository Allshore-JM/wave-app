"""Great Lakes frame job (plan section 50): render + publish the newest COMPLETE NOAA GLWU run under glwu/v1/.

python tools/model_frames/glwu.py           # env and options as tools/model_frames/run.py

The GFS job's pipeline (run.main with this Source), from NCEP's Great Lakes Wave Unstructured run (WAVEWATCH III on an
unstructured mesh, written on the NDFD 2.5-km Lambert grid; NDFD forecaster winds; NIC ice Nov-May), cycles 01/07/13/19Z,
hourly to +149 h (150 frames). The browser draws it over the five Great Lakes under the GFS selection (the GFS-Wave
frames have no lake data). Per run:
  glwu/v1/<RUN>/{hs,tp,pdir}/f<HHH>.png      831x401, 0.02 deg (decode.GLWU_GRID), the GFS encoding and ranges
  glwu/v1/<RUN>/bull/<id>.bull               the run's point bulletins (the site's Great Lakes forecast tables)
  glwu/v1/<RUN>/winds.json                   the NDFD wind at each bulletin station, every step (the tables' Wind column)
  glwu/v1/<RUN>/manifest-*.json, stats-*.json, then glwu/v1/latest.json (the pointer, last)
Fields: hs = HTSGW, tp = PERPW (1/fp, the GFS-Wave encoding), pdir = DIRPW (peak direction, FROM). Each browser node
takes its nearest GLWU cell (no averaging: every value is the model's); the model's land cells are its missing values.
A water cell with a height but no period (ice-covered, or flat calm) is left empty. A nearest-water fill of
FILL_CELLS nodes carries the colour to the lake shores, where the browser clips it (GSHHG lake polygons).
"""
import json
import os
import re
import sys
import time

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import decode as D            # noqa: E402
import encode as E            # noqa: E402
import glwu_fetch as G        # noqa: E402
import publish as P           # noqa: E402
import run as R               # noqa: E402

MODEL = {
    "name": "NOAA/NCEP GLWU (WAVEWATCH III)", "grid": "Great Lakes 2.5 km Lambert (581x361), nearest cell on 0.02 deg",
    "winds": "NDFD (NWS forecaster grids)", "horizon_h": 149, "cycle_h": 6,
    "ice": "NIC ice concentration, November to May; an ice-covered cell has no period and is left empty",
    "attribution": ("Source: NOAA/NCEP Great Lakes Wave Unstructured (GLWU) model via NOMADS; rendered by Allshore Surf. "
                    "Not an official NWS product."),
    "fields": {
        "hs":   {"label": "Wave height", "grib": "HTSGW", "definition": "Significant height of combined wind waves and swell"},
        "tp":   {"label": "Peak period", "grib": "PERPW", "definition": "Peak wave period Tp = 1/fp from WAVEWATCH III (GRIB table name 'Primary wave mean period')"},
        "pdir": {"label": "Wave direction", "grib": "DIRPW",
                 "definition": "Primary (peak) wave direction from WAVEWATCH III: degrees true, the direction the dominant waves come FROM"},
    },
}
FIELD_NAMES = ("hs", "tp", "pdir")
FIELDS = {n: dict(E.FIELDS[n], resolutions=("full",)) for n in FIELD_NAMES}
WAVE_KEYS = {"hs": "HTSGW:surface", "tp": "PERPW:surface", "pdir": "DIRPW:surface"}
FILL_CELLS = 2                          # nodes of 0.02 deg (~3-4 km) past the model's water: the lake shores the browser clips to
FILL_INFO = {
    "version": 1, "fields": sorted(FIELD_NAMES), "cells": FILL_CELLS,
    "methods": {n: E.FILL_METHODS[n] for n in sorted(FIELD_NAMES)}, "mask": "glwu-model-water",
    "limit": f"within {FILL_CELLS} nodes of the model's water",
    "note": "nearshore values are the nearest model cell's; clipped to the lake shores in the browser",
}
GRID = D.GLWU_GRID
ALLOW_ALL = np.ones((GRID["rows"], GRID["cols"]), bool)
STATION_SEARCH = 3                      # a station's wind: its own GLWU cell, else the nearest water cell within this many
BULL_ROW_RE = re.compile(r"^\s*\|\s*\d{1,2}\s+\d{1,2}\s*\|")      # "| 21 23 |", "| 22  0 |": hours are space-padded
BULL_LOC_RE = re.compile(r"Location\s*:\s*(\S+)\s+\(([\d.]+)N\s+([\d.]+)W\)")
BULL_CYCLE_RE = re.compile(r"Cycle\s*:\s*(\d{8})\s+(\d{2})\s*UTC")
BUILD_BUDGET_S = 20 * 60


def winds_key(run, prefix=P.GLWU_PREFIX):
    return f"{prefix}/{run}/winds.json"


def bull_dir(run, prefix=P.GLWU_PREFIX):
    return f"{prefix}/{run}/bull"


def check_bulletin(text, sid, run_dt):
    """Raise unless `text` is the station's bulletin for this cycle: its Location (id and position within 0.02 deg of
    glwu_fetch.STATIONS), its Cycle, and one "| dd hh |" row per step."""
    loc = BULL_LOC_RE.search(text)
    if not loc or loc.group(1) != sid:
        raise ValueError(f"{sid}: no Location line for this station")
    lat, lon = float(loc.group(2)), -float(loc.group(3))
    want = G.STATIONS[sid]
    if abs(lat - want[0]) > 0.02 or abs(lon - want[1]) > 0.02:
        raise ValueError(f"{sid}: at {lat:.2f} {lon:.2f}, expected {want[0]:.2f} {want[1]:.2f}")
    cyc = BULL_CYCLE_RE.search(text)
    if not cyc or cyc.group(1) + cyc.group(2) != f"{run_dt:%Y%m%d%H}":
        raise ValueError(f"{sid}: not the {run_dt:%Y%m%d %H}Z bulletin")
    rows = sum(1 for line in text.splitlines() if BULL_ROW_RE.match(line))
    if rows != len(G.STEPS):
        raise ValueError(f"{sid}: {rows} rows, expected {len(G.STEPS)}")


def station_cells(water):
    """{id: flat source index} of each station's wind cell: its own GLWU cell when water, else the nearest water cell
    within STATION_SEARCH cells (None outside the grid or with no water near: 45195 on Lake Champlain)."""
    nx, ny = D.GLWU_SRC["Nx"], D.GLWU_SRC["Ny"]
    w = np.asarray(water, bool).reshape(ny, nx)
    out = {}
    for sid, (lat, lon) in G.STATIONS.items():
        col, row = D.lambert_colrow(lat, lon)
        c, r = int(np.rint(col)), int(np.rint(row))
        best = None
        for dr in range(-STATION_SEARCH, STATION_SEARCH + 1):
            for dc in range(-STATION_SEARCH, STATION_SEARCH + 1):
                rr, cc = r + dr, c + dc
                if 0 <= rr < ny and 0 <= cc < nx and w[rr, cc]:
                    d = (rr - row) ** 2 + (cc - col) ** 2
                    if best is None or d < best[0]:
                        best = (d, rr * nx + cc)
        out[sid] = None if best is None else int(best[1])
    return out


class GlwuSource:
    name = "NOMADS"
    tag = "GLWU "
    prefix = P.GLWU_PREFIX
    model = MODEL
    prune_legacy = False
    stale_fail_h = 18                   # three missed cycles: the workflow fails (the owner is mailed)
    failed_retry_after_s = 1800
    failed_max_attempts = 6
    steps = G.STEPS
    schedule = G.STEP_SCHEDULE
    fields = FIELDS
    fill_info = FILL_INFO

    def __init__(self):
        self._run = self._recs = None
        self._index = None
        self._cells = None
        self._winds = {}
        self._checked = False

    def latest_complete_run(self):
        """A throttled NOMADS means "try again next tick", like no run yet."""
        try:
            run_dt, recs = G.latest_complete_run()
        except G.Throttled as exc:
            print(f"NOMADS throttled the GLWU listing ({exc}); trying again next tick")
            return None
        if run_dt is not None:
            self._start(run_dt, recs)
        return run_dt

    def _start(self, run_dt, recs):
        self._run, self._recs = run_dt, recs
        self._cells, self._winds, self._checked = None, {}, False

    def fetch(self, run_dt, step):
        if _started[0] is not None and time.monotonic() - _started[0] > BUILD_BUDGET_S:
            raise G.NotReady(f"build still running after {BUILD_BUDGET_S // 60} min (step {step}): giving up for this tick")
        if self._run != run_dt:
            self._start(run_dt, G.fetch_idx(run_dt))
        return G.fetch_step(run_dt, self._recs, step)

    def grid_blocks(self):
        return {"res": {"full": ""}, "grid": dict(GRID)}

    def encode(self, grid, name):
        return E.encode_frame(grid, name, fill=True, allow=ALLOW_ALL, cells=FILL_CELLS, resolutions=("full",))

    def decode(self, records, run_dt, step):
        """{hs, tp, pdir} on the browser grid; the step's station winds are kept for winds.json."""
        if self._index is None:
            self._index = D.glwu_index()
        flat, inside = self._index
        src = {}
        for name, key in WAVE_KEYS.items():
            if name == "hs" and not self._checked:
                vals, _meta, ll = D.decode_glwu(records[key], key, run_dt, step, coords=True)
                D.check_glwu_coords(*ll)                    # the projection, against eccodes, once per build
                self._checked = True
            else:
                vals, _meta = D.decode_glwu(records[key], key, run_dt, step)
            src[name] = vals
        blank = np.isnan(src["hs"]) | np.isnan(src["tp"])   # land, or water without a period (ice-covered, flat calm)
        ws, _ = D.decode_glwu(records["WIND:surface"], "WIND:surface", run_dt, step)
        wd, _ = D.decode_glwu(records["WDIR:surface"], "WDIR:surface", run_dt, step)
        if self._cells is None:
            self._cells = station_cells(~np.isnan(ws))
        self._winds[step] = {sid: (None if i is None or np.isnan(ws[i]) else
                                   [round(float(ws[i]), 1), int(round(float(wd[i]))) % 360])
                             for sid, i in self._cells.items()}
        grids = {}
        for name in FIELD_NAMES:
            v = np.where(blank, np.nan, src[name])
            g = np.where(inside, v[flat], np.nan).astype(np.float32)
            if name == "pdir":
                g = np.mod(g, np.float32(360.0)).astype(np.float32)
            grids[name] = g
        return grids

    def before_manifest(self, store, run_dt, run, upload):
        """The run's bulletins and station winds, stored before the manifest (the pointer never names a run without
        them). Up to BULLETIN_MAX_MISSING bulletins may be missing or bad (logged, left out); more fails the build."""
        ids, bad = [], []
        for sid in G.STATIONS:
            try:
                text = G.fetch_bulletin(run_dt, sid)
                check_bulletin(text, sid, run_dt)
            except G.Throttled:
                raise
            except (G.NotReady, ValueError) as exc:
                bad.append(str(exc))
                continue
            if upload:
                store.put(f"{bull_dir(run, self.prefix)}/{sid}.bull", text.encode("ascii", "replace"),
                          "text/plain; charset=utf-8", P.IMMUTABLE)
            ids.append(sid)
        for line in bad:
            print("bulletin left out:", line)
        if len(bad) > G.BULLETIN_MAX_MISSING:
            raise RuntimeError(f"{len(bad)} bulletins missing or bad (limit {G.BULLETIN_MAX_MISSING}): {bad[:3]}")
        steps = sorted(self._winds)
        cells = self._cells or {}
        winds = {"run": run, "steps": steps, "units": {"speed": "m/s", "dir": "deg true, FROM"},
                 "source": "NDFD 10 m wind (GLWU forcing)",
                 "stations": {sid: [self._winds[s].get(sid) for s in steps]
                              for sid in sorted(cells) if cells[sid] is not None}}
        if upload:
            store.put(winds_key(run, self.prefix), json.dumps(winds, separators=(",", ":"), sort_keys=True).encode(),
                      "application/json", P.IMMUTABLE)
        return {"bulletins": {"dir": bull_dir(run, self.prefix), "ids": sorted(ids)},
                "winds": winds_key(run, self.prefix)}


SOURCE = GlwuSource()
_started = [None]


def main(argv=None):
    _started[0] = time.monotonic()
    try:
        return R.main(argv, src=SOURCE)
    finally:
        _started[0] = None


if __name__ == "__main__":
    sys.exit(main())
