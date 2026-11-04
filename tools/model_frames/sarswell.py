"""Sentinel-1 swell partitions job (plan sections 74 and 81): the last ~30 h of SAR wave-mode swell partitions (height,
PEAK period, direction FROM) -> R2 under sarswell/v1/ as one chunk per 3-hour window (altfmt: magic SWL1), on the
machinery of the altimeter heights job (alt.py: STAC + bucket listing, the plan, downloads, publish, prune, stale).

Source: Copernicus Marine WAVE_GLO_PHY_SPC-FWK_L3_NRT_014_002, the measured (level-3) swell partition datasets of
Sentinel-1 A / C (and CFOSAT when flowing), anonymous on the open CloudFerro bucket. The propagated storm-source
("fireworks") datasets are left out: they are forecasts along the swell's path and would need their own display rule
(owner's call later). Credit as the heights (E.U. Copernicus Marine Service Information, doi 10.48670/moi-00174).

Per point: t, lat, lon, n partitions (the dominant, by height, first), mission, flags, then SWELL_PARTS x (hs m, tp s,
dir degrees true FROM). The reader refuses a file whose period is not a PEAK period or whose direction is not the
direction the swell comes FROM (the MFWAM lesson: the variable attributes say which; a mean period stops the job).

Verification against a real file is pending the session's network allowance (plan section 81, step 1): the dataset
pattern, the file name pattern and the variable names below are the documented ones and are checked on the first real
listing / file (`--inspect FILE` prints a file's variables). Nothing is published while they fail.
"""
import os
import re
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import alt as A          # noqa: E402
import altfmt as F       # noqa: E402

PREFIX = "sarswell/v1"
FORMAT = "swell-v1"
STAC_ID = "WAVE_GLO_PHY_SPC-FWK_L3_NRT_014_002"
SOURCE = {"name": "Copernicus Marine WAVE_GLO_PHY_SPC-FWK_L3_NRT_014_002 (L3 Sentinel-1 swell partitions)",
          "doi": "10.48670/moi-00174", "url": "https://doi.org/10.48670/moi-00174",
          "credit": "Generated using E.U. Copernicus Marine Service Information; https://doi.org/10.48670/moi-00174"}
# The measured datasets: one per satellite, sampled every ~15 s along the orbit (the SAR wave-mode imagettes); the
# propagated fireworks datasets carry a 3-hour timestep (PT3H) and never match.
DATASET_RE = re.compile(r"spc-fwk_nrt_(?P<token>s1[a-d]|cfo[a-z0-9]*)_l3_PT15S", re.I)
# Product file names carry the observation span (two stamps) and usually a production stamp: the mission comes from the
# dataset (token_from_file False).
FILE_RE = re.compile(r"(?P<t0>\d{8}T\d{6})_(?P<t1>\d{8}T\d{6})(?:_(?P<prod>\d{8}T\d{6}))?\.nc$")
MISSIONS = {"s1a": ("Sentinel-1A", "Sentinel-1A SAR wave mode"), "s1b": ("Sentinel-1B", "Sentinel-1B SAR wave mode"),
            "s1c": ("Sentinel-1C", "Sentinel-1C SAR wave mode"), "s1d": ("Sentinel-1D", "Sentinel-1D SAR wave mode"),
            "cfo": ("CFOSAT", "CFOSAT SWIM"), "cfosat": ("CFOSAT", "CFOSAT SWIM")}
MAX_FILE_BYTES = 50_000_000
STALE_H = 48                                  # daily batches 7-10 h behind: the newest point is up to ~34 h old between them
# Variable names tried, in order (the first present wins); every family must be found. A partition variable is either
# one 2-D array (points x partitions) or one 1-D array per partition suffixed _SW1.. / _1.. / 1..
VARS = {
    "time": ["time", "TIME"],
    "lat": ["latitude", "lat", "LATITUDE"],
    "lon": ["longitude", "lon", "LONGITUDE"],
    "hs": ["VHM0_SW", "VHM0", "swh_partition", "hs_partition", "partition_swh", "hs", "swh"],
    "tp": ["VTPK_SW", "VTPK", "peak_period_partition", "tp_partition", "partition_tp", "tp", "VTM10_SW", "VTM10", "VTM02_SW", "VTM02", "mean_period_partition"],
    "dir": ["VPED_SW", "VPED", "VMDR_SW", "VMDR", "peak_direction_partition", "dir_partition", "partition_dir", "dir", "pdir"],
}
PERIOD_PEAK_RE = re.compile(r"peak", re.I)
PERIOD_MEAN_RE = re.compile(r"\bmean\b|energy", re.I)
DIR_FROM_RE = re.compile(r"\bfrom\b|from_direction|coming", re.I)
DIR_TO_RE = re.compile(r"\btowards?\b|to_direction|going", re.I)


def _find_var(f, names):
    """The dataset for a family: a 2-D variable, or the 1-D per-partition set (returned as a list), else None."""
    for n in names:
        if n in f:
            return f[n]
        parts = []
        for k in range(1, F.SWELL_PARTS + 2):
            for cand in (f"{n}{k}", f"{n}_{k}", f"{n}_SW{k}", f"{n}SW{k}"):
                if cand in f:
                    parts.append(f[cand])
                    break
        if parts:
            return parts
    return None


def _text(ds):
    """Every string attribute of a variable, joined (the checks look for words in them)."""
    out = []
    for k in ("long_name", "standard_name", "comment", "description", "units"):
        v = A._attr(ds, k)
        if isinstance(v, str):
            out.append(f"{k}={v}")
    return " ".join(out)


def _stack(var, expect_units=None):
    """A partition family -> float64 array (points x partitions), fill as NaN; and the attributes' text (first part)."""
    if isinstance(var, list):
        cols = [A._scaled(v, expect_units=expect_units) for v in var]
        n = min(len(c) for c in cols)
        arr = np.stack([c[:n] for c in cols], axis=1)
        return arr, _text(var[0])
    v = A._scaled(var, expect_units=expect_units)
    if v.ndim == 1:
        v = v[:, None]
    if v.ndim != 2:
        raise ValueError(f"{var.name}: {v.ndim}-d partitions")
    if v.shape[1] > 16 and v.shape[0] <= 16:                            # (partitions x points) stored the other way round
        v = v.T
    return v, _text(var)


def inspect(path):
    """Print every variable with its shape and attributes (the real-file check of plan section 81, step 1)."""
    import h5py
    with h5py.File(path, "r") as f:
        print("global:", {k: A._attr(f, k) for k in f.attrs})

        def visit(name, obj):
            if isinstance(obj, h5py.Dataset):
                print(f"{name}: shape {obj.shape} dtype {obj.dtype} attrs {dict((k, A._attr(obj, k)) for k in obj.attrs)}")
        f.visititems(visit)


def read_swell(path, t0=None, t1=None):
    """A SPC-FWK L3 file -> {t, lat, lon, hs (n x P), tp (n x P), dir (n x P, degrees true FROM)} of the points with at
    least one partition, within the window when given, time-sorted; the partitions of a point sorted by height,
    the dominant first, at most SWELL_PARTS kept. Refuses: a period that is not a PEAK period, a direction that is not
    the direction the swell comes FROM (by the variables' attributes), a time axis not in seconds since 2000-01-01."""
    import h5py
    with h5py.File(path, "r") as f:
        tv = _find_var(f, VARS["time"])
        if tv is None or isinstance(tv, list):
            raise ValueError("no time variable")
        tunits = A._attr(tv, "units", "")
        if not (isinstance(tunits, str) and re.match(r"^seconds since 2000-01-01[ T]00:00:00", tunits)):
            raise ValueError(f"time units {tunits!r} are not seconds since 2000-01-01")
        t = A._scaled(tv)
        latv, lonv = _find_var(f, VARS["lat"]), _find_var(f, VARS["lon"])
        if latv is None or lonv is None or isinstance(latv, list) or isinstance(lonv, list):
            raise ValueError("no latitude / longitude")
        lat = A._scaled(latv, expect_units="degrees_north")
        lon = A._scaled(lonv, expect_units="degrees_east")
        hv, pv, dv = _find_var(f, VARS["hs"]), _find_var(f, VARS["tp"]), _find_var(f, VARS["dir"])
        if hv is None or pv is None or dv is None:
            raise ValueError("partition height / period / direction variables not found")
        hs, _ht = _stack(hv, expect_units="m")
        tp, ptext = _stack(pv, expect_units="s")
        dr, dtext = _stack(dv, expect_units="degree")
    if not PERIOD_PEAK_RE.search(ptext) or PERIOD_MEAN_RE.search(ptext):
        raise ValueError(f"the partition period is not a peak period ({ptext}); the swell job stops here")
    if DIR_TO_RE.search(dtext) and not DIR_FROM_RE.search(dtext):
        raise ValueError(f"the partition direction is not the direction the swell comes from ({dtext})")
    if not DIR_FROM_RE.search(dtext):
        raise ValueError(f"the partition direction's convention is not stated ({dtext}); verify the file first")
    n = len(t)
    if not (lat.shape == lon.shape == (n,)) or hs.shape[0] != n or tp.shape[0] != n or dr.shape[0] != n:
        raise ValueError("time / position / partition shapes differ")
    P = min(hs.shape[1], tp.shape[1], dr.shape[1])
    hs, tp, dr = hs[:, :P], tp[:, :P], dr[:, :P]
    lon = (lon + 180.0) % 360.0 - 180.0
    good = np.isfinite(hs) & np.isfinite(tp) & np.isfinite(dr) & (hs > 0) & (hs <= 30) & (tp >= 1) & (tp <= 40)
    dr = np.mod(dr, 360.0)
    hs = np.where(good, hs, np.nan)
    order = np.argsort(np.where(np.isnan(hs), -1.0, hs), axis=1)[:, ::-1]        # the dominant (highest) first
    hs = np.take_along_axis(hs, order, axis=1)[:, :F.SWELL_PARTS]
    tp = np.take_along_axis(np.where(good, tp, np.nan), order, axis=1)[:, :F.SWELL_PARTS]
    dr = np.take_along_axis(np.where(good, dr, np.nan), order, axis=1)[:, :F.SWELL_PARTS]
    ok = np.isfinite(t) & np.isfinite(lat) & np.isfinite(lon) & (np.abs(lat) <= 90) & np.isfinite(hs[:, 0])
    if t0 is not None:
        ok &= A.in_window(t, t0, t1)
    idx = np.nonzero(ok)[0]
    idx = idx[np.argsort(t[idx], kind="stable")]
    pad = F.SWELL_PARTS - hs.shape[1]
    if pad > 0:
        hs, tp, dr = (np.pad(x, ((0, 0), (0, pad)), constant_values=np.nan) for x in (hs, tp, dr))
    return {"t": t[idx], "lat": lat[idx], "lon": lon[idx], "hs": hs[idx], "tp": tp[idx], "dir": dr[idx]}


def build_swell(window, w0, w1, read, missions, frames, build_id, inputs, log=print):
    """One window's chunk (magic SWL1) from the points per file -> (bytes, record, stats). No model column."""
    by_tok = {}
    for name in sorted(read):
        tok, d = read[name]
        by_tok.setdefault(tok, []).append(d)
    ids = {m["id"]: i for i, m in enumerate(missions)}
    segs, cols = [], {k: [] for k in ("t", "lat", "lon", "hs", "tp", "dir", "mission")}
    pos = 0
    for tok in sorted(by_tok, key=lambda k: ids[k]):
        parts = by_tok[tok]
        cat = {k: np.concatenate([p[k] for p in parts]) for k in ("t", "lat", "lon", "hs", "tp", "dir")}
        o = np.argsort(cat["t"], kind="stable")
        n = len(o)
        if not n:
            continue
        segs.append([ids[tok], pos, n])
        pos += n
        for k in cat:
            cols[k].append(cat[k][o])
        cols["mission"].append(np.full(n, ids[tok], np.uint8))
    empty = lambda shape: np.zeros(shape)                                # noqa: E731
    t = np.concatenate(cols["t"]) if cols["t"] else empty(0)
    lat = np.concatenate(cols["lat"]) if cols["lat"] else empty(0)
    lon = np.concatenate(cols["lon"]) if cols["lon"] else empty(0)
    hs = np.concatenate(cols["hs"]) if cols["hs"] else empty((0, F.SWELL_PARTS))
    tp = np.concatenate(cols["tp"]) if cols["tp"] else empty((0, F.SWELL_PARTS))
    dr = np.concatenate(cols["dir"]) if cols["dir"] else empty((0, F.SWELL_PARTS))
    mission = np.concatenate(cols["mission"]) if cols["mission"] else np.zeros(0, np.uint8)
    npart = np.isfinite(hs).sum(axis=1).astype(np.uint8)
    near = A.near_land_of(lat, lon) if len(t) else np.zeros(0, bool)
    arrays = {"t": F.to_stored(t, 1, None, "u4"), "lat": F.to_stored(lat, 1e-6, None, "i4"), "lon": F.to_stored(lon, 1e-6, None, "i4"),
              "n": npart, "mission": mission.astype(np.uint8), "flags": (near.astype(np.uint8) * F.FLAG_NEAR_LAND)}
    for k in range(F.SWELL_PARTS):
        arrays[f"hs{k + 1}"] = F.to_stored(hs[:, k], 0.001, F.NONE_U2, "u2")
        arrays[f"tp{k + 1}"] = F.to_stored(tp[:, k], 0.01, F.NONE_U2, "u2")
        arrays[f"dir{k + 1}"] = F.to_stored(np.mod(dr[:, k], 360.0), 0.01, F.NONE_U2, "u2")
    newest = float(t.max()) if len(t) else None
    header = {"window": window, "t0": A.utc(w0), "t1": A.utc(w1), "build": build_id, "missions": missions, "segments": segs,
              "parts": F.SWELL_PARTS, "period": "peak", "direction": "from", "inputs": inputs, "credit": SOURCE["credit"],
              "flags": {"near_land": F.FLAG_NEAR_LAND}}
    body = F.encode_chunk(F.MAGIC_SWELL, header, {}, arrays)
    per_mission = {m["id"]: int((mission == i).sum()) for i, m in enumerate(missions) if (mission == i).any()}
    dom = hs[:, 0][np.isfinite(hs[:, 0])] if len(t) else np.zeros(0)
    stats = {"window": window, "n": int(len(t)), "missions": per_mission, "bytes": len(body), "near_land": int(near.sum()),
             "partitions": {str(k): int((npart == k).sum()) for k in range(F.SWELL_PARTS + 1)},
             "dominant_hs": {"median": round(float(np.median(dom)), 3) if len(dom) else None, "max": round(float(dom.max()), 3) if len(dom) else None}}
    record = {"window": window, "t0": A.utc(w0), "t1": A.utc(w1), "n": int(len(t)), "bytes": len(body), "build": build_id,
              "missions": sorted(per_mission), "newest_utc": A.utc(A.EPOCH + A.timedelta(seconds=newest)) if newest is not None else None,
              "inputs": inputs}
    return body, record, stats


SWELL = A.Product(prefix=PREFIX, fmt=FORMAT, magic=F.MAGIC_SWELL, stac_id=STAC_ID, dataset_re=DATASET_RE, file_re=FILE_RE,
                  reader=read_swell, missions=MISSIONS, source=SOURCE, columns=F.SWELL_COLUMNS, collocate=False, build=build_swell,
                  format_key="swell-v1/1", label="Sentinel-1 swell", max_bytes=MAX_FILE_BYTES, token_from_file=False, stale_h=STALE_H)


def main(argv=None, get=A._get, store=None, now=None):
    argv = sys.argv[1:] if argv is None else list(argv)
    if argv[:1] == ["--inspect"] and len(argv) == 2:
        inspect(argv[1])
        return 0
    return A.main(argv, get=get, store=store, product=SWELL, now=now)


if __name__ == "__main__":
    sys.exit(main())
