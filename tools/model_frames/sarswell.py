"""Sentinel-1 swell partitions job (plan sections 74 and 81): the last ~30 h of SAR wave-mode swell partitions (height,
PEAK period, direction FROM) -> R2 under sarswell/v1/ as one chunk per 3-hour window (altfmt: magic SWL1), on the
machinery of the altimeter heights job (alt.py: STAC + bucket listing, the plan, downloads, publish, prune, stale).

Source: Copernicus Marine WAVE_GLO_PHY_SPC-FWK_L3_NRT_014_002, the Sentinel-1 A / C (and CFOSAT when flowing) datasets,
anonymous on the open CloudFerro bucket. Credit as the heights (E.U. Copernicus Marine Service Information, doi
10.48670/moi-00178). Verified against a real file on 2026-11-04 (plan section 81, step 1b):
  - a dataset folder holds two kinds of file: the 3-hour observation files `dataset-wav-sar-l3-spc-nrt-global-<sat>_
    <t0>Z_<t1>Z_P<production>Z-3H-nrt.nc` (FILE_RE, the only ones read) and the propagated storm-source ("fireworks")
    files `..._<t0>Z_<t1>Z_P..Z_<storm id>-nrt.nc` (a 20-day span, 4-digit stamps: never matched);
  - the observations are the group `obs_params` (OBS_GROUP), one ROW PER PARTITION of one SAR imagette: time (days
    since 1950), latitude / longitude, VAVH (m), VTPK (s; standard_name ..._period_at_variance_spectral_density_maximum
    = the PEAK period), VPED (degree; standard_name ..._from_direction_at_... and "the direction from which the waves
    arrive, in degrees from North, clockwise" = FROM), the product's per-partition quality flag
    L2_partition_quality_flag (0 very_good .. 4 poor; QUALITY_KEEP keeps 0-2) and the per-variable coherency flags
    (VAVH_flag 0 poor / 1 passed, VTPK_flag and VPED_flag 0-5; counted in the stats, not filtered: 38 % of a real
    file's partitions fail the Hs coherency filter while the product's own flag calls them very good). The rows of one
    point share its exact time and position: they are grouped into points of up to SWELL_PARTS partitions, the dominant
    (highest) first. The file's root arrays (VAVH, VPED, ... of propagation dates x partitions) are the same partitions
    propagated along their great circles: left out (a forecast along the swell's path; owner's call later);
  - one real 3-hour file: 93 partitions at 86 points (at most 2 per point), heights 0.3-5.3 m, peak periods 12-20 s;
  - ONE BATCH A DAY, produced about 23Z, covering the 36 h up to about 18Z that day (the windows of the day before are
    produced again under the new stamp: their digests change, they are rebuilt): the newest point is 5-31 h old
    (STALE_H 48). So the job keeps KEEP_H 56 h, not the heights' 30: the 24 h before the newest point (the client's
    window) stay in reach until the next batch lands (at most 19 windows; MAX_CHUNKS 20 on both sides).
    Sentinel-1B's dataset ended in 2020, Sentinel-1A's in April 2026; Sentinel-1C and CFOSAT flow today.

Per point: t, lat, lon, n partitions, mission, flags, then SWELL_PARTS x (hs m, tp s, dir degrees true FROM). The reader
refuses a file whose period is not a PEAK period or whose direction is not the direction the swell comes FROM (the MFWAM
lesson: the variables' attributes say which; a mean period stops the job), a file without the `obs_params` group (the
root holds the propagated arrays under the same names) and a time axis in neither days since 1950 nor seconds since
2000. `--inspect FILE` prints a file's variables.
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
          "doi": "10.48670/moi-00178", "url": "https://doi.org/10.48670/moi-00178",
          "credit": "Generated using E.U. Copernicus Marine Service Information; https://doi.org/10.48670/moi-00178"}
# The observation datasets: one per satellite (ids cmems-obs-wave_glo_phy_spc-fwk_nrt_<sat>_l3_PT15S-i_<version>).
DATASET_RE = re.compile(r"spc-fwk_nrt_(?P<token>s1[a-d]|cfo[a-z0-9]*)_l3_PT15S", re.I)
# The 3-hour observation files: `..._<t0>Z_<t1>Z_P<production YYYYMMDDTHHMM>Z-3H-nrt.nc`; the mission comes from the
# dataset (token_from_file False). The fireworks files (4-digit stamps, a storm id) never match.
FILE_RE = re.compile(r"_(?P<t0>\d{8}T\d{6})Z_(?P<t1>\d{8}T\d{6})Z_P(?P<prod>\d{8}T\d{4})Z-3H-nrt\.nc$")
MISSIONS = {"s1a": ("Sentinel-1A", "Sentinel-1A SAR wave mode"), "s1b": ("Sentinel-1B", "Sentinel-1B SAR wave mode"),
            "s1c": ("Sentinel-1C", "Sentinel-1C SAR wave mode"), "s1d": ("Sentinel-1D", "Sentinel-1D SAR wave mode"),
            "cfo": ("CFOSAT", "CFOSAT SWIM"), "cfosat": ("CFOSAT", "CFOSAT SWIM")}
MAX_FILE_BYTES = 50_000_000                   # a real 3-hour file is 2-10 MB (the spectra are inside)
STALE_H = 48                                  # one batch a day, 5-7 h behind its last window: the newest point is 5-31 h old between them
KEEP_H = 56                                   # the newest point at most 31 h old + the client's 24 h window + slack
OBS_GROUP = "obs_params"
VARS = {"time": "time", "lat": "latitude", "lon": "longitude", "hs": "VAVH", "tp": "VTPK", "dir": "VPED"}
QUALITY_VAR = "L2_partition_quality_flag"     # 0 very_good, 1 good, 2 medium, 3 low, 4 poor
QUALITY_KEEP = 2
COHERENCY_FLAGS = {"hs": "VAVH_flag", "tp": "VTPK_flag", "dir": "VPED_flag"}
TIME_1950_RE = re.compile(r"^days since 1950-01-01[ T]00:00:00")
TIME_2000_RE = re.compile(r"^seconds since 2000-01-01[ T]00:00:00")
DAYS_1950_TO_2000 = 18262
# The period and direction conventions are read from the variables' standard_name first (the CF names say it outright),
# else their long_name; a phrase about the reference ("clockwise from true north") is removed before the words are
# looked for; a variable whose text says both (peak and mean, from and towards) is refused.
PERIOD_PEAK_STD = re.compile(r"variance_spectral_density_maximum|peak_period", re.I)
PERIOD_MEAN_STD = re.compile(r"mean_period|zero_upcrossing|energy_period|first_frequency_moment|second_frequency_moment", re.I)
PERIOD_PEAK_RE = re.compile(r"\bpeak\b", re.I)
PERIOD_MEAN_RE = re.compile(r"\bmean\b|\benergy\b|zero[- ]?(up)?crossing|\bt0?m[0-9]{1,2}\b|\baverage\b", re.I)
DIR_FROM_STD = re.compile(r"from_direction", re.I)
DIR_TO_STD = re.compile(r"to_direction", re.I)
DIR_REF_RE = re.compile(r"(clockwise|counter-?clockwise|measured|degrees?|deg)\s+from\s+(true\s+|geographic\s+)?north|\bfrom\s+(true\s+|geographic\s+)?north", re.I)
DIR_FROM_RE = re.compile(r"\bfrom\b|\bcoming\b|\borigin", re.I)
DIR_TO_RE = re.compile(r"\btowards?\b|\bto\b|\bgoing\b|propagat", re.I)


def convention(std, long_name, std_pos, std_neg, pos_re, neg_re, strip_re=None):
    """'pos', 'neg' or None (unstated / contradictory) from a variable's standard_name (decisive) else its long_name."""
    std = std or ""
    if std_pos.search(std) and not std_neg.search(std):
        return "pos"
    if std_neg.search(std) and not std_pos.search(std):
        return "neg"
    text = long_name or ""
    if strip_re is not None:
        text = strip_re.sub(" ", text)
    p, q = bool(pos_re.search(text)), bool(neg_re.search(text))
    return "pos" if p and not q else "neg" if q and not p else None


def _text(ds):
    """A variable's standard_name and long_name (the convention checks read these two, nothing else)."""
    out = {}
    for k in ("standard_name", "long_name"):
        v = A._attr(ds, k)
        if isinstance(v, (bytes, np.bytes_)):
            v = v.decode("utf-8", "replace")
        out[k] = v if isinstance(v, str) else ""
    return out


def inspect(path):
    """Print every variable with its shape and attributes (the real-file check of plan section 81, step 1)."""
    import h5py
    with h5py.File(path, "r") as f:
        print("global:", {k: A._attr(f, k) for k in f.attrs})

        def visit(name, obj):
            if isinstance(obj, h5py.Dataset):
                print(f"{name}: shape {obj.shape} dtype {obj.dtype} attrs {dict((k, A._attr(obj, k)) for k in obj.attrs)}")
        f.visititems(visit)


def _seconds_since_2000(tv):
    units = A._attr(tv, "units", "")
    if isinstance(units, str) and TIME_1950_RE.match(units):
        return (A._scaled(tv) - DAYS_1950_TO_2000) * 86400.0
    if isinstance(units, str) and TIME_2000_RE.match(units):
        return A._scaled(tv)
    raise ValueError(f"time units {units!r} are neither days since 1950-01-01 nor seconds since 2000-01-01")


def _flag(g, name, n):
    return g[name][()] if name in g else np.zeros(n, np.int8)


def read_swell(path, t0=None, t1=None):
    """A SPC-FWK L3 observation file -> {t, lat, lon, hs (n x P), tp (n x P), dir (n x P, degrees true FROM), quality}
    of the points with at least one partition kept, within the window when given, time-sorted; the partitions of a point
    sorted by height, the dominant first, at most SWELL_PARTS kept. The group `obs_params` holds one row per partition;
    the rows sharing a time and position are one point. A partition is kept when its L2_partition_quality_flag is at
    most QUALITY_KEEP and its height, period and direction are finite and plausible. `quality` counts the rows: read,
    kept, per quality grade, failing each coherency flag (for the stats). Refuses: a period that is not a PEAK period,
    a direction that is not the direction the swell comes FROM (by the variables' attributes), a file without the
    observation group, a time axis in neither days since 1950 nor seconds since 2000."""
    import h5py
    with h5py.File(path, "r") as f:
        if OBS_GROUP not in f:
            raise ValueError(f"no {OBS_GROUP} group (the root holds the propagated arrays, not the observations)")
        g = f[OBS_GROUP]
        for k in VARS.values():
            if k not in g:
                raise ValueError(f"{OBS_GROUP}/{k} not found")
        t = _seconds_since_2000(g[VARS["time"]])
        lat = A._scaled(g[VARS["lat"]], expect_units="degrees_north")
        lon = A._scaled(g[VARS["lon"]], expect_units="degrees_east")
        hs = A._scaled(g[VARS["hs"]], expect_units="m")
        tp = A._scaled(g[VARS["tp"]], expect_units="s")
        dr = A._scaled(g[VARS["dir"]], expect_units="degree")
        ptext, dtext = _text(g[VARS["tp"]]), _text(g[VARS["dir"]])
        n = len(t)
        qual = _flag(g, QUALITY_VAR, n)
        coh = {k: g[v][()] for k, v in COHERENCY_FLAGS.items() if v in g}      # counted where present (0 = poor)
    per = convention(ptext["standard_name"], ptext["long_name"], PERIOD_PEAK_STD, PERIOD_MEAN_STD, PERIOD_PEAK_RE, PERIOD_MEAN_RE)
    if per != "pos":
        raise ValueError(f"the partition period is not a peak period ({ptext}); the swell job stops here")
    dr_conv = convention(dtext["standard_name"], dtext["long_name"], DIR_FROM_STD, DIR_TO_STD, DIR_FROM_RE, DIR_TO_RE, DIR_REF_RE)
    if dr_conv == "neg":
        raise ValueError(f"the partition direction is not the direction the swell comes from ({dtext})")
    if dr_conv is None:
        raise ValueError(f"the partition direction's convention is not stated ({dtext}); verify the file first")
    if not (lat.shape == lon.shape == hs.shape == tp.shape == dr.shape == (n,)):
        raise ValueError("time / position / partition shapes differ (one row per partition expected)")
    lon = (lon + 180.0) % 360.0 - 180.0
    good = (np.isfinite(t) & np.isfinite(lat) & np.isfinite(lon) & (np.abs(lat) <= 90) & np.isfinite(hs) & np.isfinite(tp) & np.isfinite(dr)
            & (hs > 0) & (hs <= 30) & (tp >= 1) & (tp <= 40) & (qual >= 0) & (qual <= QUALITY_KEEP))
    quality = {"rows": int(n), "kept": int(good.sum()),
               "grades": {str(k): int((qual == k).sum()) for k in range(5)},
               "coherency_failed": {k: int(((coh[k] == 0) & good).sum()) if k in coh else None for k in COHERENCY_FLAGS}}
    # the rows of a point share its exact time and position: group them, the kept partitions first, highest first
    key = np.stack([np.where(np.isfinite(t), t, -1e18), np.where(np.isfinite(lat), lat, 999.0), lon], axis=1)
    _, inv = np.unique(key, axis=0, return_inverse=True)
    inv = inv.reshape(-1)
    order = np.lexsort((np.where(good, -hs, 1.0), inv))             # by point, then the highest kept partition first
    inv_s = inv[order]
    starts = np.flatnonzero(np.r_[True, inv_s[1:] != inv_s[:-1]])
    rank = np.arange(n) - np.repeat(starts, np.diff(np.r_[starts, n]))
    npts = len(starts)
    P = F.SWELL_PARTS
    out = {k: np.full((npts, P), np.nan) for k in ("hs", "tp", "dir")}
    take = (rank < P) & good[order]
    rows, slot = order[take], rank[take]
    pt = inv[rows]                                                   # the group ids are 0..npts-1 in group order
    out["hs"][pt, slot], out["tp"][pt, slot], out["dir"][pt, slot] = hs[rows], tp[rows], np.mod(dr[rows], 360.0)
    first = order[starts]
    pt_t, pt_lat, pt_lon = t[first], lat[first], lon[first]
    ok = np.isfinite(out["hs"][:, 0])
    if t0 is not None:
        ok &= A.in_window(pt_t, t0, t1)
    idx = np.nonzero(ok)[0]
    idx = idx[np.argsort(pt_t[idx], kind="stable")]
    return {"t": pt_t[idx], "lat": pt_lat[idx], "lon": pt_lon[idx], "hs": out["hs"][idx], "tp": out["tp"][idx], "dir": out["dir"][idx],
            "quality": quality}


def build_swell(window, w0, w1, read, missions, frames, build_id, inputs, log=print):
    """One window's chunk (magic SWL1) from the points per file -> (bytes, record, stats). No model column."""
    by_tok = {}
    quality = {"rows": 0, "kept": 0, "grades": {}, "coherency_failed": {}}
    for name in sorted(read):
        tok, d = read[name]
        by_tok.setdefault(tok, []).append(d)
        q = d.get("quality") or {}
        quality["rows"] += int(q.get("rows", 0))
        quality["kept"] += int(q.get("kept", 0))
        for k, v in (q.get("grades") or {}).items():
            quality["grades"][k] = quality["grades"].get(k, 0) + int(v)
        for k, v in (q.get("coherency_failed") or {}).items():
            if v is not None:
                quality["coherency_failed"][k] = quality["coherency_failed"].get(k, 0) + int(v)
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
              "flags": {"near_land": F.FLAG_NEAR_LAND}, "quality": {"flag": QUALITY_VAR, "kept_up_to": QUALITY_KEEP}}
    body = F.encode_chunk(F.MAGIC_SWELL, header, {}, arrays)
    per_mission = {m["id"]: int((mission == i).sum()) for i, m in enumerate(missions) if (mission == i).any()}
    dom = hs[:, 0][np.isfinite(hs[:, 0])] if len(t) else np.zeros(0)
    stats = {"window": window, "n": int(len(t)), "missions": per_mission, "bytes": len(body), "near_land": int(near.sum()),
             "partitions": {str(k): int((npart == k).sum()) for k in range(F.SWELL_PARTS + 1)},
             "dominant_hs": {"median": round(float(np.median(dom)), 3) if len(dom) else None, "max": round(float(dom.max()), 3) if len(dom) else None},
             "quality": quality}
    record = {"window": window, "t0": A.utc(w0), "t1": A.utc(w1), "n": int(len(t)), "bytes": len(body), "build": build_id,
              "missions": sorted(per_mission), "newest_utc": A.utc(A.EPOCH + A.timedelta(seconds=newest)) if newest is not None else None,
              "inputs": inputs}
    return body, record, stats


SWELL = A.Product(prefix=PREFIX, fmt=FORMAT, magic=F.MAGIC_SWELL, stac_id=STAC_ID, dataset_re=DATASET_RE, file_re=FILE_RE,
                  reader=read_swell, missions=MISSIONS, source=SOURCE, columns=F.SWELL_COLUMNS, collocate=False, build=build_swell,
                  format_key="swell-v1/2", label="Sentinel-1 swell", max_bytes=MAX_FILE_BYTES, token_from_file=False, stale_h=STALE_H,
                  keep_h=KEEP_H)


def main(argv=None, get=A._get, store=None, now=None):
    argv = sys.argv[1:] if argv is None else list(argv)
    if argv[:1] == ["--inspect"] and len(argv) == 2:
        inspect(argv[1])
        return 0
    return A.main(argv, get=get, store=store, product=SWELL, now=now)


if __name__ == "__main__":
    sys.exit(main())
