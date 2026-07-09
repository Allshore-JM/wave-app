"""CDIP 106 swell-band client (with an NDBC 51201 transport fallback -- same buoy).

The reef page's condition axes PROMISE swell-band Hs (integrated over a per-spot frequency
band) and energy-weighted a1/b1 direction. Nothing else in either repo delivers that: wave-app
partitions are peak-derived, .spec directions are compass STRINGS, and CDIP ERDDAP is bulk-only.
So this reads the raw directional spectrum from the CDIP realtime OPeNDAP `.ascii` endpoint and
integrates it here.

Transport: two GETs -- `.dds` for the record count N (the realtime file has no [last] support),
then an `.ascii` hyperslab of the last few records (fetch 4 so we can step back past a QC-rejected
newest record). Variable names are `waveA1Value`/`waveB1Value` (not `waveA1`); the `.VAR.VAR`
projection returns the bare array (half the payload). Direction is `sea_surface_wave_from_direction`,
true north, so dir = atan2(b1,a1) with NO 270-theta flip (verified band-for-band vs waveMeanDirection).

Vendored VERBATIM into the wave-app repo root alongside reef_live.py.
"""
from __future__ import annotations

import math
import re

import numpy as np
import requests

CDIP_BASE = "https://thredds.cdip.ucsd.edu/thredds/dodsC/cdip/realtime/106p1_rt.nc"
_GOOD_FLAGS = (1, 2)                 # CDIP primary QC: good / not_evaluated
_SAMPLE_MIDPOINT_S = 800             # waveTime is the START of a ~1600 s sample
FT_PER_M = 3.280839895


# ---- pure band math (unit-testable, no I/O) --------------------------------

def dir_from_a1b1(a1_bar: float, b1_bar: float) -> float:
    """Energy-weighted mean 'from' direction, degrees true, [0,360)."""
    return (math.degrees(math.atan2(b1_bar, a1_bar)) + 360.0) % 360.0


def ndbc_a1b1(alpha1_deg: float, r1: float) -> tuple[float, float]:
    """Reconstruct (a1, b1) from an NDBC .swdir mean direction alpha1 (deg) + .swr1 r1 (0-1).
    Verified against CDIP to +-0.02 on all bands."""
    a = math.radians(alpha1_deg)
    return r1 * math.cos(a), r1 * math.sin(a)


def band_indices(freqs, f_lo: float, f_hi: float, freq_flags=None, good=_GOOD_FLAGS) -> list[int]:
    """Bands whose CENTRE lies in [f_lo, f_hi] and whose per-band QC flag is good. f_hi is
    chosen to land on a CDIP band upper bound so centre-inclusion matches the tp axis exactly."""
    idx = []
    for i, f in enumerate(freqs):
        if f_lo <= f <= f_hi and (freq_flags is None or int(freq_flags[i]) in good):
            idx.append(i)
    return idx


def band_stats(freqs, bandwidths, energy, a1, b1, idx, check_factor=None) -> dict:
    """Integrate one directional-spectrum record over the band indices `idx`."""
    freqs = np.asarray(freqs, float); bw = np.asarray(bandwidths, float)
    E = np.asarray(energy, float); A = np.asarray(a1, float); B = np.asarray(b1, float)
    # Sanitize fill values / NaN so a netCDF _FillValue (9.96921e36) or a sentinel cannot
    # fabricate a monster swell (positive fill -> huge m0 -> top Hs bin) or collapse a real
    # reading (NaN -> m0 NaN -> Hs 0). Treat a non-physical band as no energy.
    E = np.where(np.isfinite(E) & (E >= 0) & (E < 1e5), E, 0.0)
    A = np.where(np.isfinite(A), A, 0.0); B = np.where(np.isfinite(B), B, 0.0)
    if not idx:
        return {"m0": 0.0, "hs_m": 0.0, "hs_ft": 0.0, "tp_s": None, "dir_deg": None,
                "r1_bar": 0.0, "spread_deg": None, "band_m0_frac": 0.0, "bands_used": 0}
    w = E[idx] * bw[idx]                         # per-band m0 contribution
    m0 = float(w.sum())
    hs_m = 4.0 * math.sqrt(m0) if m0 > 0 else 0.0
    m0_total = float((E * bw).sum())
    if m0 > 0:
        a1_bar = float((w * A[idx]).sum() / m0)
        b1_bar = float((w * B[idx]).sum() / m0)
    else:
        a1_bar = b1_bar = 0.0
    r1_bar = math.hypot(a1_bar, b1_bar)
    dir_deg = dir_from_a1b1(a1_bar, b1_bar) if m0 > 0 else None
    # circular spread from r1 (Kuik): sqrt(2(1-r1)); degrees
    spread = math.degrees(math.sqrt(max(0.0, 2.0 * (1.0 - r1_bar)))) if m0 > 0 else None
    tp_s = float(1.0 / freqs[idx][int(np.argmax(E[idx]))]) if m0 > 0 else None
    out = {"m0": m0, "hs_m": round(hs_m, 3), "hs_ft": round(hs_m * FT_PER_M, 2),
           "tp_s": round(tp_s, 1) if tp_s else None,
           "dir_deg": round(dir_deg, 1) if dir_deg is not None else None,
           "r1_bar": round(r1_bar, 3),
           "spread_deg": round(spread, 1) if spread is not None else None,
           "band_m0_frac": round(m0 / m0_total, 3) if m0_total > 0 else 0.0,
           "bands_used": len(idx)}
    if check_factor is not None:
        cf = np.asarray(check_factor, float)[idx]
        out["check_factor_weighted"] = round(float((w * cf).sum() / m0), 2) if m0 > 0 else None
        out["check_factor_max"] = round(float(cf.max()), 2)
        out["cf_saturated_m0_frac"] = round(float(w[cf >= 2.5].sum() / m0), 3) if m0 > 0 else 0.0
    return out


def bimodality(freqs, bandwidths, energy, a1, b1, idx, sep_deg=45.0, minor_m0_frac=0.25) -> dict:
    """Detect two swell systems in the band by scanning EVERY interior frequency split and
    keeping the one that best separates two systems (max separation x minor-fraction). A single
    median split silently fails when the band's energy concentrates in its top band (the common
    trade-windswell-peaking-at-8-s shape) -- one half becomes empty and the gate never fires,
    reporting a confident but non-physical mean direction. The interior-split scan cannot
    degenerate: both halves always have >=1 band. A bimodal band's mean direction is meaningless
    (a long-period swell + a windswell average to a bearing near neither)."""
    if len(idx) < 2:
        return {"bimodal": False, "separation_deg": None}
    freqs = np.asarray(freqs, float); bw = np.asarray(bandwidths, float); E = np.asarray(energy, float)
    order = [idx[k] for k in np.argsort(freqs[idx])]     # band indices, low->high frequency
    best = None
    for s in range(1, len(order)):                       # interior split: [:s] | [s:], both non-empty
        s_lo = band_stats(freqs, bw, E, a1, b1, order[:s])
        s_hi = band_stats(freqs, bw, E, a1, b1, order[s:])
        if s_lo["dir_deg"] is None or s_hi["dir_deg"] is None or s_lo["m0"] + s_hi["m0"] <= 0:
            continue
        sep = abs((s_lo["dir_deg"] - s_hi["dir_deg"] + 180) % 360 - 180)
        minor = min(s_lo["m0"], s_hi["m0"]) / (s_lo["m0"] + s_hi["m0"])
        score = sep * minor
        if best is None or score > best[0]:
            best = (score, sep, minor, [s_lo["dir_deg"], s_hi["dir_deg"]])
    if best is None:
        return {"bimodal": False, "separation_deg": None}
    _, sep, minor, dirs = best
    return {"bimodal": bool(sep > sep_deg and minor > minor_m0_frac),
            "separation_deg": round(sep, 1), "minor_frac": round(minor, 2), "sub_dirs_deg": dirs}


def conditions_from_record(freqs, bandwidths, energy, a1, b1, check_factor, freq_flags,
                           band_hz) -> dict:
    """Full swell-band conditions from ONE spectrum record + the per-spot band. Pure."""
    f_lo, f_hi = band_hz
    idx = band_indices(freqs, f_lo, f_hi, freq_flags)
    stats = band_stats(freqs, bandwidths, energy, a1, b1, idx, check_factor)
    bmod = bimodality(freqs, bandwidths, energy, a1, b1, idx)
    masked = [round(float(f), 4) for i, f in enumerate(freqs)
              if f_lo <= f <= f_hi and int(freq_flags[i]) not in _GOOD_FLAGS]
    return {**stats, **bmod, "band_hz": [f_lo, f_hi], "bands_masked": len(masked),
            "bands_masked_hz": masked}


# ---- OPeNDAP .ascii transport ----------------------------------------------

def _parse_dods_ascii(text: str) -> dict:
    """Parse a DODS `.ascii` body into {var_name: np.ndarray}. Data section follows the
    dashed separator; 1-D vars are one comma line, 2-D vars are '[i], v, v, ...' rows."""
    data = re.split(r"^-{5,}\s*$", text, flags=re.M)[-1]
    out: dict = {}
    for block in re.split(r"\n\s*\n", data.strip()):
        lines = [ln for ln in block.splitlines() if ln.strip()]
        if not lines:
            continue
        m = re.match(r"^(\w+)\[", lines[0])
        if not m:
            continue
        name, rows = m.group(1), lines[1:]
        if rows and rows[0].lstrip().startswith("["):
            arr = [[float(x) for x in r.split(",")[1:]] for r in rows]
            out[name] = np.array(arr, float)
        else:
            out[name] = np.array([float(x) for x in ",".join(rows).split(",")], float)
    return out


_VARS = ("waveTime", "waveFlagPrimary", "waveFrequency", "waveFrequencyFlagPrimary",
         "waveBandwidth", "waveEnergyDensity", "waveA1Value", "waveB1Value", "waveCheckFactor")


def fetch_cdip(session: requests.Session, band_hz, n_records: int = 4,
               timeout=(3.05, 6.0)) -> dict:
    """Fetch the last `n_records` CDIP 106 spectra and return the NEWEST QC-good record's
    swell-band conditions. Raises on transport failure or all-bad records; the caller falls
    back to NDBC. Two GETs (.dds for N, then the .ascii slice)."""
    dds = session.get(CDIP_BASE + ".dds", timeout=timeout).text
    mm = re.search(r"waveTime\s*=\s*(\d+)", dds)
    if not mm:
        raise RuntimeError("CDIP .dds: no waveTime dimension")
    n = int(mm.group(1))
    a, b = max(0, n - n_records), n - 1
    proj = (f".ascii?waveTime[{a}:1:{b}],waveFlagPrimary[{a}:1:{b}],"
            f"waveFrequency[0:1:63],waveFrequencyFlagPrimary[0:1:63],waveBandwidth[0:1:63],"
            f"waveEnergyDensity.waveEnergyDensity[{a}:1:{b}][0:1:63],"
            f"waveA1Value.waveA1Value[{a}:1:{b}][0:1:63],"
            f"waveB1Value.waveB1Value[{a}:1:{b}][0:1:63],"
            f"waveCheckFactor.waveCheckFactor[{a}:1:{b}][0:1:63]")
    d = _parse_dods_ascii(session.get(CDIP_BASE + proj, timeout=timeout).text)
    if not all(v in d for v in ("waveTime", "waveFrequency", "waveEnergyDensity")):
        raise RuntimeError("CDIP .ascii: missing variables")
    times, flags = d["waveTime"], d["waveFlagPrimary"]
    freqs, fflags, bw = d["waveFrequency"], d["waveFrequencyFlagPrimary"], d["waveBandwidth"]
    E, A, B, CF = (d["waveEnergyDensity"], d["waveA1Value"], d["waveB1Value"], d["waveCheckFactor"])
    # step back from the newest record until one passes the primary QC flag
    for r in range(len(times) - 1, -1, -1):
        if int(flags[r]) in _GOOD_FLAGS:
            cond = conditions_from_record(freqs, bw, E[r], A[r], B[r], CF[r], fflags, band_hz)
            cond["observed_utc_epoch"] = int(times[r]) + _SAMPLE_MIDPOINT_S
            cond["record_flag"] = int(flags[r])
            cond["source"] = "cdip_106"
            cond["transport"] = "thredds_opendap"
            cond["fallback"] = False
            return cond
    raise RuntimeError("CDIP: no QC-good record in the last "
                       f"{len(times)} (flags {[int(f) for f in flags]})")
