"""Phase 0 spike: decode + encode ONE forecast step of each overlay field on a Linux runner.

Measures (and prints as a Markdown table) what the plan marked as assumptions: JPEG2000 GRIB
decoding via conda-forge eccodes, decode time, 8-bit PNG sizes (full 1440x721 and half 720x361),
and the download volume per step. Writes the PNGs + a JSON report to ./spike_out for the workflow
artifact. Network: NOAA's open S3 bucket only (byte-range reads), never NOMADS.

Usage: python tools/model_frames/spike.py [--step 24] [--out spike_out]
"""
import argparse
import io
import json
import math
import os
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import numpy as np
from PIL import Image

S3 = "https://noaa-gfs-bdp-pds.s3.amazonaws.com"
FIELDS = {
    # name: (product, grib variable match in .idx, fixed colour range, units)
    "hs":   ("wave",  ["HTSGW:surface"], (0.0, 12.0), "m"),
    "tp":   ("wave",  ["PERPW:surface"], (4.0, 22.0), "s"),
    "wind": ("atmos", ["UGRD:10 m above ground", "VGRD:10 m above ground"], (0.0, 30.87), "m/s"),  # 60 kt
}


def _get(url, rng=None, timeout=60):
    req = urllib.request.Request(url, headers={"Range": rng} if rng else {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def urls(run_dt, product, step):
    d, h = run_dt.strftime("%Y%m%d"), run_dt.strftime("%H")
    if product == "wave":
        return f"{S3}/gfs.{d}/{h}/wave/gridded/gfswave.t{h}z.global.0p25.f{step:03d}.grib2"
    return f"{S3}/gfs.{d}/{h}/atmos/gfs.t{h}z.pgrb2.0p25.f{step:03d}"


def latest_complete_run(step):
    """Newest cycle whose wave AND atmos files for `step` have an .idx on S3."""
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    for back in range(0, 8):
        cand = now - timedelta(hours=back * 6)
        cand = cand.replace(hour=(cand.hour // 6) * 6)
        ok = True
        for product in ("wave", "atmos"):
            try:
                urllib.request.urlopen(urls(cand, product, step) + ".idx", timeout=30).read(200)
            except Exception:
                ok = False
                break
        if ok:
            return cand
    raise RuntimeError("no complete run found in the last 48 h")


def fetch_records(url, matches):
    """Byte-range fetch of the GRIB records whose .idx line contains one of `matches`."""
    idx = _get(url + ".idx").decode().splitlines()
    parts = [l.split(":") for l in idx]
    offs = [int(p[1]) for p in parts]
    out = {}
    for i, p in enumerate(parts):
        key = f"{p[3]}:{p[4]}"
        if key in matches:
            end = offs[i + 1] - 1 if i + 1 < len(offs) else ""
            out[key] = _get(url, rng=f"bytes={offs[i]}-{end}")
    missing = [m for m in matches if m not in out]
    if missing:
        raise RuntimeError(f"records not in index: {missing}")
    return out


def decode(blob):
    """GRIB message bytes -> (float32 grid [721,1440] north->south, lon -180..179.75, meta)."""
    import eccodes
    h = eccodes.codes_new_from_message(blob)
    try:
        ni, nj = eccodes.codes_get(h, "Ni"), eccodes.codes_get(h, "Nj")
        meta = {k: eccodes.codes_get(h, k) for k in (
            "shortName", "name", "units", "typeOfLevel", "level", "stepRange", "dataDate", "dataTime",
            "latitudeOfFirstGridPointInDegrees", "longitudeOfFirstGridPointInDegrees",
            "iDirectionIncrementInDegrees", "jDirectionIncrementInDegrees", "jScansPositively",
            "packingType", "missingValue")}
        vals = eccodes.codes_get_values(h).astype(np.float64).reshape(nj, ni)
    finally:
        eccodes.codes_release(h)
    assert (ni, nj) == (1440, 721), (ni, nj)
    assert abs(meta["latitudeOfFirstGridPointInDegrees"] - 90.0) < 1e-6 and not meta["jScansPositively"]
    assert abs(meta["longitudeOfFirstGridPointInDegrees"]) < 1e-6
    vals = np.where(vals == meta["missingValue"], np.nan, vals)
    vals = np.roll(vals, -720, axis=1)                 # 0..359.75 -> -180..179.75
    return vals.astype(np.float32), meta


def encode(grid, lo, hi):
    """float grid -> uint8: 0 = missing; 1..255 linear over [lo, hi], clamped."""
    q = np.clip(np.round((grid - lo) / (hi - lo) * 254.0) + 1.0, 1, 255)
    return np.where(np.isnan(grid), 0, q).astype(np.uint8)


def decode_q(q, lo, hi):
    out = lo + (q.astype(np.float64) - 1.0) / 254.0 * (hi - lo)
    return np.where(q == 0, np.nan, out)


def png_bytes(arr, resample=None):
    im = Image.fromarray(arr, "L")
    if resample:
        im = im.resize(resample, Image.NEAREST)
    buf = io.BytesIO()
    im.save(buf, "PNG", optimize=True)
    return buf.getvalue()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--step", type=int, default=24)
    ap.add_argument("--out", default="spike_out")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    run = latest_complete_run(a.step)
    report = {"run": run.strftime("%Y-%m-%dT%HZ"), "step": a.step, "fields": {}}
    rows = []
    for name, (product, matches, (lo, hi), units) in FIELDS.items():
        url = urls(run, product, a.step)
        t = time.time()
        recs = fetch_records(url, matches)
        t_fetch = time.time() - t
        nbytes = sum(len(b) for b in recs.values())
        t = time.time()
        grids = {k: decode(b) for k, b in recs.items()}
        t_decode = time.time() - t
        if name == "wind":
            u, v = grids[matches[0]][0], grids[matches[1]][0]
            grid, meta = np.sqrt(u * u + v * v), grids[matches[0]][1]
        else:
            grid, meta = grids[matches[0]]
        t = time.time()
        q = encode(grid, lo, hi)
        full = png_bytes(q)
        half = png_bytes(q, (720, 361))
        t_encode = time.time() - t
        back = decode_q(q, lo, hi)
        inrange = (~np.isnan(grid)) & (grid >= lo) & (grid <= hi)
        max_err = float(np.nanmax(np.abs(back[inrange] - grid[inrange]))) if inrange.any() else 0.0
        quantum = (hi - lo) / 254.0
        open(os.path.join(a.out, f"{name}_full.png"), "wb").write(full)
        open(os.path.join(a.out, f"{name}_half.png"), "wb").write(half)
        info = {
            "grib": {k: (v if isinstance(v, (int, float, str)) else str(v)) for k, v in meta.items()},
            "download_bytes": nbytes, "fetch_s": round(t_fetch, 2), "decode_s": round(t_decode, 3),
            "encode_s": round(t_encode, 3), "png_full_bytes": len(full), "png_half_bytes": len(half),
            "valid_points": int(np.count_nonzero(~np.isnan(grid))), "min": float(np.nanmin(grid)),
            "max": float(np.nanmax(grid)), "range": [lo, hi], "units": units,
            "quantum": quantum, "max_roundtrip_err": max_err, "clamped_pts": int(np.count_nonzero(grid > hi)),
        }
        report["fields"][name] = info
        rows.append(f"| {name} | {meta['name']} ({meta['units']}, {meta['typeOfLevel']} {meta['level']}, {meta['packingType']}) "
                    f"| {nbytes/1e6:.2f} MB / {t_fetch:.1f}s | {t_decode*1000:.0f} ms | {len(full)/1024:.0f} KB | {len(half)/1024:.0f} KB "
                    f"| {info['min']:.2f}..{info['max']:.2f} | {max_err:.4f} (<= {quantum/2:.4f}) |")
    json.dump(report, open(os.path.join(a.out, "report.json"), "w"), indent=1)
    md = ["| field | GRIB | download | decode | PNG full | PNG half | data range | round-trip err |",
          "|---|---|---|---|---|---|---|---|", *rows]
    print(f"run {report['run']} step f{a.step:03d}\n" + "\n".join(md))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        open(summary, "a").write(f"### Spike: run {report['run']} f{a.step:03d}\n\n" + "\n".join(md) + "\n")


if __name__ == "__main__":
    sys.exit(main())
