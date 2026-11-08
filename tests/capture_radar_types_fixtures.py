"""Capture the radar job's precipitation-type fixtures (plan section 89) from one set of NOAA MRMS CONUS files of the same time:
    python -I tests/capture_radar_types_fixtures.py <SeamlessHSR .grib2.gz> <PrecipFlag .grib2.gz> <Model_WetBulbTemp .grib2.gz>
Each file is cut to the box below (1 x 2 degrees off Vancouver: rain, snow and a near-freezing band) with its own header, scales
and bit depth kept (the reflectivity 16-bit, the flag 8-bit, CONUS's wet-bulb 24-bit RGB). The expected values come from eccodes
(an independent decode) and the expected codes from the rule written out here, not from radar.py. Captured from the files of
2026-11-08 18:52 UTC (flag, reflectivity) and 17:00 UTC (wet-bulb)."""
import gzip
import hashlib
import io
import json
import os
import struct
import sys

import eccodes
import numpy as np
from PIL import Image

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIX = os.path.join(ROOT, "tests", "fixtures", "radar")
BOX = (49, -124, 50, -122)                       # south, west, north, east
BAND = (-0.5, 1.5)
RAINS = (1, 6, 7, 10, 91, 96)


def sections(raw):
    """{section number: bytes} of one GRIB2 message (section 0 = the 16-byte indicator)."""
    out, p = {0: raw[:16]}, 16
    while p < len(raw) - 4:
        n, num = struct.unpack(">IB", raw[p:p + 5])
        out[num] = raw[p:p + n]
        p += n
    return out


def decode(raw):
    h = eccodes.codes_new_from_message(raw)
    try:
        ni, nj = eccodes.codes_get(h, "Ni"), eccodes.codes_get(h, "Nj")
        vals = eccodes.codes_get_values(h).reshape(nj, ni)
        corners = (eccodes.codes_get(h, "latitudeOfFirstGridPointInDegrees"),
                   eccodes.codes_get(h, "longitudeOfFirstGridPointInDegrees"),
                   eccodes.codes_get(h, "latitudeOfLastGridPointInDegrees"),
                   eccodes.codes_get(h, "longitudeOfLastGridPointInDegrees"))
    finally:
        eccodes.codes_release(h)
    return vals, corners


def cut(path, name):
    """The file cut to BOX, written to the fixtures; returns the cut's values as eccodes decodes the WHOLE file."""
    raw = gzip.open(path).read()
    vals, _ = decode(raw)
    secs = sections(raw)
    assert secs[6][5] == 255, "a bitmap"
    s3, s5 = bytearray(secs[3]), bytearray(secs[5])
    lat1, lon1 = struct.unpack(">iI", bytes(s3[46:54]))
    di, dj = struct.unpack(">II", bytes(s3[63:71]))
    r0 = round((lat1 - (BOX[2] * 1e6 - dj / 2)) / dj)
    c0 = round(((BOX[1] + 360) * 1e6 + di / 2 - lon1) / di)
    rows, cols = round((BOX[2] - BOX[0]) * 1e6 / dj), round((BOX[3] - BOX[1]) * 1e6 / di)
    with Image.open(io.BytesIO(secs[7][5:])) as im:
        a = np.array(im)
    part = np.ascontiguousarray(a[r0:r0 + rows, c0:c0 + cols])
    if part.ndim == 2 and part.dtype != np.uint8:
        part = part.astype(np.uint16)
    buf = io.BytesIO()
    Image.fromarray(part).save(buf, "PNG")
    pic = buf.getvalue()
    s3[6:10] = struct.pack(">I", rows * cols)
    s3[30:38] = struct.pack(">II", cols, rows)
    s3[46:54] = struct.pack(">iI", lat1 - r0 * dj, lon1 + c0 * di)
    s3[55:63] = struct.pack(">iI", lat1 - (r0 + rows - 1) * dj, lon1 + (c0 + cols - 1) * di)
    s5[5:9] = struct.pack(">I", rows * cols)
    secs[3], secs[5], secs[7] = bytes(s3), bytes(s5), struct.pack(">IB", 5 + len(pic), 7) + pic
    body = b"".join(secs[k] for k in sorted(secs) if k)
    msg = secs[0][:8] + struct.pack(">Q", 16 + len(body) + 4) + body + b"7777"
    mine = vals[r0:r0 + rows, c0:c0 + cols]
    back, corners = decode(msg)                                     # the cut file read back by eccodes: the same values, the box
    assert back.shape == mine.shape and np.array_equal(back, mine), name
    want = (BOX[2] - dj / 2e6, BOX[1] + di / 2e6, BOX[0] + dj / 2e6, BOX[3] - di / 2e6)
    got = (corners[0], corners[1] - 360 if corners[1] > 180 else corners[1], corners[2],
           corners[3] - 360 if corners[3] > 180 else corners[3])
    assert all(abs(g - w) < 1e-6 for g, w in zip(got, want)), (name, got, want)
    with open(os.path.join(FIX, f"types_{name}.grib2.gz"), "wb") as fh:
        fh.write(gzip.compress(msg, mtime=0))
    return mine


def main(hsr_path, flag_path, tw_path):
    dbz, flag, tw = cut(hsr_path, "hsr"), cut(flag_path, "flag"), cut(tw_path, "tw")
    fl = np.round(flag).astype(int)
    kind = np.zeros(dbz.shape, int)
    kind[fl == 3] = 1
    inband = (tw >= BAND[0] - 1e-6) & (tw <= BAND[1] + 1e-6) & (tw > -900)
    kind[(np.isin(fl, RAINS) | (fl == 3)) & inband] = 2
    codes = np.ones(dbz.shape, int)
    codes[np.isnan(dbz) | (dbz < -900)] = 0
    e = dbz >= 5.0
    for k, c0, c1, st in ((0, 2, 142, 0.5), (1, 143, 213, 1.0), (2, 214, 249, 2.0)):
        sel = e & (kind == k)
        codes[sel] = np.clip(c0 + np.round((dbz[sel] - 5) / st), c0, c1)
    Image.fromarray(codes.astype(np.uint8)).save(os.path.join(FIX, "types_expect.png"))
    rng = np.random.default_rng(89)
    pts = [(int(r), int(c)) for r, c in zip(rng.integers(0, dbz.shape[0], 40), rng.integers(0, dbz.shape[1], 40))]
    echo = list(zip(*np.nonzero(e)))
    pts += [(int(r), int(c)) for r, c in (echo[i] for i in rng.integers(0, len(echo), 40))]
    out = {"box": list(BOX), "band": list(BAND), "shape": list(dbz.shape),
           "samples": {"hsr": [[r, c, round(float(dbz[r, c]), 3)] for r, c in pts],
                       "flag": [[r, c, int(fl[r, c])] for r, c in pts],
                       "tw": [[r, c, round(float(tw[r, c]), 3)] for r, c in pts]},
           "counts": {"rain": int(((codes >= 2) & (codes <= 142)).sum()), "snow": int(((codes >= 143) & (codes <= 213)).sum()),
                      "mix": int((codes >= 214).sum())},
           "codes_sha256": hashlib.sha256(codes.astype(np.uint8).tobytes()).hexdigest()}
    with open(os.path.join(FIX, "types_expect.json"), "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=0)
    print(out["counts"])


if __name__ == "__main__":
    main(*sys.argv[1:4])
