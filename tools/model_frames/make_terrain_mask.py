"""Build static_overlay/terrain_half.png: the model terrain on the overlays' half (0.5-degree) grid, for the isobars' H / L
marks (plan section 46, G32 re-check H-1: the sea-level pressure both models reduce from the surface leaves a fixed bump
on every mountain node, so the raw-code centres put an "H" on the Big Island at every step; the client marks no centre on
high ground).

    python tools/model_frames/make_terrain_mask.py [YYYYMMDDHH]

Source: NOAA GFS `HGT:surface` (the model's orography, geopotential metres, 0.25 degrees; public domain) of the f000 record
of one cycle (the newest complete one without an argument). A half-grid node takes the HIGHEST 0.25-degree node of its
footprint (the 3 x 3 nodes centred on it, rows clamped at the poles, columns wrapping), so a half node whose footprint
holds a ridge counts as high. Stored as an 8-bit greyscale PNG, 720 x 361, code = round(metres / TERRAIN_M_PER_CODE)
clamped to 0-255 (0-5,100 m; the Dead Sea and other depressions are 0), the same geometry as the frames' half grid (row 0 =
90 N, column 0 = 180 W, node centres). The client reads the threshold from its own constant (ISO_TERRAIN_M), so a different
threshold needs no rebuild; a rebuild changes the asset hash (bump OVERLAY_ASSET_VERSION, re-pin the fixture).
"""
import io
import os
import sys
from datetime import datetime, timezone

import numpy as np
from PIL import Image

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import decode as D   # noqa: E402
import fetch as F    # noqa: E402

KEY = "HGT:surface"
TERRAIN_M_PER_CODE = 20
OUT = os.path.join(os.path.dirname(os.path.dirname(HERE)), "static_overlay", "terrain_half.png")


def half_max(grid):
    """721 x 1440 metres -> 361 x 720: the highest 0.25-degree node of each half node's 3 x 3 footprint."""
    g = np.where(np.isfinite(grid), grid, -np.inf)
    rows = np.pad(g, ((1, 1), (0, 0)), mode="edge")                      # rows beyond the poles repeat the pole row
    cols = np.concatenate([rows[:, -1:], rows, rows[:, :1]], axis=1)      # columns wrap
    m = cols[0:-2, :]
    for dr in (1, 2):
        m = np.maximum(m, cols[dr:cols.shape[0] - 2 + dr, :])
    out = m[:, 0:-2]
    for dc in (1, 2):
        out = np.maximum(out, m[:, dc:m.shape[1] - 2 + dc])
    out = out[::2, ::2]
    assert out.shape == (361, 720), out.shape
    return out


def quantise(metres):
    """Metres (361 x 720) -> uint8 codes of TERRAIN_M_PER_CODE metres, 0-255."""
    q = np.round(np.where(np.isfinite(metres), metres, 0.0) / TERRAIN_M_PER_CODE)
    return np.clip(q, 0, 255).astype(np.uint8)


def build(run_dt):
    blob = F.fetch_records(F.atmos_url(run_dt, 0), [KEY])[KEY]
    grid, meta = D.decode(blob)
    if meta["shortName"] not in ("orog", "h") or meta["typeOfLevel"] != "surface":
        raise ValueError(f"{KEY}: got {meta['shortName']}/{meta['typeOfLevel']}")
    if str(meta["stepRange"]) != "0" or int(meta["dataDate"]) != int(run_dt.strftime("%Y%m%d")) or int(meta["dataTime"]) != run_dt.hour * 100:
        raise ValueError(f"{KEY}: step {meta['stepRange']} / cycle {meta['dataDate']} {meta['dataTime']}")
    return quantise(half_max(grid)), meta


def main(argv):
    run_dt = datetime.strptime(argv[0], "%Y%m%d%H").replace(tzinfo=timezone.utc) if argv else F.latest_complete_run()
    q, meta = build(run_dt)
    buf = io.BytesIO()
    Image.fromarray(q, "L").save(buf, format="PNG", optimize=True)
    open(OUT, "wb").write(buf.getvalue())
    node = lambda lat, lon: int(q[int(round((90 - lat) / 0.5)), int(round((lon + 180) / 0.5)) % 720])   # noqa: E731
    print(f"run {run_dt:%Y%m%d%H} {meta['shortName']} ({meta['units']}) -> {OUT} {len(buf.getvalue())} B, max {int(q.max()) * TERRAIN_M_PER_CODE} m")
    for m in (200, 300, 400, 500, 600, 800):
        print(f"  nodes at or above {m} m: {int((q >= m / TERRAIN_M_PER_CODE).sum())}")
    for name, lat, lon in (("Big Island", 19.5, -155.5), ("Maui", 20.8, -156.3), ("Oahu", 21.5, -158.0), ("Kauai", 22.0, -159.5),
                           ("Taiwan", 23.5, 121.0), ("Luzon", 16.5, 121.0), ("Tibet", 32.0, 88.0), ("Ethiopia", 9.0, 38.5),
                           ("Honolulu sea", 21.0, -157.5), ("Denver", 39.7, -105.0), ("Amsterdam", 52.5, 5.0)):
        print(f"  {name}: {node(lat, lon) * TERRAIN_M_PER_CODE} m")


if __name__ == "__main__":
    main(sys.argv[1:])
