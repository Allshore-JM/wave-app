"""Build tools/model_frames/fill_allow.png: where the coastal fill may write (G6 review, P1).

A 0.25-degree node (the frames' 1440x721 centre-registered grid, row 0 = 90 N, column 0 = 180 W)
is allowed when its own box, or one of its 8 neighbours' boxes, contains GSHHG land (levels 1 + 5,
the same tier-1 polygons the browser clips to). Open water far from land -- in practice the sea-ice
pack the model masks -- is never filled, so no waves are invented over ice.

    python tools/model_frames/make_fill_mask.py --coast build/coast/v1

The coast directory is a coast-v1 build (tools/coast/build_coast.py); its tier-1 content hash is
written into the PNG (text chunk "coast") and pinned by the tests together with the PNG's sha256.
Land is rasterised at 1/40 degree (about 20 s, 110 MB).
"""
import argparse
import glob
import hashlib
import json
import os
import sys

import numpy as np
from PIL import Image, ImageDraw, PngImagePlugin

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "coast"))
import build_coast as BC  # noqa: E402

R = 40                                    # raster cells per degree (10 per 0.25-degree node)
OUT = os.path.join(HERE, "fill_allow.png")


def rasterise_land(coast_dir):
    img = Image.new("1", (360 * R, 180 * R), 0)
    dr = ImageDraw.Draw(img)
    for fn in sorted(glob.glob(os.path.join(coast_dir, "f", "*.bin"))):
        d = BC.decode_file(open(fn, "rb").read())
        for pc in d["pieces"]:
            for ix, iy in pc["rings"]:
                x = (ix / BC.Q + 180.0) * R
                y = (90.0 - iy / BC.Q) * R
                dr.polygon(list(zip(x.tolist(), y.tolist())), fill=1)
    return np.array(img, dtype=bool)


def node_boxes(land):
    """Any GSHHG land in each node's 0.25-degree box (centre-registered): shape (721, 1440)."""
    pad = np.concatenate([np.repeat(land[:1], 5, 0), land, np.repeat(land[-1:], 5, 0)], 0)   # 7210 rows
    pad = np.roll(pad, 5, axis=1)                          # the box of column j = raster columns 10j-5 .. 10j+4
    return pad.reshape(721, 10, 1440, 10).any(axis=(1, 3))


def dilate(m):
    """One Chebyshev step, longitude periodic, nothing beyond the poles."""
    out = m.copy()
    for dy in (-1, 0, 1):
        for dx in (-1, 0, 1):
            s = np.roll(m, dx, axis=1)
            if dy == 1:
                s = np.vstack([np.zeros((1, m.shape[1]), bool), s[:-1]])
            elif dy == -1:
                s = np.vstack([s[1:], np.zeros((1, m.shape[1]), bool)])
            out |= s
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--coast", required=True, help="a coast-v1 build directory (index.json + f/*.bin)")
    ap.add_argument("--out", default=OUT)
    a = ap.parse_args(argv)
    index = json.load(open(os.path.join(a.coast, "index.json")))
    allow = dilate(node_boxes(rasterise_land(a.coast)))
    info = PngImagePlugin.PngInfo()
    info.add_text("coast", "%s tier1.sha256=%s" % (index.get("format_key"), index["tier1"].get("sha256")))
    Image.fromarray(allow.astype(np.uint8) * 255, "L").convert("1").save(a.out, "PNG", optimize=True, pnginfo=info)
    print("allowed nodes %d of %d; %s %d bytes sha256 %s" % (
        allow.sum(), allow.size, a.out, os.path.getsize(a.out), hashlib.sha256(open(a.out, "rb").read()).hexdigest()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
