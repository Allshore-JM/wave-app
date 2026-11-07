"""Taiwan CWA radar composite for the radar job (plan section 86): the O-A0059-001 history files on CWA's public bucket.

Credited under Taiwan's Open Government Data License v1.0; no key (CWA's own metadata points at this bucket). Every 10 minutes, about 7
minutes late, kept 15 days, an absent key answers 404:
  https://cwaopendata.s3.ap-northeast-1.amazonaws.com/history/Observation/<YYYYMMDDHHMM in +08:00>compref_mosaic.xml
8.9 MB of XML: 921 x 881 points of 0.0125 degrees, west to east then SOUTH TO NORTH from 115.0 E 18.0 N, dBZ; -99 "invalid data" (read
here as covered, no echo: an inference), -999 "outside the radars' range or removed by quality control". The file states the TWD67
datum; no correction is applied: on the 2026-11 checks the shift against JMA's frames could not be told from nothing.
"""
import re
from datetime import datetime, timedelta, timezone

import numpy as np

BUCKET_URL = "https://cwaopendata.s3.ap-northeast-1.amazonaws.com/history/Observation"
COLS, ROWS = 921, 881
LON0, LAT0, STEP = 115.0, 18.0, 0.0125
MAX_BYTES = 16_000_000
TAIPEI = timezone(timedelta(hours=8))
_TAG = re.compile(rb"<([A-Za-z]+)>([^<]{0,64})</\1>")


def url_of(when):
    return f"{BUCKET_URL}/{when.astimezone(TAIPEI):%Y%m%d%H%M}compref_mosaic.xml"


def read(body):
    """A history file -> {values: float32 (881, 921), row 0 the SOUTH edge, in dBZ; time: 'YYYYMMDDHHMM' in UTC}. Any other layout is a
    ValueError."""
    i, j = body.find(b"<content>"), body.find(b"</content>")
    if i < 0 or j < i:
        raise ValueError("no content")
    head = dict(_TAG.findall(body[:i]))
    want = {b"StartPointLongitude": LON0, b"StartPointLatitude": LAT0, b"GridResolution": STEP, b"GridDimensionX": COLS, b"GridDimensionY": ROWS}
    for name, value in want.items():
        if name not in head or abs(float(head[name]) - value) > 1e-9:
            raise ValueError(f"{name.decode()} is {head.get(name)!r}, not {value}")
    if head.get(b"Reflectivity") != b"dBZ":
        raise ValueError(f"unit {head.get(b'Reflectivity')!r}")
    if b"DateTime" not in head:
        raise ValueError("no DateTime")
    when = datetime.fromisoformat(head[b"DateTime"].decode("ascii"))
    if when.tzinfo is None:
        raise ValueError("a DateTime without a zone")
    vals = np.array(body[i + 9:j].split(b","), dtype=np.float64)
    if vals.size != COLS * ROWS:
        raise ValueError(f"{vals.size} values, not {COLS * ROWS}")
    return {"values": vals.reshape(ROWS, COLS).astype(np.float32), "time": when.astimezone(timezone.utc).strftime("%Y%m%d%H%M")}


def lattice_index(s, w, n, e, ppd):
    """(source rows, source columns) nearest the region's lattice pixel centres; -1 outside the grid."""
    lats = n - (np.arange((n - s) * ppd) + 0.5) / ppd
    lons = w + (np.arange((e - w) * ppd) + 0.5) / ppd
    rows = np.floor((lats - LAT0) / STEP + 0.5).astype(np.int64)
    cols = np.floor((lons - LON0) / STEP + 0.5).astype(np.int64)
    rows[(rows < 0) | (rows >= ROWS)] = -1
    cols[(cols < 0) | (cols >= COLS)] = -1
    return rows, cols
