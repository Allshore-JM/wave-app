"""A strict reader of one WAVEWATCH III point-spectra file (`gfswave.<id>.spec`, plan section 77).

The file: a header line `'WAVEWATCH III SPECTRA' nf nd 1 'name'`, then nf frequencies (Hz) and nd directions
(radians, the direction the waves travel TO, on one evenly spaced grid in the file's own order), then per time step a
`YYYYMMDD HHMMSS` line, one quoted point line (`'id' lat lon depth u10 udir cur curdir`) and nf x nd densities in
m2/Hz/rad, frequency varying fastest (the first nf values are the first direction's).

`read(data)` returns Spectra(freqs, dirs, steps): dirs in degrees FROM (true north, the file's order, decoded to the
grid the file prints: 10 deg for NOAA's 36), steps = [(naive UTC datetime, E[nf, nd] in m2/Hz/DEGREE, u10 m/s,
udir FROM deg)] in file order. Anything off the format raises ValueError: a short or cut member, a wrong count, a
non-numeric value, directions not on one grid round the circle, a step without its point line. The point line's
u10 / udir are the 4th and 5th numbers after the name (the site's and the winds job's rule); a step without them
carries nan for both (the wind-sea rule then skips).
"""
import math
import re
from datetime import datetime

import numpy as np

MAGIC = b"'WAVEWATCH III SPECTRA'"
STEP_RE = re.compile(rb"^[ \t]*(\d{8})[ \t]+(\d{6})[ \t]*\r?\n", re.M)
HEAD_RE = re.compile(rb"^'WAVEWATCH III SPECTRA'[ \t]+(\d+)[ \t]+(\d+)[ \t]+(\d+)[ \t]*(?:'[^'\n]*')?[ \t]*\r?$", re.M)
FLOAT_RE = re.compile(rb"-?\d+\.\d+")
EXP_RE = re.compile(rb"[-+]?\d*\.?\d+E[-+]\d+")
GRID_TOL_DEG = 0.6          # the file prints 3 significant figures of radians: 1.48 rad = 84.8 deg for the 85-deg bin
MAX_MEMBER = 32 * 1024 * 1024
MAX_STEPS = 400


class Spectra:
    __slots__ = ("freqs", "dirs", "steps")

    def __init__(self, freqs, dirs, steps):
        self.freqs, self.dirs, self.steps = freqs, dirs, steps


def _grid(dirs_deg):
    """The directions decoded to the evenly spaced grid they print, in their own order (FROM, degrees)."""
    nd = dirs_deg.size
    step = 360.0 / nd
    half = step / 2.0                       # WW3's first direction is rth0 x step with rth0 = 0 or 0.5 (NOAA: 5, 15 ...)
    snapped = (np.round(dirs_deg / half) * half) % 360.0
    if np.max(np.abs(((dirs_deg - snapped) + 180.0) % 360.0 - 180.0)) > GRID_TOL_DEG:
        raise ValueError("directions are not on one evenly spaced grid")
    offset = snapped[0] % step
    rem = (snapped - offset) % step
    if np.max(np.minimum(rem, step - rem)) > 1e-9:
        raise ValueError("directions are not on one evenly spaced grid")
    # one full circle, every node once, a constant step in one sense
    kk = np.round((snapped - offset) / step).astype(int) % nd
    if sorted(kk.tolist()) != list(range(nd)):
        raise ValueError("directions do not cover the circle once")
    d = np.diff(np.concatenate([kk, kk[:1]])) % nd
    if not (np.all(d == 1) or np.all(d == nd - 1)):
        raise ValueError("directions do not go round the circle in order")
    snapped = np.round(snapped, 6)
    snapped[snapped >= 360.0] = 0.0
    return snapped


def read(data):
    if not isinstance(data, (bytes, bytearray)):
        raise ValueError("bytes expected")
    if len(data) > MAX_MEMBER:
        raise ValueError(f"member too large: {len(data)} B")
    if not data.startswith(MAGIC):
        raise ValueError("not a WAVEWATCH III spectra file")
    hm = HEAD_RE.match(data)
    if not hm:
        raise ValueError("bad header line")
    nf, nd = int(hm.group(1)), int(hm.group(2))
    if not (1 <= nf <= 200 and 2 <= nd <= 360):
        raise ValueError(f"bad counts {nf} x {nd}")
    marks = list(STEP_RE.finditer(data, hm.end()))
    if not marks:
        raise ValueError("no time steps")
    if len(marks) > MAX_STEPS:
        raise ValueError(f"{len(marks)} steps")
    head = data[hm.end():marks[0].start()]
    vals = EXP_RE.findall(head)
    if len(vals) != nf + nd:
        raise ValueError(f"header holds {len(vals)} numbers, {nf + nd} expected")
    try:
        hv = np.array([float(v) for v in vals], float)
    except ValueError:
        raise ValueError("bad header number") from None
    freqs = hv[:nf]
    if not (np.all(np.isfinite(freqs)) and np.all(freqs > 0) and np.all(np.diff(freqs) > 0)):
        raise ValueError("frequencies must be positive and ascending")
    dirs_to = np.degrees(hv[nf:nf + nd])
    if not np.all(np.isfinite(dirs_to)):
        raise ValueError("bad direction")
    dirs = _grid((dirs_to + 180.0) % 360.0)
    steps = []
    n = nf * nd
    scale = math.pi / 180.0
    for i, m in enumerate(marks):
        body = data[m.end():marks[i + 1].start() if i + 1 < len(marks) else len(data)]
        try:
            when = datetime.strptime((m.group(1) + m.group(2)).decode(), "%Y%m%d%H%M%S")
        except ValueError:
            raise ValueError(f"bad time line {m.group(0)!r}") from None
        lines = body.lstrip(b" \t\r\n").split(b"\n", 1)
        point = lines[0].rstrip(b"\r")
        rest = lines[1] if len(lines) > 1 else b""
        if not point.startswith(b"'") or point.count(b"'") < 2:
            raise ValueError(f"step {when:%Y%m%d %H}: no point line")
        after = point.split(b"'", 2)[2]
        nums = FLOAT_RE.findall(after)
        u10 = udir = math.nan
        if len(nums) >= 5:
            try:
                u10, udir = float(nums[3]), float(nums[4])
            except ValueError:
                u10 = udir = math.nan
        toks = rest.split()
        if len(toks) != n:
            raise ValueError(f"step {when:%Y%m%d %H}: {len(toks)} values, {n} expected")
        try:
            v = np.array(toks, dtype=float)
        except ValueError:
            raise ValueError(f"step {when:%Y%m%d %H}: a non-numeric value") from None
        if not np.all(np.isfinite(v)):
            raise ValueError(f"step {when:%Y%m%d %H}: a non-finite value")
        E = v.reshape(nd, nf).T * scale          # frequency fastest in the file -> E[nf, nd] per degree
        steps.append((when, E, u10, udir))
    return Spectra(freqs, dirs, steps)


def hs_of(freqs, dirs, E):
    """4 sqrt(m0) of one spectrum (m2/Hz/deg), the tests' independent total."""
    from partition2d import bin_widths
    df = bin_widths(np.asarray(freqs, float))
    m0 = float((np.asarray(E, float) * df[:, None] * (360.0 / len(dirs))).sum())
    return 4.0 * math.sqrt(m0) if m0 > 0 else 0.0
