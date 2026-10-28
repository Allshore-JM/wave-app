"""Reader for SWAN standard 2-D spectral files (plan section 59): NOAA NWPS's point spectra `nwps.tHHz.spc2d_<id>_CG1.<office>.txt`.

The file: a `SWAN   1` magic line, `$` comments, `TIME` / `LONLAT` (one location) / `AFREQ` (absolute frequencies, Hz)
/ `NDIR` (spectral nautical directions = waves FROM, degrees clockwise from north) / `QUANT` (one quantity, VaDens in
m2/Hz/degr with an exception value), then per time a `YYYYMMDD.HHMMSS` line followed by `FACTOR` + the factor + nf rows
of nd integers (variance density = integer x factor), or `NODATA` (no spectrum: a land cell or an unset time), or `ZERO`
(a spectrum of zeros). Strict: anything else is a ValueError. Pure Python, no numpy: the job and the tests share it.
"""
import re
from datetime import datetime

MAGIC = "SWAN   1"
MAX_BYTES = 2 * 1024 * 1024
MAX_LINE = 4096
MAX_FREQS = 200
MAX_DIRS = 180
MAX_STEPS = 400                 # NWPS writes 145 hours; a longer file is not one of NOAA's
MAX_FACTOR = 1e3                # NOAA's factors are ~1e-6..1e-3: anything near this is not a spectrum
INT_RE = re.compile(r"^-?\d{1,9}$")
TIME_RE = re.compile(r"^(\d{8})\.(\d{6})\s")


class Spectra:
    """freqs: Hz ascending; dirs: degrees FROM, normalised to [0, 360); lon / lat: the point; steps: [(datetime, rows)]
    where rows is nf lists of nd floats (m2/Hz/degr; the exception value -> nan) or None for NODATA."""

    def __init__(self, freqs, dirs, lon, lat, steps, exception):
        self.freqs, self.dirs, self.lon, self.lat, self.steps, self.exception = freqs, dirs, lon, lat, steps, exception


def _tok(line):
    return line.split()[0] if line.split() else ""


def read(text):
    if isinstance(text, bytes):
        if len(text) > MAX_BYTES:
            raise ValueError(f"spectral file over {MAX_BYTES} bytes")
        text = text.decode("ascii", "strict")
    elif len(text) > MAX_BYTES:
        raise ValueError(f"spectral file over {MAX_BYTES} bytes")
    lines = text.splitlines()
    if not lines or not lines[0].startswith(MAGIC):
        raise ValueError("not a SWAN spectral file")
    if any(len(l) > MAX_LINE for l in lines):
        raise ValueError("a line over the limit")
    i = 1
    while i < len(lines) and lines[i].startswith("$"):
        i += 1

    def expect(word):
        nonlocal i
        if i >= len(lines) or not lines[i].startswith(word):
            raise ValueError(f"expected {word} at line {i + 1}")
        i += 1

    def number(kind=float):
        nonlocal i
        if i >= len(lines):
            raise ValueError("file ends early")
        try:
            v = kind(_tok(lines[i]))
        except ValueError:
            raise ValueError(f"expected a number at line {i + 1}") from None
        i += 1
        return v

    expect("TIME")
    if number(int) != 1:
        raise ValueError("unsupported time coding")
    expect("LONLAT")
    if number(int) != 1:
        raise ValueError("one location expected")
    if i >= len(lines) or len(lines[i].split()) < 2:
        raise ValueError("expected the location")
    lon, lat = float(lines[i].split()[0]), float(lines[i].split()[1])
    if not (-180 <= lon <= 360 and -90 <= lat <= 90):
        raise ValueError("location out of range")
    if lon > 180:
        lon -= 360
    i += 1
    expect("AFREQ")
    nf = number(int)
    if not 2 <= nf <= MAX_FREQS:
        raise ValueError("bad frequency count")
    freqs = [number() for _ in range(nf)]
    if any(f <= 0 for f in freqs) or any(b <= a for a, b in zip(freqs, freqs[1:])):
        raise ValueError("frequencies must be positive and ascending")
    expect("NDIR")
    nd = number(int)
    if not 4 <= nd <= MAX_DIRS:
        raise ValueError("bad direction count")
    dirs = [number() % 360.0 for _ in range(nd)]
    expect("QUANT")
    if number(int) != 1:
        raise ValueError("one quantity expected")
    if i >= len(lines) or not lines[i].startswith("VaDens"):
        raise ValueError("VaDens expected")
    i += 1
    if i >= len(lines) or not lines[i].startswith("m2/Hz/degr"):
        raise ValueError("m2/Hz/degr expected")
    i += 1
    exception = number()

    steps = []
    while i < len(lines):
        line = lines[i]
        if not line.strip():
            i += 1
            continue
        m = TIME_RE.match(line)
        if not m:
            raise ValueError(f"expected a time at line {i + 1}")
        try:
            when = datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S")
        except ValueError:
            raise ValueError(f"bad time at line {i + 1}") from None
        i += 1
        if i >= len(lines):
            raise ValueError("file ends after a time")
        key = _tok(lines[i])
        if key == "NODATA":
            steps.append((when, None))
            i += 1
            if len(steps) > MAX_STEPS:
                raise ValueError(f"more than {MAX_STEPS} time steps")
        elif key == "ZERO":
            steps.append((when, [[0.0] * nd for _ in range(nf)]))
            if len(steps) > MAX_STEPS:
                raise ValueError(f"more than {MAX_STEPS} time steps")
            i += 1
        elif key == "FACTOR":
            i += 1
            factor = number()
            if not 0 <= factor <= MAX_FACTOR:          # also refuses nan and inf
                raise ValueError("bad factor")
            rows = []
            for _ in range(nf):
                if i >= len(lines):
                    raise ValueError("file ends inside a spectrum")
                toks = lines[i].split()
                if len(toks) != nd:
                    raise ValueError(f"expected {nd} values at line {i + 1}")
                if not all(INT_RE.match(t) for t in toks):
                    raise ValueError(f"bad value at line {i + 1}")     # integers only (no nan, inf, 2.5, 1_000)
                vals = [float(t) for t in toks]
                if any(v < 0 and v != exception for v in vals):
                    raise ValueError(f"negative value at line {i + 1}")
                rows.append([float("nan") if v == exception else v * factor for v in vals])
                i += 1
            steps.append((when, rows))
            if len(steps) > MAX_STEPS:
                raise ValueError(f"more than {MAX_STEPS} time steps")
        else:
            raise ValueError(f"expected FACTOR, NODATA or ZERO at line {i + 1}")
    if not steps:
        raise ValueError("no time steps")
    if any(b <= a for (a, _), (b, _) in zip(steps, steps[1:])):
        raise ValueError("times must ascend")
    return Spectra(freqs, dirs, lon, lat, steps, exception)
