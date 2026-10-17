"""Capture the isobar tracer's real-data fixture (plan section 46, G32a A-1 (b)) from one GFS PRMSL record (network +
eccodes, which decodes PRMSL's complex packing locally):

    python tests/capture_pres_fixture.py 2026101712 24

tests/fixtures/pres_half.png       the half-resolution frame exactly as the job publishes it (encode.encode_frame, 920-1060 hPa)
tests/fixtures/pres_half_true.bin.gz  the same nodes' true values: uint16 little-endian, (hPa - 900) * 20 (0.05 hPa), row-major 361 x 720
"""
import gzip
import os
import sys
from datetime import datetime, timezone

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "tools", "model_frames"))

import decode as D   # noqa: E402
import encode as E   # noqa: E402
import fetch as F    # noqa: E402

KEY = "PRMSL:mean sea level"


def main(run, step):
    run_dt = datetime.strptime(run, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    blob = F.fetch_records(F.atmos_url(run_dt, step), [KEY])[KEY]
    p = D.hpa(D.decode(blob, KEY, run_dt, step)[0]).astype(np.float64)
    out = E.encode_frame(p, "pres")
    open(os.path.join(HERE, "fixtures", "pres_half.png"), "wb").write(out["half"])
    half = p[::2, ::2]
    assert half.shape == (361, 720) and np.isfinite(half).all()
    raw = np.clip(np.round((half - 900.0) * 20.0), 0, 65535).astype("<u2").tobytes()
    with open(os.path.join(HERE, "fixtures", "pres_half_true.bin.gz"), "wb") as fh:
        fh.write(gzip.compress(raw, compresslevel=9, mtime=0))
    print(run, step, "range %.2f-%.2f hPa" % (half.min(), half.max()), "png", len(out["half"]), "stats", out["stats"])


if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]))
