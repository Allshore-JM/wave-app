"""Pack the per-bin SWAN outputs of the nearshore model folder into the one table file the publisher reads
(plan section 109, stage 2). Run on the machine that holds the model folder:

    python tools/model_frames/nearshore_table.py <model root> <dx> <rows> <out.npz> <version> <lon0> <lat0>

Per bin and cell: the height gain K = sqrt(K2) in one byte on a log scale (steps of 2.3 %), the local wave direction
in one byte (steps of 1.4 deg). Depths in centimetres. Only the first `rows` rows (the surf band) are kept."""
import hashlib
import json
import os
import sys

import numpy as np

import nearshore_engine as E

F = [0.04, 0.0455, 0.05, 0.0556, 0.0625, 0.0667, 0.0714, 0.0833, 0.1, 0.125, 0.15, 0.2]     # 25 .. 5 s
D = [240, 255, 270, 285, 300, 315, 330, 345, 0, 15, 30, 45, 60]                             # deg FROM


def pack(root, dx, rows, out, version, lon0, lat0, F=F, D=D):
    t = E.Table.from_runs(root, dx, F, D, rows=rows)
    info = json.load(open(os.path.join(root, "grid", f"grid_{dx:g}m.json")))
    nb = len(F) * len(D); ny, nx = t.dep.shape
    dirs = (np.degrees(np.arctan2(t.KS, t.KC)) % 360.0).reshape(nb, ny, nx)
    np.savez_compressed(out, F=np.array(F), D=np.array(D, float), dx=float(dx), alpc=float(info["alpc"]), xpc=float(info["xpc"]),
                        ypc=float(info["ypc"]), lon0=float(lon0), lat0=float(lat0), version=str(version),
                        dep_cm=np.clip(np.rint(np.nan_to_num(t.dep, nan=-5.0) * 100.0), -32000, 32000).astype(np.int16),
                        K=E.Table.encode_k(t.K2).reshape(nb, ny, nx), DIR=(np.rint(dirs * (256.0 / 360.0)).astype(np.int64) % 256).astype(np.uint8))
    sha = hashlib.sha256(open(out, "rb").read()).hexdigest()
    return dict(path=out, bytes=os.path.getsize(out), sha256=sha, bins=nb, shape=[ny, nx], version=str(version))


if __name__ == "__main__":
    a = sys.argv
    print(json.dumps(pack(a[1], float(a[2]), int(a[3]), a[4], a[5], float(a[6]), float(a[7]))))
