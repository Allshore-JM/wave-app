"""The MRMS and OPERA output must not move when a source is added (plan section 86): every object's decoded codes, the order objects
are written and files asked for, and the pointer, over ticks that cover new frames, a late frame, a damaged file, a source that is down,
the backfill budget, a gap and retirement. Section 89's precipitation type leaves it as it was where NOAA has no PrecipFlag file (these
ticks): its requests and the pointer's "type_codes" are left out. Captured from production 312da88 (before the source table existed):
    python -I tests/model_frames/test_radar_golden.py --capture
Decoded codes, not PNG bytes: a PNG depends on the zlib build of the machine. Nothing reaches the network."""
import gzip
import hashlib
import io
import json
import os
import struct
import sys
import urllib.error
from datetime import datetime, timedelta, timezone

import numpy as np
from PIL import Image

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
import mrms    # noqa: E402
import radar as R  # noqa: E402

GOLDEN = os.path.join(ROOT, "tests", "fixtures", "radar", "golden_v1.json")
NOW = datetime(2026, 11, 6, 12, 6, 0, tzinfo=timezone.utc)
REGIONS = {"r1": ("One", "mrms", "D1", 20, -160, 22, -156, 1),
           "h1": ("Two", "mrms", "D2", 20, -150, 22, -146, 2),
           "e1": ("Eur", "opera", None, 50, 0, 52, 4, 1)}
# what the pointer gains on purpose in sections 86 and 89 stays out of the comparison
NOT_COMPARED = ("regions", "credit", "began", "type_codes")
# section 89 asks for each MRMS minute's PrecipFlag file (none here: the frames stay untyped) and wet-bulb hours: not in the golden
TYPE_FILES = (mrms.FLAG_PRODUCT, mrms.TW_PRODUCT)


def _png16(x):
    buf = io.BytesIO()
    Image.fromarray(x.astype(np.uint16)).save(buf, "PNG")
    return buf.getvalue()


def _grib(values, lat1, lon1, step):
    """A one-field GRIB2 message as MRMS writes it, from dBZ (one decimal)."""
    nj, ni = values.shape
    x = np.round((values * 10) + 9990).astype(np.int64)
    sec1 = struct.pack(">IB", 21, 1) + bytes(16)
    s3 = bytearray(72)
    s3[:5] = struct.pack(">IB", 72, 3)
    s3[30:38] = struct.pack(">II", ni, nj)
    s3[46:54] = struct.pack(">iI", round(lat1 * 1e6), round(((lon1 + 360) % 360) * 1e6))
    s3[63:71] = struct.pack(">II", round(step * 1e6), round(step * 1e6))
    s4 = bytearray(34)
    s4[:5] = struct.pack(">IB", 34, 4)
    s4[9], s4[10] = 8, 8
    s5 = bytearray(21)
    s5[:5] = struct.pack(">IB", 21, 5)
    s5[5:9] = struct.pack(">I", nj * ni)
    s5[9:11] = struct.pack(">H", 41)
    s5[11:15] = struct.pack(">f", -9990.0)
    s5[15:19] = struct.pack(">HH", 0, 1)
    s5[19] = 16
    s6 = struct.pack(">IBB", 6, 6, 255)
    pic = _png16(x)
    s7 = struct.pack(">IB", 5 + len(pic), 7) + pic
    body = sec1 + bytes(s3) + bytes(s4) + bytes(s5) + s6 + s7 + b"7777"
    return b"GRIB\x00\x00" + bytes([209, 2]) + struct.pack(">Q", 16 + len(body)) + body


def _d1(when, value=30.0):
    v = np.full((200, 400), -99.0)
    v[10:60, 10:60] = value
    v[150:, :] = -999.0
    return {mrms.url_of("D1", when): gzip.compress(_grib(v, 21.995, -159.995, 0.01))}


def _d2(when, value=40.0):
    v = np.full((400, 800), -99.0)
    v[30:90, 100:300] = value
    v[390:, :] = -999.0
    return {mrms.url_of("D2", when): gzip.compress(_grib(v, 21.9975, -149.9975, 0.005))}


class _Answer:
    def __init__(self, body):
        self.stream = io.BytesIO(body)
        self.headers = {"Content-Length": str(len(body))}

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self, n=-1):
        return self.stream.read(n)


class _Web:
    def __init__(self, files=None, down=(), bad=()):
        self.files, self.down, self.bad, self.asked = dict(files or {}), tuple(down), tuple(bad), []

    def __call__(self, url, headers=None, timeout=60):
        self.asked.append(url)
        if any(d in url for d in self.down):
            raise TimeoutError("timed out")
        if any(d in url for d in self.bad):
            return _Answer(gzip.compress(b"not grib"))
        if url in self.files:
            return _Answer(self.files[url])
        raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)


class _Store:
    bucket = "fake"

    def __init__(self):
        self.objects, self.order, self.meta = {}, [], {}

    def put(self, key, body, content_type, cache_control):
        self.objects[key] = body
        if key.endswith(".png"):                                       # uploads run on a pool: their order within a region is not fixed
            self.order.append(key)
        self.meta[key] = (content_type, cache_control)

    def get_json(self, key):
        return json.loads(self.objects[key].decode()) if key in self.objects else None

    def delete_prefix(self, prefix):
        for k in [k for k in self.objects if k.startswith(prefix)]:
            del self.objects[k]


def _digest(store):
    out = {}
    for key, body in sorted(store.objects.items()):
        if key.endswith(".png"):
            im = Image.open(io.BytesIO(body))
            out[key] = [im.mode, list(im.size), hashlib.sha256(im.tobytes()).hexdigest()[:16], list(store.meta[key])]
    return out


def _pointer(store):
    p = store.get_json(f"{R.PREFIX}/latest.json")
    return {k: v for k, v in p.items() if k not in NOT_COMPARED}


def _groups(keys):
    """[slot/region, objects] in the order they were written: uploads run on a pool, so the order inside a region is not fixed."""
    out = []
    for key in keys:
        group = "/".join(key.split("/")[2:4])
        if out and out[-1][0] == group:
            out[-1][1] += 1
        else:
            out.append([group, 1])
    return out


def scenario():
    """The ticks, on the job's globals as the tests set them. -> everything the job did, as plain data."""
    saved = R.REGIONS, R.opera_frame, R.time.sleep
    R.REGIONS, R.time.sleep = REGIONS, (lambda s: None)
    labels = set()

    def opera_frame(dt, get):
        name = R.slot_name(dt)
        if name not in labels:
            raise R.Missing("HTTP 404")
        a = np.ones((200, 400), np.uint8)
        a[20:80, 20:80] = 60 + int(name[-2:]) // 10
        return a, 0, 160

    R.opera_frame = opera_frame
    try:
        slot = R.slot_of(NOW)
        ago = lambda m: slot - timedelta(minutes=m)                    # noqa: E731
        store = _Store()
        record = []

        def tick(name, web, now, args=(), clock=None):
            kw = {"clock": clock} if clock else {}
            rc = R.main(list(args), get=web, store=store, now=now, **kw)
            record.append({"name": name, "rc": rc, "asked": [u for u in web.asked if not any(t in u for t in TYPE_FILES)],
                           "written": _groups(store.order),
                           "objects": _digest(store), "pointer": _pointer(store)})
            store.order.clear()

        # new frames, the +2 offset, a damaged file, a European frame late
        labels |= {R.slot_name(slot)}
        tick("first", _Web({**_d1(slot + timedelta(minutes=4)), **_d1(ago(10) + timedelta(minutes=2)), **_d2(slot + timedelta(minutes=4))},
                           bad=((ago(20) + timedelta(minutes=4)).strftime("%Y%m%d-%H%M"),)), NOW)
        # the next tick: one source down, the late frame in
        labels |= {R.slot_name(ago(10))}
        later = NOW + timedelta(minutes=10)
        tick("down", _Web({**_d1(R.slot_of(later) + timedelta(minutes=4))}, down=("/D2/",)), later)
        # the backfill budget: 100 s into the tick, older slots wait for MRMS but not for OPERA
        t = iter([0.0])
        labels |= {R.slot_name(ago(120))}
        tick("budget", _Web({**_d1(ago(110) + timedelta(minutes=4))}), NOW + timedelta(minutes=20), clock=lambda: next(t, 100.0))
        # past the deadline nothing recent is started
        c = {"v": 0.0}

        def fast():
            c["v"] += R.DEADLINE_S + 1
            return c["v"]

        tick("deadline", _Web(), NOW + timedelta(minutes=30), clock=fast)
        # a source at a time, a limit on the slots
        tick("sources", _Web({**_d1(R.slot_of(NOW + timedelta(minutes=40)) + timedelta(minutes=4))}), NOW + timedelta(minutes=40),
             args=("--sources", "mrms", "--slots", "2"))
        # a day on: everything retires, the grace runs out
        tick("retired", _Web(), NOW + timedelta(hours=25))
        tick("pruned", _Web(), NOW + timedelta(hours=26, minutes=10))
        return record
    finally:
        R.REGIONS, R.opera_frame, R.time.sleep = saved


def test_mrms_and_opera_output_is_unchanged():
    with open(GOLDEN, encoding="utf-8") as fh:
        want = json.load(fh)
    got = json.loads(json.dumps(scenario()))
    assert len(got) == len(want)
    for g, w in zip(got, want):
        for field in ("rc", "asked", "written", "objects", "pointer"):
            assert g[field] == w[field], f"tick {w['name']}: {field} moved"


if __name__ == "__main__" and "--capture" in sys.argv:
    data = scenario()
    with open(GOLDEN, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=0, sort_keys=True)
    print(os.path.getsize(GOLDEN), "bytes;", [(d["name"], d["rc"], len(d["asked"]), len(d["objects"])) for d in data])
