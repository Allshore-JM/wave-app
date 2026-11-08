"""The radar job (plan section 85): readers, tiers, geometry, pointer, failure rules. Nothing reaches the network."""
import gzip
import inspect
import io
import json
import os
import re
import struct
import sys
import urllib.error
import zlib
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest
from PIL import Image

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
import mrms    # noqa: E402
import opera   # noqa: E402
import radar as R  # noqa: E402

FIX = os.path.join(ROOT, "tests", "fixtures", "radar")
EXPECT = json.load(open(os.path.join(FIX, "expect.json"), encoding="utf-8"))
NOW = datetime(2026, 11, 6, 12, 6, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------- helpers
def png16(x):
    im = Image.fromarray(x.astype(np.uint16))
    buf = io.BytesIO()
    im.save(buf, "PNG")
    return buf.getvalue()


def grib(values, lat1, lon1, step, ident=(209, 8, 8), template=0, packing=41, scan=0, bitmap=255, bits=16, scales=(0, 1)):
    """A one-field GRIB2 message as MRMS writes it, from dBZ (one decimal)."""
    nj, ni = values.shape
    x = np.round((values * 10) + 9990).astype(np.int64)
    sec1 = struct.pack(">IB", 21, 1) + bytes(16)
    s3 = bytearray(72)
    s3[:5] = struct.pack(">IB", 72, 3)
    s3[12:14] = struct.pack(">H", template)
    s3[30:38] = struct.pack(">II", ni, nj)
    s3[46:54] = struct.pack(">iI", round(lat1 * 1e6), round(((lon1 + 360) % 360) * 1e6))
    s3[63:71] = struct.pack(">II", round(step * 1e6), round(step * 1e6))
    s3[71] = scan
    s4 = bytearray(34)
    s4[:5] = struct.pack(">IB", 34, 4)
    s4[9], s4[10] = ident[1], ident[2]
    s5 = bytearray(21)
    s5[:5] = struct.pack(">IB", 21, 5)
    s5[5:9] = struct.pack(">I", nj * ni)
    s5[9:11] = struct.pack(">H", packing)
    s5[11:15] = struct.pack(">f", -9990.0)
    s5[15:19] = struct.pack(">HH", *scales)
    s5[19] = bits
    s6 = struct.pack(">IBB", 6, 6, bitmap)
    pic = png16(x)
    s7 = struct.pack(">IB", 5 + len(pic), 7) + pic
    body = sec1 + bytes(s3) + bytes(s4) + bytes(s5) + s6 + s7 + b"7777"
    total = 16 + len(body)
    return b"GRIB\x00\x00" + bytes([ident[0], 2]) + struct.pack(">Q", total) + body


class FakeStore:
    bucket = "fake"

    def __init__(self):
        self.objects, self.order, self.meta = {}, [], {}

    def put(self, key, body, content_type, cache_control):
        self.objects[key] = body
        self.order.append(key)
        self.meta[key] = (content_type, cache_control)

    def get_json(self, key):
        return json.loads(self.objects[key].decode()) if key in self.objects else None

    def delete_prefix(self, prefix):
        for k in [k for k in self.objects if k.startswith(prefix)]:
            del self.objects[k]


class Answer:
    def __init__(self, body, length=None):
        self.stream = io.BytesIO(body)
        self.headers = {} if length is None else {"Content-Length": str(length)}

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self, n=-1):
        return self.stream.read(n)


class Web:
    """urls -> bytes; anything else 404; url parts that time out; url parts answering a file that is not GRIB."""

    def __init__(self, files=None, down=(), bad=()):
        self.files, self.down, self.bad, self.asked = dict(files or {}), tuple(down), tuple(bad), []

    def __call__(self, url, headers=None, timeout=60):
        self.asked.append(url)
        if any(d in url for d in self.down):
            raise TimeoutError("timed out")
        if any(d in url for d in self.bad):
            return Answer(gzip.compress(b"not grib"))
        if url in self.files:
            return Answer(self.files[url])
        raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)


@pytest.fixture
def small(monkeypatch):
    """Two small regions, so the job's whole path runs in milliseconds."""
    monkeypatch.setattr(R, "REGIONS", {"r1": ("One", "mrms", "D1", 20, -160, 22, -156, 1),
                                       "e1": ("Eur", "opera", None, 50, 0, 52, 4, 1)})
    monkeypatch.setattr(R.time, "sleep", lambda s: None)


def r1_file(when, value=30.0):
    v = np.full((200, 400), -99.0)
    v[10:60, 10:60] = value
    v[150:, :] = -999.0
    return {mrms.url_of("D1", when): gzip.compress(grib(v, 21.995, -159.995, 0.01))}


def e1_frame(monkeypatch, labels):
    """opera_frame stubbed: labels it answers; the rest 404."""
    def frame(dt, get):
        if R.slot_name(dt) not in labels:
            raise R.Missing("HTTP 404")
        a = np.ones((200, 400), np.uint8)
        a[20:80, 20:80] = 60
        return a, 0, 160
    monkeypatch.setattr(R, "opera_frame", frame)


def tick(store, web, now=NOW, args=("--sources", "mrms,opera"), clock=None):
    kw = {"clock": clock} if clock else {}
    return R.main(list(args), get=web, store=store, now=now, **kw)


# ---------------------------------------------------------------- the GRIB reader
def fixture_message():
    return mrms.read(mrms.gunzip(open(os.path.join(FIX, "HAWAII.grib2.gz"), "rb").read()))


def test_the_real_hawaii_file_equals_eccodes():
    m = fixture_message()
    assert (m["ni"], m["nj"], m["di"], m["dj"]) == (2600, 2200, 0.005, 0.005)
    assert m["lat1"] == 25.998 and round(m["lon1"], 3) == -163.998
    t = mrms.value_table(m)
    assert abs(t[0] + 999.0) < 0.01 and abs(t[9000] + 99.0) < 0.01
    for r, c, v in EXPECT["hawaii_samples"]:
        assert abs(t[m["x"][r, c]] - v) < 0.051, (r, c)


def test_the_real_hawaii_file_makes_the_expected_lattice():
    m = fixture_message()
    codes = R.mrms_lattice("hawaii", m)
    assert codes.shape == (1100, 1300)
    t = mrms.value_table(m)[m["x"]]
    for r, c in ((452, 628), (445, 656)):                              # echo samples of the file, each in its 2x2 block
        block = t[2 * r:2 * r + 2, 2 * c:2 * c + 2]
        assert codes[r, c] == R.code_table(block).max()


@pytest.mark.parametrize("change", [
    {"ident": (209, 8, 9)}, {"ident": (0, 8, 8)}, {"template": 30}, {"packing": 40}, {"scan": 64}, {"bitmap": 0}, {"bits": 8},
])
def test_a_message_of_another_shape_is_refused(change):
    raw = grib(np.full((4, 6), -99.0), 25.0, -160.0, 0.01, **change)
    with pytest.raises(ValueError):
        mrms.read(raw)


@pytest.mark.parametrize("damage", ["short", "marker", "magic", "png"])
def test_a_damaged_message_is_refused(damage):
    raw = grib(np.full((4, 6), -99.0), 25.0, -160.0, 0.01)
    if damage == "short":
        raw = raw[:-40]
    elif damage == "marker":
        raw = raw[:-4] + b"XXXX"
    elif damage == "magic":
        raw = b"BRIG" + raw[4:]
    else:
        i = raw.index(b"\x89PNG")
        raw = raw[:i] + b"junkjunk" + raw[i + 8:]
    with pytest.raises(ValueError):
        mrms.read(raw)


def test_gunzip_refuses_a_cut_file():
    body = gzip.compress(b"x" * 10000)
    with pytest.raises(ValueError):
        mrms.gunzip(body[:20])


# ---------------------------------------------------------------- codes
@pytest.mark.parametrize("dbz,code", [
    (-999.0, 0), (float("nan"), 0), (-99.0, 1), (float("-inf"), 1), (-27.5, 1), (4.99, 1), (5.0, 2), (5.5, 3), (10.0, 12),
    (75.0, R.MAX_CODE), (80.0, R.MAX_CODE),
])
def test_codes_at_their_boundaries(dbz, code):
    assert R.code_table(np.array([dbz]))[0] == code


def test_the_code_constants():
    assert R.MAX_CODE == 142 and R.MIN_CODE == 12
    assert R.COARSE_K == {25: 2, 10: 3} and R.MAX_TRIES == 3 and R.PPD_T2 == 100 and R.PPD_T1 == 25 and R.PPD_T0 == 10


def test_a_code_reads_back_within_half_a_step():
    for dbz in np.arange(5.0, 75.0, 0.1):
        code = R.code_table(np.array([dbz]))[0]
        assert abs(R.FLOOR_DBZ + (code - 2) * R.STEP_DBZ - dbz) <= R.STEP_DBZ / 2 + 1e-5


# ---------------------------------------------------------------- pooling
def test_one_stray_pixel_does_not_paint_a_coarse_pixel():
    a = np.ones((10, 10), np.uint8)
    a[3, 3] = 100
    assert R.pool_kth(a, 10, 3)[0, 0] == 1
    a[4, 4], a[5, 5] = 90, 80
    assert R.pool_kth(a, 10, 3)[0, 0] == 80


def test_a_coarse_pixel_is_covered_when_any_fine_pixel_is():
    a = np.zeros((4, 8), np.uint8)
    a[1, 1] = 1
    assert R.pool_kth(a, 4, 2).tolist() == [[1, 0]]


def test_pool_refuses_a_shape_that_does_not_divide():
    with pytest.raises(ValueError):
        R.pool_kth(np.zeros((5, 8), np.uint8), 4, 2)
    with pytest.raises(ValueError):
        R.pool_max(np.zeros((5, 8), np.uint8), 2)


def test_pool_max_takes_the_strongest():
    a = np.array([[1, 5, 0, 0], [2, 3, 0, 9]], np.uint8)
    assert R.pool_max(a, 2).tolist() == [[5, 9]]


# ---------------------------------------------------------------- cells
def test_the_cell_tables_are_fixed():
    assert len(R.cell_table("conus", 5)) == 7 * 14
    assert R.cell_table("hawaii", 5)[0] == (15, -165) and R.cell_table("hawaii", 5)[-1] == (25, -155)
    assert R.cell_table("guam", 20) == [(0, 140)]
    assert R.cell_table("alaska", 20)[0] == (40, -180)


def test_bitsets_round_trip():
    flags = [True, False, False, True, False, True] + [False] * 7 + [True]
    assert R.hex_bits(R.bits_hex(flags), len(flags)) == flags
    assert R.bits_hex([False] * 9) == ""
    assert R.bits_hex([True, False, False, False, False, True]) == "12"


def test_a_cell_picture_places_every_pixel(small):
    arr = np.zeros((200, 400), np.uint8)                               # r1: 20-22 N, 160-156 W, 100 pixels a degree
    arr[0, 0] = 50                                                     # 21.995 N, 159.995 W
    arr[199, 399] = 60                                                 # 20.005 N, 156.005 W
    pic = R.cell_picture(arr, "r1", 100, 5, 20, -160)
    n = 500
    # lat = lat0 + cell + 0.5/ppd - r/ppd; lon = lon0 - 0.5/ppd + c/ppd
    r = round((20 + 5 + 0.005 - 21.995) * 100)
    c = round((-159.995 + 160 + 0.005) * 100)
    assert pic.shape == (n + 2, n + 2) and pic[r, c] == 50
    r2 = round((20 + 5 + 0.005 - 20.005) * 100)
    c2 = round((-156.005 + 160 + 0.005) * 100)
    assert pic[r2, c2] == 60
    assert (pic > 0).sum() == 2


def test_the_apron_comes_from_the_neighbour():
    arr = np.zeros((2200, 5000), np.uint8)                             # alaska lattice
    s, w, n, e = R.box("alaska")
    row = (n - 60) * 100                                               # first row south of 60 N
    col = (-150 - w) * 100 - 1                                         # last column west of 150 W
    arr[row, col] = 77
    north = R.cell_picture(arr, "alaska", 100, 5, 60, -150)            # the cell north-east of that pixel
    assert north[-1, 0] == 77 and R.inner_echo(north) == 0             # in its apron, not counted as its echo
    own = R.cell_picture(arr, "alaska", 100, 5, 55, -155)
    assert own[1, -2] == 77 and R.inner_echo(own) == 1


def test_a_cell_keeps_echo_only():
    arr = np.ones((200, 400), np.uint8)
    pic = R.cell_picture(arr, "conus", 100, 5, 50, -130)
    assert pic.max() == 0


def test_a_stray_pixel_stays_out_of_the_coarse_tiers_and_two_paint_t1(small):
    arr = np.ones((200, 400), np.uint8)
    arr[3, 3] = 60                                                     # one hot pixel in t1's 4 x 4 block and t0's 10 x 10 block
    _o, _f, t0 = R.tiers_of(arr, "r1")
    assert t0[0, 0] == 1
    t1 = R.pool_kth(arr, R.PPD_T2 // R.PPD_T1, R.COARSE_K[R.PPD_T1])
    assert t1[0, 0] == 1
    arr[2, 2] = 50
    assert R.pool_kth(arr, R.PPD_T2 // R.PPD_T1, R.COARSE_K[R.PPD_T1])[0, 0] == 50
    assert R.tiers_of(arr, "r1")[2][0, 0] == 1                         # t0 asks for a third
    arr[1, 1] = 40
    assert R.tiers_of(arr, "r1")[2][0, 0] == 40


def test_only_cells_holding_echo_are_written(small):
    arr = np.ones((200, 400), np.uint8)                                # covered, dry: the whole-region frame alone
    objects, flags, _t0 = R.tiers_of(arr, "r1")
    assert [o[0][0] for o in objects] == ["t0"]
    arr[0, 0] = 40                                                     # one fine pixel: its t2; t1 asks for two in a block
    tiers = [o[0][0] for o in R.tiers_of(arr, "r1")[0]]
    assert tiers.count("t2") == 1 and tiers.count("t1") == 0
    arr[:20, :20] = 40
    objects, flags, _t0 = R.tiers_of(arr, "r1")
    assert [o[0][0] for o in objects].count("t2") == 1 and flags["t2"]
    assert [o[0][0] for o in objects].count("t1") == 1 and flags["t1"]


# ---------------------------------------------------------------- MRMS
def test_a_grid_that_moved_is_refused(small):
    v = np.full((200, 400), -99.0)
    with pytest.raises(ValueError):
        R.mrms_lattice("r1", mrms.read(grib(v, 22.5, -159.995, 0.01)))
    with pytest.raises(ValueError):
        R.mrms_lattice("r1", mrms.read(grib(v[:100], 21.995, -159.995, 0.01)))
    with pytest.raises(ValueError):                                    # one row over is a kilometre wrong
        R.mrms_lattice("r1", mrms.read(grib(v, 22.005, -159.995, 0.01)))
    with pytest.raises(ValueError):
        R.mrms_lattice("r1", mrms.read(grib(v, 21.995, -159.985, 0.01)))
    assert R.mrms_lattice("r1", mrms.read(grib(v, 21.995, -159.995, 0.01))).shape == (200, 400)


def test_mrms_takes_the_newest_file_of_the_slot(small):
    slot = datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc)
    files = {**r1_file(slot + timedelta(minutes=2)), **r1_file(slot + timedelta(minutes=4))}
    web = Web(files)
    _l, off, _info = R.mrms_frame("r1", slot, NOW, web)
    assert off == 4 and len([u for u in web.asked if mrms.PRODUCT in u]) == 1      # (and the minutes' PrecipFlag files: none here)
    del web.files[mrms.url_of("D1", slot + timedelta(minutes=4))]
    assert R.mrms_frame("r1", slot, NOW, web)[1] == 2


def test_mrms_never_asks_for_a_file_in_the_future(small):
    slot = datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc)
    web = Web()
    with pytest.raises(R.Missing):
        R.mrms_frame("r1", slot, slot + timedelta(minutes=1), web)
    assert web.asked == [mrms.url_of("D1", slot)]


def test_the_url_forms():
    when = datetime(2026, 11, 6, 1, 4, tzinfo=timezone.utc)
    assert mrms.url_of("HAWAII", when) == ("https://noaa-mrms-pds.s3.amazonaws.com/HAWAII/SeamlessHSR_00.00/20261106/"
                                           "MRMS_SeamlessHSR_00.00_20261106-010400.grib2.gz")
    assert opera.url_of(when) == ("https://s3.waw3-1.cloudferro.com/openradar-24h/2026/11/06/OPERA/COMP/"
                                  "OPERA@20261106T0104@0@DBZH.h5")


# ---------------------------------------------------------------- OPERA
def test_the_projection_equals_pyproj():
    for lat, lon, x, y in EXPECT["opera_forward"]:
        a, b = opera.forward(lat, lon)
        assert abs(float(a) - x) < 0.01 and abs(float(b) - y) < 0.01


def test_the_grid_corners_land_on_its_outer_corners():
    # a hundredth of the way from each corner toward the grid's centre: the grid's axes are rotated there, so never plain N / E steps
    near = lambda la, lo: (la + (55.0 - la) * 0.01, lo + (10.0 - lo) * 0.01)
    ul, ll = near(67.0228327624372, -39.5357864125034), near(31.7462153182675, -10.4345768386404)
    i = opera.source_index(np.array([ul[0], ll[0]]), np.array([ul[1], ll[1]]))
    assert 0 <= i[0] < 60 * opera.COLS and i[0] % opera.COLS < 60      # the top-left pixels
    assert i[1] // opera.COLS > opera.ROWS - 60 and i[1] % opera.COLS < 60    # the bottom-left pixels


def test_outside_the_grid_is_minus_one():
    assert opera.source_index(np.float64(30.0), np.array([10.0]))[0] == -1
    assert opera.source_index(np.float64(55.0), np.array([100.0]))[0] == -1


def write_h5(path, data=None, proj=None, shape=None, quantity=b"DBZH", corner_shift=0.0, drop=None, xsize=3800, gain=1.0, offset=0.0):
    import h5py
    shape = shape or (opera.ROWS, opera.COLS)
    data = np.full(shape, -9999000.0) if data is None else data
    with h5py.File(path, "w") as f:
        w = f.create_group("where")
        w.attrs["projdef"] = proj or b"+proj=laea +lat_0=55.0 +lon_0=10.0 +x_0=1950000.0 +y_0=-2100000.0 +units=m +ellps=WGS84"
        w.attrs["xsize"], w.attrs["ysize"] = np.uint64(xsize), np.uint64(4400)
        w.attrs["xscale"], w.attrs["yscale"] = 1000.0, 1000.0
        for name, (la, lo) in {"UL": (67.0228327624372, -39.5357864125034), "UR": (67.6210371071631, 57.8119647501499),
                               "LL": (31.7462153182675, -10.4345768386404), "LR": (31.987650276733, 29.421038635578)}.items():
            w.attrs[f"{name}_lat"], w.attrs[f"{name}_lon"] = la + corner_shift, lo
        t = f.create_group("what")
        t.attrs["date"], t.attrs["time"] = b"20261106", b"005000"
        f.create_group("how").attrs["nodes"] = b"a,b,c"
        d = f.create_group("dataset1")
        d.create_group("what").attrs["quantity"] = quantity
        g = d.create_group("data1")
        q = g.create_group("what")
        q.attrs["gain"], q.attrs["offset"] = gain, offset
        q.attrs["nodata"], q.attrs["undetect"] = -9999000.0, -8888000.0
        q.attrs["quantity"] = quantity
        g.create_dataset("data", data=data, compression="gzip", compression_opts=1)
        if drop == "corner":
            del w.attrs["UL_lat"]
        elif drop:
            del f[drop]


def test_the_reader_keeps_no_radar_and_no_echo_apart(tmp_path):
    a = np.full((opera.ROWS, opera.COLS), -9999000.0)
    a[100, 100] = -8888000.0
    a[100, 101] = 33.5
    a[100, 102] = -33.5
    p = str(tmp_path / "f.h5")
    write_h5(p, a)
    r = opera.read(p)
    assert r["time"] == "202611060050" and r["radars"] == 3
    v = r["values"]
    assert np.isneginf(v[100, 100]) and v[100, 101] == 33.5 and np.isnan(v[0, 0])
    c = R.code_table(v[100, 99:103])
    assert c.tolist() == [0, 1, R.code_table(np.array([33.5]))[0], 1]


@pytest.mark.parametrize("kw", [
    {"proj": b"+proj=stere +lat_0=90"}, {"shape": (3800, 4400)}, {"quantity": b"RATE"}, {"corner_shift": 0.2}, {"corner_shift": 0.03},
    {"corner_shift": float("nan")}, {"drop": "corner"},
    {"drop": "dataset1/data1/what"}, {"drop": "where"}, {"xsize": 3900},
])
def test_a_file_of_another_layout_is_refused(tmp_path, kw):
    p = str(tmp_path / "f.h5")
    write_h5(p, **kw)
    with pytest.raises(ValueError):
        opera.read(p)


def test_a_file_that_is_not_hdf5_is_refused(tmp_path):
    p = tmp_path / "f.h5"
    p.write_bytes(b"not hdf5")
    with pytest.raises(ValueError):
        opera.read(str(p))


def test_the_lattice_takes_the_source_pixel_under_each_point(small, monkeypatch):
    monkeypatch.setattr(R, "_OPERA_MAP", {})
    tgt, src, shape = R.opera_map()
    assert shape == (200, 400) and len(tgt) == 80000                   # all of e1 lies inside the grid
    vals = np.full((opera.ROWS, opera.COLS), np.nan, dtype=np.float32)
    x, y = opera.forward(51.005, 2.005)                                # the centre of a lattice pixel
    row, col = int(-y // 1000), int(x // 1000)
    vals[row, col] = 40.0
    lat = R.opera_lattice(vals)
    assert lat[99, 200] == R.code_table(np.array([40.0]))[0]           # 52 - 51.005 = 0.995 deg: row 99; 2.005 E: column 200
    assert (lat >= 2).sum() < 10                                       # that source pixel alone, a few lattice pixels
    assert (lat == 0).all() or (lat[lat < 2] == 0).all()               # the rest: no radar


def test_the_label_must_be_the_slot(small, monkeypatch, tmp_path):
    p = tmp_path / "f.h5"
    write_h5(str(p))
    slot = datetime(2026, 11, 6, 1, 0, tzinfo=timezone.utc)            # the file says 00:50
    web = Web({opera.url_of(slot): p.read_bytes()})
    with pytest.raises(ValueError):
        R.opera_frame(slot, web)


# ---------------------------------------------------------------- one tick
def test_a_tick_publishes_with_the_pointer_last(small, monkeypatch):
    slot = R.slot_of(NOW)
    e1_frame(monkeypatch, {R.slot_name(slot)})
    store, web = FakeStore(), Web(r1_file(slot + timedelta(minutes=4)))
    assert tick(store, web) == 0
    name = R.slot_name(slot)
    assert f"radar/v1/{name}/r1/t0.png" in store.objects and f"radar/v1/{name}/e1/t0.png" in store.objects
    assert store.order[-1] == "radar/v1/latest.json"
    first_pointer = store.order.index("radar/v1/latest.json")
    assert any(k.startswith(f"radar/v1/{name}/r1/") for k in store.order[:first_pointer])
    p = store.get_json("radar/v1/latest.json")
    assert p["format"] == "radar-v1" and p["newest"] == {"mrms": name, "opera": name}
    s = [x for x in p["slots"] if x["t"] == name][0]
    assert s["r"]["r1"]["o"] == 4 and s["r"]["e1"]["o"] == 0
    assert store.meta["radar/v1/latest.json"] == ("application/json", "public, max-age=60")
    assert store.meta[f"radar/v1/{name}/r1/t0.png"] == ("image/png", "public, max-age=31536000, immutable")


def test_the_pointer_names_exactly_the_cells_written(small, monkeypatch):
    slot = R.slot_of(NOW)
    e1_frame(monkeypatch, {R.slot_name(slot)})
    store = FakeStore()
    tick(store, Web(r1_file(slot + timedelta(minutes=4))))
    p = store.get_json("radar/v1/latest.json")
    name = R.slot_name(slot)
    for region, entry in [x for x in p["slots"] if x["t"] == name][0]["r"].items():
        for tier, cell in (("t1", 20), ("t2", 5)):
            table = R.cell_table(region, cell)
            named = {f"radar/v1/{name}/{region}/{tier}/{a}_{b}.png" for (a, b), f in zip(table, R.hex_bits(entry.get(tier, ""), len(table))) if f}
            there = {k for k in store.objects if k.startswith(f"radar/v1/{name}/{region}/{tier}/")}
            assert named == there and named


def test_a_written_t2_cell_decodes_to_the_source(small, monkeypatch):
    slot = R.slot_of(NOW)
    e1_frame(monkeypatch, set())
    store = FakeStore()
    tick(store, Web(r1_file(slot + timedelta(minutes=4), value=30.0)))
    key = f"radar/v1/{R.slot_name(slot)}/r1/t2/20_-160.png"
    a = np.array(Image.open(io.BytesIO(store.objects[key])))
    assert a.shape == (502, 502)
    # the echo is 21.9 .. 21.4 N, 159.9 .. 159.4 W; picture row = (25 + .005 - lat) * 100, column = (lon + 160 + .005) * 100
    r = round((25.005 - 21.895) * 100)
    c = round((-159.895 + 160.005) * 100)
    assert a[r, c] == R.code_table(np.array([30.0]))[0]
    assert a[r - 1, c] == 0 and a[r, c - 1] == 0


def test_t0_holds_coverage_and_echo(small, monkeypatch):
    slot = R.slot_of(NOW)
    e1_frame(monkeypatch, set())
    store = FakeStore()
    tick(store, Web(r1_file(slot + timedelta(minutes=4))))
    a = np.array(Image.open(io.BytesIO(store.objects[f"radar/v1/{R.slot_name(slot)}/r1/t0.png"])))
    assert a.shape == (20, 40)
    assert a[19, 0] == 0                                               # the south quarter: no coverage
    assert a[12, 39] == 1                                              # covered, no echo
    assert a[2, 2] == R.code_table(np.array([30.0]))[0]


class Refuses:
    def __init__(self, code):
        self.code = code

    def __call__(self, url, headers=None, timeout=60):
        raise urllib.error.HTTPError(url, self.code, "no", {}, None)


def test_fetch_tells_missing_from_down_and_caps_the_body(monkeypatch):
    monkeypatch.setattr(R.time, "sleep", lambda s: None)
    with pytest.raises(R.Missing):
        R.fetch("https://x/y", Refuses(404), 100)
    for code in (403, 500, 503):                                       # a refusal is an outage, never "not published"
        with pytest.raises(R.Down):
            R.fetch("https://x/y", Refuses(code), 100)
    with pytest.raises(ValueError):
        R.fetch("https://x/y", lambda url, headers=None, timeout=60: Answer(b"x" * 101), 100)
    assert R.fetch("https://x/y", lambda url, headers=None, timeout=60: Answer(b"x" * 100), 100) == b"x" * 100


def test_newest_never_moves_back_while_older_slots_fill(small, monkeypatch):
    slot = R.slot_of(NOW)
    e1_frame(monkeypatch, set())
    store = FakeStore()
    web = Web({**r1_file(slot + timedelta(minutes=4)), **r1_file(slot - timedelta(minutes=10) + timedelta(minutes=4))})
    tick(store, web, args=("--sources", "mrms", "--slots", "2"))
    p = store.get_json("radar/v1/latest.json")
    assert p["newest"]["mrms"] == R.slot_name(slot)
    assert R.slot_name(slot - timedelta(minutes=10)) in [x["t"] for x in p["slots"]]


def test_a_pointer_of_an_older_build_gets_the_current_credit(small, monkeypatch):
    slot = R.slot_of(NOW)
    e1_frame(monkeypatch, set())
    store = FakeStore()
    p = R.empty_pointer()
    p["credit"], p["regions"] = "old", {}
    store.put("radar/v1/latest.json", json.dumps(p).encode(), "application/json", "x")
    tick(store, Web(r1_file(slot + timedelta(minutes=4))), args=("--sources", "mrms", "--slots", "1"))
    q = store.get_json("radar/v1/latest.json")
    assert q["credit"] == R.CREDIT and sorted(q["regions"]) == ["e1", "r1"]


def test_a_late_european_frame_fills_in_at_a_later_tick(small, monkeypatch):
    slot = R.slot_of(NOW)
    name = R.slot_name(slot)
    e1_frame(monkeypatch, set())
    store = FakeStore()
    files = r1_file(slot + timedelta(minutes=4))
    assert tick(store, Web(files), args=("--sources", "mrms,opera", "--slots", "1")) == 0
    p = store.get_json("radar/v1/latest.json")
    assert "e1" not in [x for x in p["slots"] if x["t"] == name][0]["r"]
    assert not [x for x in p["slots"] if x["t"] == name][0].get("x")   # a recent slot is not counted as missing
    e1_frame(monkeypatch, {name})
    assert tick(store, Web(files), now=NOW + timedelta(minutes=10), args=("--sources", "mrms,opera", "--slots", "2")) == 0
    p = store.get_json("radar/v1/latest.json")
    assert "e1" in [x for x in p["slots"] if x["t"] == name][0]["r"]


def test_a_filled_region_is_not_asked_again(small, monkeypatch):
    slot = R.slot_of(NOW)
    name = R.slot_name(slot)
    e1_frame(monkeypatch, {name})
    store = FakeStore()
    tick(store, Web(r1_file(slot + timedelta(minutes=4))))
    web = Web()
    tick(store, web)
    assert web.asked and not [u for u in web.asked if "20261106-1204" in u or "20261106-1202" in u or "20261106-1200" in u]


def test_a_source_that_is_down_is_quiet_and_the_other_publishes(small, monkeypatch):
    slot = R.slot_of(NOW)
    name = R.slot_name(slot)
    e1_frame(monkeypatch, {name})
    store = FakeStore()
    web = Web(r1_file(slot + timedelta(minutes=4)), down=("noaa-mrms-pds",))
    assert R.main(["--slots", "3"], get=web, store=store, now=NOW) == 0
    p = store.get_json("radar/v1/latest.json")
    assert p["newest"] == {"opera": name}
    assert len([u for u in web.asked if "noaa-mrms-pds" in u]) == 2    # one retry, then the source is left alone this tick


def test_a_changed_layout_alarms_once_and_the_other_source_publishes(small, monkeypatch):
    def opera_at(now):
        e1_frame(monkeypatch, {R.slot_name(R.slot_of(now))})

    store, web = FakeStore(), Web(bad=("/D1/",))
    opera_at(NOW)
    assert tick(store, web) == 0                                       # the job has just started: nothing has been quiet for long
    assert store.get_json("radar/v1/latest.json")["newest"] == {"opera": R.slot_name(R.slot_of(NOW))}
    later = NOW + timedelta(minutes=31)
    opera_at(later)
    assert tick(store, web, now=later) == 1                            # no frame of the region for over half an hour
    assert "last_format_r1_alarm_utc" in store.get_json("radar/v1/stale-alarm.json")
    opera_at(later + timedelta(minutes=10))
    assert tick(store, web, now=later + timedelta(minutes=10)) == 0    # the same alarm within 12 h: quiet
    opera_at(later + timedelta(hours=13))
    assert tick(store, web, now=later + timedelta(hours=13)) == 1
    assert not any("r1" in s.get("x", {}) for s in store.get_json("radar/v1/latest.json")["slots"])    # a changed layout burns no slot


def test_a_source_stale_for_six_hours_alarms_once_per_twelve(small, monkeypatch):
    slot = R.slot_of(NOW)
    e1_frame(monkeypatch, {R.slot_name(slot)})
    store = FakeStore()
    files = r1_file(slot + timedelta(minutes=4))
    assert tick(store, Web(files)) == 0
    later = NOW + timedelta(hours=5)
    e1_frame(monkeypatch, {R.slot_name(R.slot_of(later))})
    assert tick(store, Web({}, down=("noaa-mrms-pds",)), now=later) == 0
    later = NOW + timedelta(hours=7)
    e1_frame(monkeypatch, {R.slot_name(R.slot_of(later))})
    web = Web({}, down=("noaa-mrms-pds",))
    assert tick(store, web, now=later) == 1
    assert "last_region_r1_alarm_utc" in store.get_json("radar/v1/stale-alarm.json")
    e1_frame(monkeypatch, {R.slot_name(R.slot_of(later + timedelta(minutes=10)))})
    assert tick(store, web, now=later + timedelta(minutes=10)) == 0


def test_a_dry_run_does_not_alarm_or_store(small, monkeypatch):
    e1_frame(monkeypatch, set())
    assert R.main(["--dry-run"], get=Web(), now=NOW) == 0              # nothing published anywhere: still no alarm
    assert R.main(["--dry-run", "--slots", "1"], get=Web(), now=NOW) == 0


def test_a_local_build_does_not_alarm(small, monkeypatch, tmp_path):
    e1_frame(monkeypatch, set())
    assert R.main(["--local", str(tmp_path / "o")], get=Web(), now=NOW) == 0


def test_an_old_file_never_published_becomes_a_gap_after_three_tries(small, monkeypatch):
    e1_frame(monkeypatch, set())
    store = FakeStore()
    old = R.slot_of(NOW) - timedelta(hours=3)
    name = R.slot_name(old)
    p = R.empty_pointer()
    store.put("radar/v1/latest.json", json.dumps(p).encode(), "application/json", "x")
    web = Web()
    for _ in range(R.MAX_TRIES):
        R.main(["--sources", "mrms"], get=web, store=store, now=NOW)
    rec = [x for x in store.get_json("radar/v1/latest.json")["slots"] if x["t"] == name][0]
    assert rec["x"]["r1"] == R.MAX_TRIES
    web2 = Web()
    R.main(["--sources", "mrms"], get=web2, store=store, now=NOW)
    assert not [u for u in web2.asked if f"{old:%Y%m%d-%H}{old.minute:02d}" in u]


def test_a_transport_failure_never_counts_as_a_gap(small, monkeypatch):
    e1_frame(monkeypatch, set())
    store = FakeStore()
    R.main(["--sources", "mrms"], get=Web(down=("noaa-mrms-pds",)), store=store, now=NOW)
    p = store.get_json("radar/v1/latest.json")
    assert p is None or not any(s.get("x") for s in p["slots"])


def test_nothing_new_rewrites_nothing(small, monkeypatch):
    e1_frame(monkeypatch, {R.slot_name(R.slot_of(NOW))})
    store = FakeStore()
    files = r1_file(R.slot_of(NOW) + timedelta(minutes=4))
    tick(store, Web(files), args=("--sources", "mrms,opera", "--slots", "1"))
    n = len(store.order)
    tick(store, Web(files), args=("--sources", "mrms,opera", "--slots", "1"))
    assert len(store.order) == n                                       # the older slot it asked for next is simply not out


def test_an_older_gap_waits_for_the_budget_but_the_newest_never_does(small, monkeypatch):
    slot = R.slot_of(NOW)
    e1_frame(monkeypatch, {R.slot_name(slot)})
    store = FakeStore()
    web = Web({**r1_file(slot + timedelta(minutes=4)), **r1_file(slot - timedelta(hours=2) + timedelta(minutes=4))})
    t = iter([0.0])

    def clock():                                                       # the start, then always 200 s in: past every backfill budget
        return next(t, 200.0)

    R.main([], get=web, store=store, now=NOW, clock=clock)
    p = store.get_json("radar/v1/latest.json")
    names = [x["t"] for x in p["slots"] if x["r"]]
    assert R.slot_name(slot) in names and R.slot_name(slot - timedelta(hours=2)) not in names


def test_the_last_source_has_the_longer_backfill_budget(small, monkeypatch):
    slot = R.slot_of(NOW)
    old = slot - timedelta(hours=2)
    e1_frame(monkeypatch, {R.slot_name(slot), R.slot_name(old)})
    store = FakeStore()
    web = Web({**r1_file(slot + timedelta(minutes=4)), **r1_file(old + timedelta(minutes=4))})
    t = iter([0.0])

    def clock():                                                       # 100 s in: over MRMS's half, under Europe's whole budget
        return next(t, 100.0)

    R.main([], get=web, store=store, now=NOW, clock=clock)
    p = store.get_json("radar/v1/latest.json")
    rec = [x for x in p["slots"] if x["t"] == R.slot_name(old)][0]
    assert "e1" in rec["r"] and "r1" not in rec["r"]


def test_the_deadline_stops_even_the_recent_slots(small, monkeypatch):
    e1_frame(monkeypatch, set())
    web = Web()
    store = FakeStore()
    t = {"v": 0.0}

    def clock():
        t["v"] += R.DEADLINE_S + 1
        return t["v"]

    R.main(["--sources", "mrms"], get=web, store=store, now=NOW, clock=clock)
    assert web.asked == []


def test_retention_grace_and_prune(small, monkeypatch):
    e1_frame(monkeypatch, set())
    store = FakeStore()
    old = R.slot_of(NOW) - timedelta(minutes=10 * R.KEEP_SLOTS)
    name = R.slot_name(old)
    keep = R.slot_name(R.slot_of(NOW) - timedelta(minutes=10 * (R.KEEP_SLOTS - 1)))
    p = R.empty_pointer()
    p["slots"] = [{"t": name, "r": {"r1": {"o": 4}}}, {"t": keep, "r": {"r1": {"o": 4}}}]
    store.put("radar/v1/latest.json", json.dumps(p).encode(), "application/json", "x")
    store.put(f"radar/v1/{name}/r1/t0.png", b"x", "image/png", "x")
    store.put(f"radar/v1/{keep}/r1/t0.png", b"x", "image/png", "x")
    web = Web()
    R.main(["--sources", "mrms", "--slots", "1"], get=web, store=store, now=NOW)
    q = store.get_json("radar/v1/latest.json")
    assert [s["t"] for s in q["slots"] if s["t"] == name] == [] and q["gone"][0]["t"] == name
    assert f"radar/v1/{name}/r1/t0.png" in store.objects               # inside its grace, for open tabs
    R.main(["--sources", "mrms", "--slots", "1"], get=web, store=store, now=NOW + timedelta(minutes=R.GRACE_MIN + 1))
    q = store.get_json("radar/v1/latest.json")
    assert name not in [g["t"] for g in q["gone"]] and f"radar/v1/{name}/r1/t0.png" not in store.objects
    assert [g["t"] for g in q["gone"]] == [keep]                       # 61 minutes on, the next slot has left the window
    assert f"radar/v1/{keep}/r1/t0.png" in store.objects               # and it has its own grace


def test_a_prune_failure_is_a_warning(small, monkeypatch):
    e1_frame(monkeypatch, set())
    store = FakeStore()
    p = R.empty_pointer()
    p["gone"] = [{"t": "202611010000", "at": "2026-11-01T00:00:00Z"}]
    store.put("radar/v1/latest.json", json.dumps(p).encode(), "application/json", "x")
    monkeypatch.setattr(store, "delete_prefix", lambda prefix: (_ for _ in ()).throw(RuntimeError("no")))
    assert R.main(["--sources", "mrms", "--slots", "1"], get=Web(), store=store, now=NOW) == 0
    assert store.get_json("radar/v1/latest.json")["gone"]


@pytest.mark.parametrize("body", [{"format": "other", "slots": [], "gone": [], "newest": {}}, {"format": "radar-v1"}, [1, 2]])
def test_a_foreign_pointer_is_refused(small, body):
    store = FakeStore()
    store.put("radar/v1/latest.json", json.dumps(body).encode(), "application/json", "x")
    n = len(store.order)
    assert R.main([], get=Web(), store=store, now=NOW) == 2
    assert len(store.order) == n


def test_an_unreadable_pointer_is_refused(small):
    store = FakeStore()
    store.put("radar/v1/latest.json", b"{not json", "application/json", "x")
    assert R.main([], get=Web(), store=store, now=NOW) == 2


def test_a_bucket_that_does_not_answer_is_quiet(small):
    class Dead(FakeStore):
        def get_json(self, key):
            raise ConnectionError("no")
    assert R.main([], get=Web(), store=Dead(), now=NOW) == 0


def test_bad_arguments():
    assert R.main(["--sources", "nope"], get=Web(), store=FakeStore(), now=NOW) == 2
    assert R.main(["--slots", "0"], get=Web(), store=FakeStore(), now=NOW) == 2


def test_the_full_pointer_fits_the_clients_cap():
    p = R.empty_pointer()
    start = R.slot_of(NOW)
    for i in range(R.KEEP_SLOTS):
        rec = {"t": R.slot_name(start - timedelta(minutes=10 * i)), "r": {}}
        for region in R.REGIONS:
            t1, t2 = R.cell_table(region, R.CELL_T1), R.cell_table(region, R.CELL_T2)
            rec["r"][region] = {"o": 4, "t1": R.bits_hex([True] * len(t1)), "t2": R.bits_hex([True] * len(t2))}
        p["slots"].append(rec)
    assert len(json.dumps(p, separators=(",", ":"))) < 180_000
    assert sum(len(R.cell_table(r, R.CELL_T2)) for r in R.REGIONS) < 500


def test_the_regions_are_whole_degrees_and_the_tiers_divide():
    for name in R.REGIONS:
        s, w, n, e = R.box(name)
        assert all(isinstance(v, int) for v in (s, w, n, e))
        for ppd in (R.PPD_T0, R.PPD_T1, R.PPD_T2):
            assert ((n - s) * ppd) % 1 == 0
    assert R.PPD_T2 % R.PPD_T1 == 0 and R.PPD_T2 % R.PPD_T0 == 0


def test_europe_holds_the_whole_grid():
    s, w, n, e = R.box("europe")
    for la, lo in ((67.0228327624372, -39.5357864125034), (67.6210371071631, 57.8119647501499),
                   (31.7462153182675, -10.4345768386404), (31.987650276733, 29.421038635578)):
        assert s < la < n and w < lo < e                               # the grid's four corners
    assert float(opera.forward(n, 10.0)[1]) > 0                        # 75 N lies above the grid's top
    assert float(opera.forward(s, 10.0)[1]) < -opera.ROWS * 1000       # 31 N lies below its bottom


# ---------------------------------------------------------------- the workflow
def _wf(name):
    return open(os.path.join(ROOT, ".github", "workflows", name), encoding="utf-8").read()


def test_the_workflow():
    wf = _wf("model-radar.yml")
    assert "group: model-radar" in wf and "cancel-in-progress: false" in wf
    assert "timeout-minutes: 12" in wf
    assert "h5py=3.14.0" in wf and "pillow=12.3.0" in wf and "numpy=2.4.6" in wf and "eccodes" not in wf.replace("# ", "")
    assert "tools/model_frames/radar.py" in wf
    assert "ubuntu-24.04" in wf


def test_the_temporary_dry_run_is_gone():
    assert not os.path.exists(os.path.join(ROOT, ".github", "workflows", "radar-dryrun.yml"))


def test_the_trigger_keepalive_and_tests_paths():
    cfg = open(os.path.join(ROOT, "tools", "model_frames", "trigger", "wrangler.jsonc"), encoding="utf-8").read()
    assert "model-radar.yml" in re.search(r'"GH_WORKFLOW":\s*"([^"]+)"', cfg).group(1).split(",")
    assert "gh workflow enable model-radar.yml" in _wf("model-frames-keepalive.yml")
    assert _wf("model-frames-tests.yml").count('".github/workflows/model-radar.yml"') == 2


# ---------------------------------------------------------------- G56a: what a file can do to a reader
BAD_GZ = b"\x1f\x8b\x08\x00" + bytes(6) + b"\xff" * 64                 # a gzip header, then a deflate stream zlib refuses
NOT_GRIB = gzip.compress(b"not grib")


@pytest.fixture
def two(monkeypatch):
    """Two regions of one source and Europe: a break in one can be seen not to touch the others."""
    monkeypatch.setattr(R, "REGIONS", {"r1": ("One", "mrms", "D1", 20, -160, 22, -156, 1),
                                       "r2": ("Two", "mrms", "D2", 20, -150, 22, -146, 1),
                                       "e1": ("Eur", "opera", None, 50, 0, 52, 4, 1)})
    monkeypatch.setattr(R.time, "sleep", lambda s: None)


def r2_file(when, value=30.0):
    v = np.full((200, 400), -99.0)
    v[10:60, 10:60] = value
    return {mrms.url_of("D2", when): gzip.compress(grib(v, 21.995, -149.995, 0.01))}


def minutes(n):
    return timedelta(minutes=n)


def slot_rec(store, name):
    recs = [x for x in store.get_json("radar/v1/latest.json")["slots"] if x["t"] == name]
    return recs[0] if recs else None


def test_a_bad_deflate_stream_is_a_damaged_file(small):
    slot = R.slot_of(NOW)
    with pytest.raises(ValueError):
        R.mrms_frame("r1", slot, NOW, Web({mrms.url_of("D1", slot + minutes(4)): BAD_GZ}))


def test_a_png_claiming_a_huge_size_is_a_damaged_file(small):
    slot = R.slot_of(NOW)
    raw = bytearray(grib(np.full((4, 6), -99.0), 25.0, -160.0, 0.01))
    i = raw.index(b"IHDR")
    raw[i + 4:i + 12] = struct.pack(">II", 30000, 30000)
    raw[i + 17:i + 21] = struct.pack(">I", zlib.crc32(bytes(raw[i:i + 17])))
    with pytest.raises(ValueError):
        mrms.read(bytes(raw))
    with pytest.raises(ValueError):
        R.mrms_frame("r1", slot, NOW, Web({mrms.url_of("D1", slot + minutes(4)): gzip.compress(bytes(raw))}))


@pytest.mark.parametrize("exc", [zlib.error("bad"), OSError("x"), TypeError("x"), AttributeError("x"), MemoryError("x"), KeyError("x")])
def test_any_reader_error_is_a_damaged_file(small, monkeypatch, exc):
    def boom(_):
        raise exc
    slot = R.slot_of(NOW)
    monkeypatch.setattr(mrms, "read", boom)
    with pytest.raises(ValueError):
        R.mrms_frame("r1", slot, NOW, Web({mrms.url_of("D1", slot + minutes(4)): gzip.compress(b"x")}))
    monkeypatch.setattr(opera, "read", boom)
    with pytest.raises(ValueError):
        R.opera_frame(slot, Web({opera.url_of(slot): b"x"}))


def hostile_h5(path, kind):
    import h5py
    write_h5(path)
    if kind == "damaged":
        with open(path, "rb") as fh:
            b = bytearray(fh.read())
        for i in range(len(b) // 2, len(b) * 9 // 10, 16):
            b[i] ^= 0xFF
        with open(path, "wb") as fh:
            fh.write(b)
        return
    with h5py.File(path, "a") as f:
        g = f["dataset1/data1"]
        if kind == "group":
            del g["data"]
            g.create_group("data")
        elif kind == "strings":
            del g["data"]
            g.create_dataset("data", data=np.zeros((opera.ROWS, opera.COLS), dtype="S1"), compression="gzip", compression_opts=1)
        elif kind == "array_attr":
            f["where"].attrs["xsize"] = np.array([3800, 3800])
        elif kind == "array_gain":
            g["what"].attrs["gain"] = np.array([1.0, 1.0])


@pytest.mark.parametrize("kind", ["damaged", "group", "strings", "array_attr", "array_gain"])
def test_a_hostile_hdf5_file_is_a_damaged_file(small, tmp_path, kind):
    p = tmp_path / "f.h5"
    hostile_h5(str(p), kind)
    slot = datetime(2026, 11, 6, 0, 50, tzinfo=timezone.utc)           # the label write_h5 gives
    with pytest.raises(ValueError):
        R.opera_frame(slot, Web({opera.url_of(slot): p.read_bytes()}))


def test_a_damaged_file_costs_nothing_else(two, monkeypatch):
    slot = R.slot_of(NOW)
    name = R.slot_name(slot)
    e1_frame(monkeypatch, {name})
    store = FakeStore()
    web = Web({mrms.url_of("D1", slot + minutes(4)): BAD_GZ, **r2_file(slot + minutes(4))})
    assert tick(store, web) == 0
    assert sorted(slot_rec(store, name)["r"]) == ["e1", "r2"]
    assert store.get_json("radar/v1/latest.json")["newest"]["opera"] == name


def test_a_changed_layout_in_one_region_leaves_the_others_and_the_older_slots(two, monkeypatch):
    slot, older = R.slot_of(NOW), R.slot_of(NOW) - minutes(10)
    e1_frame(monkeypatch, set())
    store = FakeStore()
    web = Web({**r2_file(slot + minutes(4)), **r2_file(older + minutes(4))}, bad=("/D1/",))
    assert tick(store, web) == 0                                       # the job is minutes old: too soon to mail
    for name in (R.slot_name(slot), R.slot_name(older)):
        assert list(slot_rec(store, name)["r"]) == ["r2"]
    assert len([u for u in web.asked if "/D1/" in u and mrms.PRODUCT in u]) == R.BAD_PER_TICK


def test_one_damaged_old_file_becomes_a_gap_without_a_mail(small, monkeypatch):
    slot, older = R.slot_of(NOW), R.slot_of(NOW) - minutes(10)
    e1_frame(monkeypatch, set())
    store = FakeStore()
    bad_url = mrms.url_of("D1", older + minutes(4))
    files = {**r1_file(slot + minutes(4)), bad_url: BAD_GZ}
    for _ in range(R.MAX_TRIES):
        assert R.main(["--sources", "mrms"], get=Web(files), store=store, now=NOW) == 0
    assert slot_rec(store, R.slot_name(older))["x"]["r1"] == R.MAX_TRIES
    web = Web(files)
    assert R.main(["--sources", "mrms"], get=web, store=store, now=NOW) == 0
    assert bad_url not in web.asked
    assert (store.get_json("radar/v1/stale-alarm.json") or {}) == {}


@pytest.mark.parametrize("age,rc", [(20, 0), (30, 1)])
def test_a_changed_layout_burns_no_slot_and_mails_when_the_region_goes_quiet(small, monkeypatch, age, rc):
    slot = R.slot_of(NOW)                                              # 12:00; the frame held is `age` + 6 minutes old
    lines = []
    monkeypatch.setattr(R.A, "_summary", lines.append)
    e1_frame(monkeypatch, set())
    store = FakeStore()
    p = R.empty_pointer()
    p["started"] = "2026-11-06T00:00:00Z"
    p["slots"] = [{"t": R.slot_name(slot - minutes(age)), "r": {"r1": {"o": 4}}}]
    store.put("radar/v1/latest.json", json.dumps(p).encode(), "application/json", "x")
    assert R.main(["--sources", "mrms"], get=Web(bad=("/D1/",)), store=store, now=NOW) == rc
    assert not any(s.get("x") for s in store.get_json("radar/v1/latest.json")["slots"])
    if rc:
        assert "last_format_r1_alarm_utc" in store.get_json("radar/v1/stale-alarm.json")
        assert any("r1" in line and "cannot be read" in line for line in lines)


def test_two_regions_breaking_each_get_their_own_mail(two, monkeypatch):
    e1_frame(monkeypatch, set())
    store = FakeStore()
    p = R.empty_pointer()
    p["started"] = "2026-11-06T00:00:00Z"
    store.put("radar/v1/latest.json", json.dumps(p).encode(), "application/json", "x")
    assert R.main(["--sources", "mrms"], get=Web(bad=("/D1/", "/D2/")), store=store, now=NOW) == 1
    rec = store.get_json("radar/v1/stale-alarm.json")
    assert "last_format_r1_alarm_utc" in rec and "last_format_r2_alarm_utc" in rec


# ---------------------------------------------------------------- G56a: the wire
class Recording:
    def __init__(self, answer):
        self.answer, self.timeouts, self.urls = answer, [], []

    def __call__(self, url, headers=None, timeout=60):
        self.urls.append(url)
        self.timeouts.append(timeout)
        return self.answer()


def test_a_body_shorter_than_its_length_is_a_dropped_connection(monkeypatch):
    monkeypatch.setattr(R.time, "sleep", lambda s: None)
    get = Recording(lambda: Answer(b"x" * 50, length=100))
    with pytest.raises(R.Down):
        R.fetch("https://x/y", get, 1000)
    assert len(get.urls) == 2                                          # asked once more first
    assert R.fetch("https://x/y", Recording(lambda: Answer(b"x" * 100, length=100)), 1000) == b"x" * 100
    assert R.fetch("https://x/y", Recording(lambda: Answer(b"x" * 100)), 1000) == b"x" * 100


def test_a_cut_body_is_no_format_alarm(small, monkeypatch):
    slot = R.slot_of(NOW)
    e1_frame(monkeypatch, set())
    store = FakeStore()
    body = list(r1_file(slot + minutes(4)).values())[0]

    def get(url, headers=None, timeout=60):
        if "noaa-mrms-pds" in url:
            return Answer(body[:len(body) // 2], length=len(body))
        raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)

    p = R.empty_pointer()
    p["started"] = "2026-11-06T11:00:00Z"                              # an hour ago: long enough for a format alarm, not for a stale one
    store.put("radar/v1/latest.json", json.dumps(p).encode(), "application/json", "x")
    assert R.main(["--sources", "mrms"], get=get, store=store, now=NOW) == 0
    assert store.get_json("radar/v1/stale-alarm.json") is None


def test_every_read_has_its_timeout_and_a_whole_file_its_limit(monkeypatch):
    monkeypatch.setattr(R.time, "sleep", lambda s: None)
    get = Recording(lambda: Answer(b"x"))
    R.fetch("https://x/y", get, 10)
    assert get.timeouts == [R.FETCH_TIMEOUT_S] and R.FETCH_TIMEOUT_S == 45 and R.FETCH_MAX_S == 120
    monkeypatch.setattr(R, "FETCH_MAX_S", -1)                          # a drip outruns the limit: the source is down, the file not bad
    with pytest.raises(R.Down):
        R.fetch("https://x/y", Recording(lambda: Answer(b"x" * 10)), 100)


def test_an_endless_body_is_cut_at_the_cap():
    class Endless:
        reads = 0

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self, n=-1):
            Endless.reads += 1
            assert 0 < n <= R.READ_CHUNK                               # a read never asks for the whole body
            return b"x" * n

    with pytest.raises(ValueError):
        R.fetch("https://x/y", lambda url, headers=None, timeout=60: Endless(), R.READ_CHUNK * 3)
    assert Endless.reads == 4


def test_a_refused_file_never_counts_as_a_gap(small, monkeypatch):
    e1_frame(monkeypatch, set())
    store = FakeStore()
    for _ in range(5):
        assert R.main(["--sources", "mrms"], get=Refuses(403), store=store, now=NOW) == 0
    p = store.get_json("radar/v1/latest.json")
    assert p is not None and not any(s.get("x") for s in p["slots"])


def test_the_job_names_itself_and_uses_its_own_get(monkeypatch):
    seen = {}

    def urlopen(req, timeout=None):
        seen["ua"], seen["timeout"] = req.get_header("User-agent"), timeout
        return Answer(b"")

    monkeypatch.setattr(R.urllib.request, "urlopen", urlopen)
    R._get("https://x/y", timeout=7)
    assert "radar" in seen["ua"] and "@" not in seen["ua"] and seen["timeout"] == 7
    assert inspect.signature(R.main).parameters["get"].default is R._get


def test_the_job_gives_the_bucket_short_limits(monkeypatch):
    got = {}
    monkeypatch.setattr(R.P, "r2_client", lambda *a, **k: got.update(k) or object())
    monkeypatch.setattr(R.P, "Store", lambda client, bucket: ("store", bucket))
    for name in ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"):
        monkeypatch.setenv(name, "x")
    monkeypatch.setenv("R2_BUCKET", "b")
    assert R._store_from_env() == ("store", "b")
    assert got == {"max_attempts": 2, "connect_timeout": 10, "read_timeout": 30}


def test_the_other_jobs_keep_their_client():
    pytest.importorskip("boto3")
    cfg = R.P.r2_client("a", "b", "c").meta.config
    assert cfg.connect_timeout == 60 and cfg.read_timeout == 60        # botocore's own, as before
    assert cfg.retries["total_max_attempts"] == 6                      # 5 retries
    short = R.P.r2_client("a", "b", "c", max_attempts=2, connect_timeout=10, read_timeout=30).meta.config
    assert short.connect_timeout == 10 and short.read_timeout == 30


# ---------------------------------------------------------------- G56a: time
def test_an_upload_past_the_hard_limit_stops(small):
    arr = np.ones((200, 400), np.uint8)
    store = FakeStore()
    with pytest.raises(R.Down):
        R.store_region(store, "202611061200", "r1", arr, 4, stop=lambda: True)
    assert store.objects == {}
    assert R.HARD_S == 420 and R.DEADLINE_S == 300 and R.HARD_S + 300 <= 12 * 60


def test_the_deadline_stops_the_next_region_inside_a_slot(two, monkeypatch):
    slot = R.slot_of(NOW)
    e1_frame(monkeypatch, set())
    store = FakeStore()
    t = {"v": 0.0}
    files = {**r1_file(slot + minutes(4)), **r2_file(slot + minutes(4))}
    asked = []

    def get(url, headers=None, timeout=60):
        asked.append(url)
        t["v"] = R.DEADLINE_S + 1                                      # the first region's download used the time up
        if url in files:
            return Answer(files[url])
        raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)

    R.main(["--sources", "mrms"], get=get, store=store, now=NOW, clock=lambda: t["v"])
    assert not [u for u in asked if "/D2/" in u]
    assert list(slot_rec(store, R.slot_name(slot))["r"]) == ["r1"]


def test_mrms_waits_for_the_slots_settled_file(small):
    slot = R.slot_of(NOW)
    name = R.slot_name(slot)
    p = R.empty_pointer()
    assert name not in [x[0] for x in R.plan(p, slot + timedelta(seconds=R.MRMS_READY_S - 1), "mrms")]
    assert name in [x[0] for x in R.plan(p, slot + timedelta(seconds=R.MRMS_READY_S), "mrms")]
    assert name in [x[0] for x in R.plan(p, slot + timedelta(seconds=1), "opera")]
    assert R.MRMS_READY_S == 270 and R.MRMS_OFFSETS == (4, 2, 0)


def test_the_window_is_a_day_and_a_frame(small):
    assert R.KEEP_SLOTS == 145
    assert len(R.plan(R.empty_pointer(), NOW, "opera")) == 145
    assert len(R.plan(R.empty_pointer(), NOW, "mrms")) == 145
    assert len(R.plan(R.empty_pointer(), R.slot_of(NOW) + minutes(2), "mrms")) == 144


# ---------------------------------------------------------------- G56a: alarms and a bucket that refuses
class NoWrites(FakeStore):
    def put(self, key, body, content_type, cache_control):
        raise ConnectionError("read only")


def old_pointer(**kw):
    p = R.empty_pointer()
    p["started"] = "2026-11-05T00:00:00Z"
    p.update(kw)
    return json.dumps(p).encode()


@pytest.mark.parametrize("hour,minute,rc", [(0, 0, 1), (0, 9, 1), (0, 10, 0), (12, 5, 1), (11, 59, 0), (13, 6, 0)])
def test_the_rated_failure(hour, minute, rc):
    assert R.rated(1, datetime(2026, 11, 6, hour, minute, tzinfo=timezone.utc)) == rc


def test_a_bucket_refusing_writes_fails_a_run_only_twice_a_day(small, monkeypatch):
    e1_frame(monkeypatch, set())
    store = NoWrites()
    store.objects["radar/v1/latest.json"] = old_pointer()
    assert R.main(["--sources", "mrms"], get=Web(), store=store, now=NOW) == 1                    # 12:06
    assert R.main(["--sources", "mrms"], get=Web(), store=store, now=NOW + timedelta(hours=1)) == 0
    assert R.main([], get=Web(), store=store, now=NOW + timedelta(hours=1)) == 0


def test_only_the_alarm_record_failing_is_rated_too(small, monkeypatch):
    e1_frame(monkeypatch, set())

    class NoRecord(FakeStore):
        def put(self, key, body, content_type, cache_control):
            if key.endswith("stale-alarm.json"):
                raise ConnectionError("no")
            super().put(key, body, content_type, cache_control)

    store = NoRecord()
    store.objects["radar/v1/latest.json"] = old_pointer()
    assert R.main(["--sources", "mrms"], get=Web(), store=store, now=NOW) == 1
    assert R.main(["--sources", "mrms"], get=Web(), store=store, now=NOW + timedelta(hours=1)) == 0


def test_a_corrupt_alarm_record_is_rated_not_uncaught(small, monkeypatch):
    e1_frame(monkeypatch, set())
    store = FakeStore()
    store.objects["radar/v1/latest.json"] = old_pointer()
    store.objects["radar/v1/stale-alarm.json"] = b"{not json"
    assert R.main(["--sources", "mrms"], get=Web(), store=store, now=NOW + timedelta(hours=1)) == 0
    assert R.main(["--sources", "mrms"], get=Web(), store=store, now=NOW) == 1


def test_state_alarms_that_break_are_rated(small, monkeypatch):
    e1_frame(monkeypatch, set())
    monkeypatch.setattr(R, "state_alarms", lambda *a: (_ for _ in ()).throw(RuntimeError("no")))
    store = FakeStore()
    assert R.main(["--sources", "mrms"], get=Web(), store=store, now=NOW) == 1
    assert R.main(["--sources", "mrms"], get=Web(), store=store, now=NOW + timedelta(hours=1)) == 0


def test_a_foreign_pointer_mails_only_twice_a_day(small):
    store = FakeStore()
    store.objects["radar/v1/latest.json"] = b'{"format": "other"}'
    assert R.main([], get=Web(), store=store, now=NOW) == 2
    assert R.main([], get=Web(), store=store, now=NOW + timedelta(hours=1)) == 0
    store.objects["radar/v1/latest.json"] = b"{not json"
    assert R.main([], get=Web(), store=store, now=NOW + timedelta(hours=1)) == 0
    assert R.main(["--slots", "0"], get=Web(), store=store, now=NOW + timedelta(hours=1)) == 2    # bad arguments always


@pytest.mark.parametrize("change", [
    {"slots": [{"r": {}}]}, {"slots": [{"t": "garbage", "r": {}}]}, {"slots": [{"t": 202611060000, "r": {}}]},
    {"slots": [{"t": "202611060000", "r": []}]}, {"slots": [{"t": "202611060000", "r": {}, "x": 3}]}, {"slots": [7]},
    {"newest": {"mrms": "x"}}, {"newest": {"mrms": 5}}, {"newest": []},
    {"gone": [{"t": "202611060000"}]}, {"gone": [{"t": "202611060000", "at": "soon"}]}, {"gone": [{"at": "2026-11-06T00:00:00Z"}]},
    {"gone": "x"}, {"started": "soon"},
])
def test_a_damaged_pointer_is_not_ours(change):
    p = R.empty_pointer()
    p.update(change)
    assert not R.good_pointer(p)


def test_a_sound_pointer_is_ours():
    p = R.empty_pointer()
    assert R.good_pointer(p)
    p.update(started="2026-11-06T00:00:00Z", newest={"mrms": "202611060000"}, gone=[{"t": "202611060000", "at": "2026-11-06T00:00:00Z"}],
             slots=[{"t": "202611060000", "r": {}, "x": {"r1": 1}}])
    assert R.good_pointer(p)


def test_a_first_tick_never_mails_and_starts_the_clock(small, monkeypatch):
    e1_frame(monkeypatch, set())
    store = FakeStore()
    web = Web(down=("noaa-mrms-pds",))
    assert R.main(["--sources", "mrms"], get=web, store=store, now=NOW) == 0
    p = store.get_json("radar/v1/latest.json")
    assert p["started"] == "2026-11-06T12:06:00Z"
    assert R.main(["--sources", "mrms"], get=web, store=store, now=NOW + timedelta(hours=6)) == 0
    assert store.get_json("radar/v1/latest.json")["started"] == "2026-11-06T12:06:00Z"
    assert R.main(["--sources", "mrms"], get=web, store=store, now=NOW + timedelta(hours=6, minutes=6)) == 1
    assert "last_region_r1_alarm_utc" in store.get_json("radar/v1/stale-alarm.json")


def test_a_region_going_silent_mails_though_its_source_goes_on(two, monkeypatch):
    start = R.slot_of(NOW)
    e1_frame(monkeypatch, set())
    store = FakeStore()
    assert R.main(["--sources", "mrms"], get=Web({**r1_file(start + minutes(4)), **r2_file(start + minutes(4))}), store=store, now=NOW) == 0
    later = NOW + timedelta(hours=7)
    # only r2 keeps publishing
    assert R.main(["--sources", "mrms"], get=Web(r2_file(R.slot_of(later) + minutes(4))), store=store, now=later) == 1
    rec = store.get_json("radar/v1/stale-alarm.json")
    assert "last_region_r1_alarm_utc" in rec and "last_region_r2_alarm_utc" not in rec


def test_the_stale_line_is_exact(two, monkeypatch):
    e1_frame(monkeypatch, set())
    store = FakeStore()
    name = R.slot_name(R.slot_of(NOW))
    p = R.empty_pointer()
    p.update(started="2026-11-06T00:00:00Z", slots=[{"t": name, "r": {"r1": {"o": 4}, "r2": {"o": 4}}}])
    store.put("radar/v1/latest.json", json.dumps(p).encode(), "application/json", "x")
    web = Web(down=("noaa-mrms-pds",))
    assert R.main(["--sources", "mrms"], get=web, store=store, now=R.slot_of(NOW) + timedelta(hours=R.SOURCE_STALE_H)) == 0
    assert R.main(["--sources", "mrms"], get=web, store=store, now=R.slot_of(NOW) + timedelta(hours=6.1)) == 1
    assert R.SOURCE_STALE_H == 6 and R.FORMAT_ALARM_MIN == 30


# ---------------------------------------------------------------- G56a: deleting
class Deletes(FakeStore):
    def __init__(self, fail=False):
        super().__init__()
        self.deleted, self.fail = [], fail

    def delete_prefix(self, prefix):
        self.deleted.append(prefix)
        if self.fail:
            raise ConnectionError("no")


def gone_list(n, at="2026-11-06T00:00:00Z"):
    base = datetime(2026, 11, 5, tzinfo=timezone.utc)
    return [{"t": R.slot_name(base + minutes(10 * i)), "at": at} for i in range(n)]


def test_a_backlog_is_deleted_a_few_slots_a_tick():
    store = Deletes()
    p = R.empty_pointer()
    p["gone"] = gone_list(20)
    assert R.prune_gone(store, p, NOW) is True
    assert len(store.deleted) == R.PRUNE_PER_TICK == 6 and len(p["gone"]) == 14
    assert all(d.startswith("radar/v1/") and d.endswith("/") for d in store.deleted)   # the slash: never another slot's prefix


def test_a_failed_delete_ends_the_deleting_for_the_tick():
    store = Deletes(fail=True)
    p = R.empty_pointer()
    p["gone"] = gone_list(5)
    assert R.prune_gone(store, p, NOW) is False
    assert len(store.deleted) == 1 and len(p["gone"]) == 5


def test_no_delete_past_the_time_limit():
    store = Deletes()
    p = R.empty_pointer()
    p["gone"] = gone_list(5)
    assert R.prune_gone(store, p, NOW, over=lambda: True) is False
    assert store.deleted == [] and len(p["gone"]) == 5


def test_a_slot_inside_its_grace_does_not_use_a_turn():
    store = Deletes()
    p = R.empty_pointer()
    p["gone"] = gone_list(8, at=R.A.utc(NOW)) + gone_list(3)
    R.prune_gone(store, p, NOW)
    assert len(store.deleted) == 3 and len(p["gone"]) == 8


@pytest.mark.parametrize("hours,rc", [(5, 0), (7, 1)])
def test_slots_that_cannot_be_deleted_mail_at_six_hours(small, monkeypatch, hours, rc):
    e1_frame(monkeypatch, set())
    store = Deletes(fail=True)
    now = NOW + timedelta(hours=1)
    p = R.empty_pointer()
    p.update(started=R.A.utc(now - minutes(5)), gone=gone_list(1, at=R.A.utc(now - timedelta(hours=hours))))
    store.objects["radar/v1/latest.json"] = json.dumps(p).encode()
    assert R.main(["--sources", "mrms"], get=Web(), store=store, now=now) == rc
    if rc:
        assert "last_prune_alarm_utc" in store.get_json("radar/v1/stale-alarm.json")
        assert R.main(["--sources", "mrms"], get=Web(), store=store, now=now + minutes(10)) == 0


# ---------------------------------------------------------------- G56a: pins for what the first suite did not read
def test_the_coverage_threshold_and_a_hot_pixel_over_no_coverage():
    assert R.code_table(np.array([-100.0, -899.5, -900.5, -1000.0])).tolist() == [1, 1, 0, 0]
    a = np.zeros((10, 10), np.uint8)
    a[3, 3] = 100
    assert R.pool_kth(a, 10, 3)[0, 0] == 1                             # covered: one stray pixel over nothing else is not "no radar"
    assert R.pool_kth(a, 10, 1)[0, 0] == 100


@pytest.mark.parametrize("field,value", [("di", 0.02), ("dj", 0.02), ("lon1", -159.9), ("nj", 100), ("ni", 100)])
def test_a_grid_of_another_spacing_is_refused(small, field, value):
    msg = mrms.read(grib(np.full((200, 400), -99.0), 21.995, -159.995, 0.01))
    msg[field] = value
    with pytest.raises(ValueError):
        R.mrms_lattice("r1", msg)


def test_slots_are_stored_in_order(small, monkeypatch):
    slot = R.slot_of(NOW)
    e1_frame(monkeypatch, set())
    store = FakeStore()
    web = Web({**r1_file(slot + minutes(4)), **r1_file(slot - minutes(6))})
    tick(store, web, args=("--sources", "mrms", "--slots", "2"))
    names = [x["t"] for x in store.get_json("radar/v1/latest.json")["slots"]]
    assert len(names) == 2 and names == sorted(names)


def test_the_credit_says_whose_and_what_was_done():
    for word in ("NOAA", "MRMS", "EUMETNET", "OPERA", "CC BY 4.0", "property rights EUMETNET", "modified", "https://creativecommons.org/licenses/by/4.0/"):
        assert word in R.CREDIT
    assert R.empty_pointer()["credit"] == R.CREDIT


def test_the_cell_thresholds_are_exact(small):
    def tiers(arr):
        return [o[0][0] for o in R.tiers_of(arr, "r1")[0]]

    for n, want in ((R.T2_MIN - 1, 0), (R.T2_MIN, 1)):
        arr = np.ones((200, 400), np.uint8)
        arr[0, :n] = R.MIN_CODE
        assert tiers(arr).count("t2") == want
    arr = np.ones((200, 400), np.uint8)
    arr[0, :R.T2_MIN] = R.MIN_CODE - 1
    assert tiers(arr).count("t2") == 0                                 # below 10 dBZ it is echo, but not echo enough
    for blocks, want in ((R.T1_MIN - 1, 0), (R.T1_MIN, 1)):            # each t1 pixel needs two fine pixels in its 4 x 4 block
        arr = np.ones((200, 400), np.uint8)
        for i in range(blocks):
            arr[0, 4 * i:4 * i + 2] = R.MIN_CODE
        assert tiers(arr).count("t1") == want
    assert (R.T1_MIN, R.T2_MIN, R.MIN_DBZ) == (1, 1, 10.0)             # the owner's call: no tier flips in light showers


def test_the_lattice_equals_the_projection_everywhere(small, monkeypatch):
    monkeypatch.setattr(R, "_OPERA_MAP", {})
    rows, cols = np.indices((opera.ROWS, opera.COLS))
    vals = (5.0 + ((rows * 7 + cols * 13) % 140) * 0.5).astype(np.float32)   # every source pixel names itself: half a pixel off shows
    lat = R.opera_lattice(vals)
    lons, lats = np.meshgrid((np.arange(400) + 0.5) / 100, 52 - (np.arange(200) + 0.5) / 100)
    x, y = opera.forward(lats, lons)
    want = R.code_table(vals[np.floor(-y / 1000).astype(int), np.floor(x / 1000).astype(int)])
    assert (lat == want).all()


def test_the_east_edge_of_the_grid():
    lo, hi = 30.0, 70.0
    for _ in range(50):
        mid = (lo + hi) / 2
        if float(opera.forward(55.0, mid)[0]) < opera.COLS * opera.PIXEL_M:
            lo = mid
        else:
            hi = mid
    inside = int(opera.source_index(np.float64(55.0), np.array([lo - 1e-4]))[0])
    assert inside % opera.COLS == opera.COLS - 1
    assert int(opera.source_index(np.float64(55.0), np.array([hi + 1e-4]))[0]) == -1   # never the next row's first column


def test_the_gzip_limits(monkeypatch):
    with pytest.raises(ValueError):
        monkeypatch.setattr(mrms, "MAX_RAW_BYTES", 100)
        mrms.gunzip(gzip.compress(b"x" * 1000))
    monkeypatch.undo()
    with pytest.raises(ValueError):
        monkeypatch.setattr(mrms, "MAX_GZ_BYTES", 5)
        mrms.gunzip(gzip.compress(b"x" * 1000))
    monkeypatch.undo()
    asked = []

    class Fake:
        def __init__(self, fileobj):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self, n=-1):
            asked.append(n)
            return b"x"

    monkeypatch.setattr(mrms.gzip, "GzipFile", Fake)
    mrms.gunzip(b"anything")
    assert asked == [mrms.MAX_RAW_BYTES + 1]                           # a bomb stops at the limit


def walk_to(raw, number):
    p = 16
    while True:
        n, num = struct.unpack(">IB", raw[p:p + 5])
        if num == number:
            return p, n
        p += n


def test_a_lying_section_length_is_refused():
    raw = bytearray(grib(np.full((4, 6), -99.0), 25.0, -160.0, 0.01))
    p, _n = walk_to(raw, 3)
    for lie in (4, len(raw) * 2):
        bad = bytearray(raw)
        bad[p:p + 4] = struct.pack(">I", lie)
        with pytest.raises(ValueError):
            mrms.read(bytes(bad))
    p, n = walk_to(raw, 7)                                             # the last section reaching one byte into the end marker
    bad = bytearray(raw)
    bad[p:p + 4] = struct.pack(">I", n + 1)
    with pytest.raises(ValueError):
        mrms.read(bytes(bad))


def test_a_file_of_two_fields_is_refused():
    raw = grib(np.full((4, 6), -99.0), 25.0, -160.0, 0.01)
    p, n = walk_to(raw, 7)
    two_fields = bytearray(raw[:-4] + raw[p:p + n] + b"7777")
    two_fields[8:16] = struct.pack(">Q", len(two_fields))
    with pytest.raises(ValueError):
        mrms.read(bytes(two_fields))


def test_a_picture_of_another_size_is_refused():
    raw = bytearray(grib(np.full((4, 6), -99.0), 25.0, -160.0, 0.01))
    p, _n = walk_to(raw, 3)
    raw[p + 30:p + 38] = struct.pack(">II", 7, 4)
    with pytest.raises(ValueError):
        mrms.read(bytes(raw))


@pytest.mark.parametrize("scales,x,want", [
    ((0, 1), 3, -998.7), ((1, 1), 3, -998.4), ((0x8001, 1), 3, -998.85), ((0, 0), 3, -9987.0), ((0, 0x8001), 3, -99870.0),
])
def test_the_scales_are_read_as_grib_writes_them(scales, x, want):
    msg = mrms.read(grib(np.full((4, 6), -99.0), 25.0, -160.0, 0.01, scales=scales))
    assert msg["binary_scale"] == (-1 if scales[0] == 0x8001 else scales[0])
    assert abs(float(mrms.value_table(msg)[x]) - want) < 0.01


def test_opera_gain_and_offset_are_applied(tmp_path):
    a = np.full((opera.ROWS, opera.COLS), -9999000.0)
    a[5, 5] = 10.0
    p = str(tmp_path / "f.h5")
    write_h5(p, a, gain=2.0, offset=1.0)
    assert opera.read(p)["values"][5, 5] == 21.0


def test_an_upload_outliving_the_hard_limit_stops_the_source_for_the_tick(two, monkeypatch):
    slot = R.slot_of(NOW)
    e1_frame(monkeypatch, set())
    store = FakeStore()
    t = {"v": 0.0}
    files = {**r1_file(slot + minutes(4)), **r2_file(slot + minutes(4))}

    def get(url, headers=None, timeout=60):
        t["v"] = R.HARD_S + 1
        return Answer(files[url])

    R.main(["--sources", "mrms"], get=get, store=store, now=NOW, clock=lambda: t["v"])
    assert not [k for k in store.objects if k.endswith(".png")]


def test_the_frame_just_published_keeps_the_regions_clock(small, monkeypatch):
    slot, older = R.slot_of(NOW), R.slot_of(NOW) - minutes(10)
    e1_frame(monkeypatch, set())
    store = FakeStore()
    p = R.empty_pointer()
    p["started"] = "2026-11-06T10:00:00Z"                              # two hours ago: a bad file would mail if the region looked quiet
    store.put("radar/v1/latest.json", json.dumps(p).encode(), "application/json", "x")
    files = {**r1_file(slot + minutes(4)), mrms.url_of("D1", older + minutes(4)): BAD_GZ}
    assert R.main(["--sources", "mrms"], get=Web(files), store=store, now=NOW) == 0
    assert slot_rec(store, R.slot_name(older))["x"]["r1"] == 1


def test_two_regions_stale_each_get_their_own_mail(two, monkeypatch):
    e1_frame(monkeypatch, set())
    store = FakeStore()
    store.objects["radar/v1/latest.json"] = old_pointer()
    assert R.main(["--sources", "mrms"], get=Web(down=("noaa-mrms-pds",)), store=store, now=NOW) == 1
    rec = store.get_json("radar/v1/stale-alarm.json")
    assert "last_region_r1_alarm_utc" in rec and "last_region_r2_alarm_utc" in rec


def test_a_tick_past_the_deadline_deletes_nothing(small, monkeypatch):
    e1_frame(monkeypatch, set())
    store = Deletes()
    p = R.empty_pointer()
    p.update(started="2026-11-06T11:00:00Z", gone=gone_list(2))
    store.objects["radar/v1/latest.json"] = json.dumps(p).encode()
    t = iter([0.0])
    R.main(["--sources", "mrms"], get=Web(), store=store, now=NOW, clock=lambda: next(t, R.DEADLINE_S + 1.0))
    assert store.deleted == []


def test_a_retired_slot_reaches_the_pointer_on_a_tick_with_nothing_else_to_write(small, monkeypatch):
    e1_frame(monkeypatch, set())
    store = FakeStore()
    old = R.slot_name(R.slot_of(NOW) - timedelta(minutes=10 * R.KEEP_SLOTS))
    p = R.empty_pointer()
    p.update(started="2026-11-06T11:00:00Z", slots=[{"t": old, "r": {"r1": {"o": 4}}}])
    store.put("radar/v1/latest.json", json.dumps(p).encode(), "application/json", "x")
    assert R.main(["--sources", "mrms", "--slots", "1"], get=Web(), store=store, now=NOW) == 0
    assert [g["t"] for g in store.get_json("radar/v1/latest.json")["gone"]] == [old]


def test_a_local_build_never_alarms_on_an_old_pointer(small, monkeypatch, tmp_path):
    e1_frame(monkeypatch, set())
    target = tmp_path / "radar" / "v1" / "latest.json"
    target.parent.mkdir(parents=True)
    target.write_bytes(old_pointer())
    assert R.main(["--local", str(tmp_path), "--sources", "mrms", "--slots", "1"], get=Web(), now=NOW) == 0
    assert R.main(["--local", str(tmp_path), "--sources", "mrms"], get=Web(), now=NOW) == 0
    assert not (tmp_path / "radar" / "v1" / "stale-alarm.json").exists()
