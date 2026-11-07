"""Japan's and Taiwan's radar frames (plan section 86), on real fixtures (tests/capture_radar2_fixtures.py). Nothing reaches the network."""
import gzip
import io
import json
import math
import os
import re
import sys
import urllib.error
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest
from PIL import Image

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
import cwa                # noqa: E402
import jma                # noqa: E402
import radar as R         # noqa: E402

FIX = os.path.join(ROOT, "tests", "fixtures", "radar")
NOW = datetime(2026, 11, 7, 4, 16, 0, tzinfo=timezone.utc)
SLOT = R.slot_of(NOW)
POINTER = "radar/v1/latest.json"


def fixture(name):
    with open(os.path.join(FIX, name), "rb") as fh:
        return fh.read()


RAIN, DRY, OUTLINE, CWA_HEAD = (fixture(n) for n in ("jma_tile_rain.png", "jma_tile_dry.png", "jma_outline.geojson.gz", "cwa_head.xml"))


class Store:
    bucket = "fake"

    def __init__(self):
        self.objects = {}

    def put(self, key, body, content_type, cache_control):
        self.objects[key] = body

    def get_json(self, key):
        return json.loads(self.objects[key].decode()) if key in self.objects else None

    def delete_prefix(self, prefix):
        for k in [k for k in self.objects if k.startswith(prefix)]:
            del self.objects[k]


class Answer:
    def __init__(self, body):
        self.stream = io.BytesIO(body)
        self.headers = {"Content-Length": str(len(body))}

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self, n=-1):
        return self.stream.read(n)


class Web:
    """Maps URL patterns to bodies; a body that is an int answers that status."""

    def __init__(self, routes):
        self.routes, self.asked = routes, []

    def __call__(self, url, headers=None, timeout=60):
        self.asked.append(url)
        for pat, body in self.routes:
            if re.search(pat, url):
                if isinstance(body, int):
                    raise urllib.error.HTTPError(url, body, "x", {}, None)
                if isinstance(body, Exception):
                    raise body
                return Answer(body)
        raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)


def jma_web(*extra):
    return Web(list(extra) + [(r"hrpns_nd/data\.geojson", OUTLINE), (r"/hrpns/6/56/25\.png", RAIN), (r"/hrpns/6/\d+/\d+\.png", DRY)])


def png_of(arr, palette=None, transparency=None):
    arr = np.asarray(arr, dtype=np.uint8)
    im = Image.frombytes("P", (arr.shape[1], arr.shape[0]), arr.tobytes())
    im.putpalette(palette or [255, 255, 255] * 256)
    buf = io.BytesIO()
    kw = {} if transparency is None else {"transparency": transparency}
    im.save(buf, "PNG", **kw)
    return buf.getvalue()


@pytest.fixture(autouse=True)
def fresh(monkeypatch):
    R._JMA.clear()
    R._CWA.clear()
    monkeypatch.setattr(R.time, "sleep", lambda s: None)
    yield
    R._JMA.clear()
    R._CWA.clear()


# ---------------------------------------------------------------- JMA: tiles
def test_a_real_rain_tile_decodes_to_the_classes_its_colours_say():
    got = jma.decode_tile(RAIN)
    rgba = np.asarray(Image.open(io.BytesIO(RAIN)).convert("RGBA"))
    assert got.shape == (256, 256) and got.any()
    of = {c[0]: i + 1 for i, c in enumerate(jma.CLASSES)}
    for r, c in zip(*np.nonzero(got)):
        assert of[tuple(int(v) for v in rgba[r, c, :3])] == got[r, c]
        assert rgba[r, c, 3] == 255
    assert ((rgba[..., 3] == 0) == (got == 0)).all()


def test_the_dry_picture_is_a_dry_tile():
    assert len(DRY) == 334
    assert not jma.decode_tile(DRY).any()


def test_an_opaque_pixel_on_an_rgba_tile_is_a_changed_feed():
    a = np.zeros((256, 256, 4), dtype=np.uint8)
    a[3, 3] = (9, 9, 9, 255)
    buf = io.BytesIO()
    Image.fromarray(a, "RGBA").save(buf, "PNG")
    with pytest.raises(ValueError, match="opaque"):
        jma.decode_tile(buf.getvalue())


def test_a_colour_off_the_table_is_a_changed_feed():
    pal = [1, 2, 3] * 256
    body = png_of(np.ones((256, 256)), palette=pal, transparency=b"\x00")      # index 1 opaque, not on the table
    with pytest.raises(ValueError, match="rain-rate table"):
        jma.decode_tile(body)


def test_a_colour_off_the_table_on_a_transparent_index_does_not_matter():
    pal = [1, 2, 3] * 256
    assert not jma.decode_tile(png_of(np.zeros((256, 256)), palette=pal, transparency=b"\x00")).any()


def test_the_transparent_index_can_be_a_single_number():
    pal = [242, 242, 255] * 256
    arr = np.zeros((256, 256))
    arr[0, 0] = 1
    got = jma.decode_tile(png_of(arr, palette=pal, transparency=0))
    assert got[0, 0] == 1 and got.sum() == 1


def test_palette_entries_past_a_short_transparency_table_are_opaque():
    pal = [9, 9, 9] * 2 + [160, 210, 255] * 254
    arr = np.full((256, 256), 2)
    arr[0, 0] = 0
    got = jma.decode_tile(png_of(arr, palette=pal, transparency=b"\x00\x00"))
    assert got[0, 0] == 0 and got[0, 1] == 2 and got[255, 255] == 2


@pytest.mark.parametrize("size,mode", [((128, 128), "RGBA"), ((256, 256), "L"), ((256, 256), "RGB")])
def test_another_size_or_mode_is_a_changed_feed(size, mode):
    buf = io.BytesIO()
    Image.new(mode, size).save(buf, "PNG")
    with pytest.raises(ValueError):
        jma.decode_tile(buf.getvalue())


def test_each_class_is_the_code_of_its_geometric_middle():
    for (_rgb, lo, hi, code) in jma.CLASSES[1:7]:
        mid = math.sqrt(lo * hi)
        dbz = 10 * math.log10(200 * mid ** 1.6)
        assert code == 2 + round((dbz - R.FLOOR_DBZ) / R.STEP_DBZ), (lo, hi)
    assert all(c >= R.MIN_CODE for c in jma.CLASS_CODES)      # every class passes the cell rule
    assert list(jma.CLASS_CODES) == sorted(set(jma.CLASS_CODES))


def test_the_page_shares_the_class_table():
    src = open(os.path.join(ROOT, "static_overlay", "overlay.js"), encoding="utf-8").read()
    m = re.search(r"RADAR_JMA8 = \[([^;]+)\];", src)
    if not m and "RADAR_JMA8" not in src:
        pytest.skip("the job's branch before the client's commit")
    assert m, "the client's class table"
    rows = re.findall(r"\[(\d+), (\d+), (\d+|null), '", m.group(1))
    assert [(int(a), int(b), None if c == "null" else int(c)) for a, b, c in rows] == [(c[3], c[1], c[2]) for c in jma.CLASSES]


# ---------------------------------------------------------------- JMA: geometry
def test_the_tile_rows_and_columns_are_web_mercator():
    # the top of tile row 25 at zoom 6, by the tile formula
    lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * 25 / 64))))
    assert jma.merc_row(lat) == pytest.approx(25 * 256, abs=1e-6)
    assert jma.merc_col(-180.0) == 0 and jma.merc_col(0.0) == 32 * 256
    assert jma.merc_col(101.25) == pytest.approx(50 * 256)


def test_lattice_pixels_read_the_source_pixel_under_their_centres():
    s, w, n, e = R.box("japan")
    rows, cols = jma.lattice_index(s, w, n, e, 100)
    for r in range(0, 2700, 7):
        lat = math.radians(n - (r + 0.5) / 100)
        wy = (1 - math.log(math.tan(lat) + 1 / math.cos(lat)) / math.pi) / 2 * 64 * 256
        assert rows[r] == int(wy) - 22 * 256, r
    for c in (0, 133, 1350, 2699):
        lon = w + (c + 0.5) / 100
        assert cols[c] == int((lon + 180) / 360 * 64 * 256) - 53 * 256, c


def test_lattice_pixels_beyond_the_mosaic_are_marked():
    rows, cols = jma.lattice_index(70, 90, 75, 170, 10)
    assert (rows == -1).all() and (cols == -1).sum() > 0 and (cols >= 0).sum() > 0
    rows, _ = jma.lattice_index(0, 122, 5, 123, 10)
    assert (rows == -1).all()


def test_a_mask_counts_the_rain_it_drops():
    classes = np.zeros((4, 4), dtype=np.uint8)
    classes[0, :] = 3
    rows, cols = np.arange(4), np.arange(4)
    mask = np.ones((4, 4), dtype=bool)
    mask[0, 1:3] = False
    codes, dropped = jma.lattice(classes, rows, cols, mask)
    assert dropped == 2
    assert codes[0, 0] == jma.CLASS_CODES[2] and codes[0, 1] == 0 and codes[1, 1] == 1


def test_the_lattice_finds_each_source_pixel():
    s, w, n, e = R.box("japan")
    rows, cols = jma.lattice_index(s, w, n, e, 100)
    assert len(rows) == (n - s) * 100 and len(cols) == (e - w) * 100
    assert (rows >= 0).all() and (cols >= 0).all()             # the census's tiles reach past the whole box
    assert (np.diff(rows) >= 0).all() and (np.diff(cols) >= 0).all()   # north to south, west to east
    assert set(np.diff(cols).tolist()) <= {0, 1} and set(np.diff(rows).tolist()) <= {0, 1}   # no source pixel is skipped


def test_a_point_lands_on_its_tile_pixel():
    s, w, n, e = R.box("japan")
    rows, cols = jma.lattice_index(s, w, n, e, 100)
    classes = np.zeros((len(jma.YS) * 256, len(jma.XS) * 256), dtype=np.uint8)
    # Tokyo: 35.68 N 139.77 E
    r, c = int((n - 35.68) * 100), int((139.77 - w) * 100)
    wy = jma.merc_row(35.675 + 0.0) - jma.YS[0] * 256
    classes[rows[r], cols[c]] = 3
    mask = np.ones((len(rows), len(cols)), dtype=bool)
    codes, dropped = jma.lattice(classes, rows, cols, mask)
    assert codes[r, c] == jma.CLASS_CODES[2] and dropped == 0
    assert abs(rows[r] - wy) <= 2


def test_the_real_outline_is_one_data_area():
    rings = jma.parse_outline(OUTLINE)
    assert len(rings) >= 1
    assert 120 < rings[0][:, 0].min() < 123 and 148 < rings[0][:, 0].max() < 150
    assert 19 < rings[0][:, 1].min() < 22 and 46 < rings[0][:, 1].max() < 47.5


def mask_at(mask, lat, lon):
    s, w, n, e = R.box("japan")
    return bool(mask[int((n - lat) * 100), int((lon - w) * 100)])


def test_the_real_outline_as_a_mask():
    s, w, n, e = R.box("japan")
    mask = jma.coverage(jma.parse_outline(OUTLINE), s, w, n, e, 100)
    for lat, lon in ((35.68, 139.77), (43.06, 141.35), (26.33, 127.8), (33.59, 130.4), (24.4, 124.2)):
        assert mask_at(mask, lat, lon), (lat, lon)
    for lat, lon in ((25.03, 121.56), (22.0, 121.0), (46.9, 148.9)):       # Taipei and Taiwan's east are cut; past the north edge
        if lon >= w:
            assert not mask_at(mask, lat, lon), (lat, lon)
    assert not mask[:, :int((jma.CUT_WEST - w) * 100)].any()


def test_an_absurd_area_refuses_the_frame():
    ring = np.array([[130.0, 30.0], [131.0, 30.0], [131.0, 31.0], [130.0, 31.0], [130.0, 30.0]])
    s, w, n, e = R.box("japan")
    with pytest.raises(ValueError, match="square degrees"):
        jma.coverage([ring], s, w, n, e, 100)


def test_fill_rings_equals_a_brute_force_even_odd():
    outer = np.array([[2.0, 2.0], [8.0, 2.5], [7.5, 8.0], [2.5, 7.0], [2.0, 2.0]])
    inner = np.array([[4.0, 4.0], [6.0, 4.0], [6.0, 6.0], [4.0, 6.0], [4.0, 4.0]])
    got = jma.fill_rings([outer, inner], 0, 0, 10, 10, 10)

    def inside(x, y, ring):
        c = False
        for i in range(len(ring) - 1):
            (x0, y0), (x1, y1) = ring[i], ring[i + 1]
            if (y0 <= y) != (y1 <= y) and x < x0 + (y - y0) / (y1 - y0) * (x1 - x0):
                c = not c
        return c
    for r in range(100):
        for col in range(100):
            y, x = 10 - (r + .5) / 10, (col + .5) / 10
            assert got[r, col] == (inside(x, y, outer) != inside(x, y, inner)), (r, col)


def test_a_ring_left_open_is_closed():
    closed = np.array([[2.0, 2.0], [8.0, 2.0], [8.0, 8.0], [2.0, 8.0], [2.0, 2.0]])
    assert (jma.fill_rings([closed[:-1]], 0, 0, 10, 10, 10) == jma.fill_rings([closed], 0, 0, 10, 10, 10)).all()


@pytest.mark.parametrize("doc", [
    {"features": []},
    {"features": [{"geometry": {"type": "Polygon", "coordinates": [[[-45, 85], [315, 85], [315, -85], [-45, 85]]]}}]},
    {"features": [{"geometry": {"type": "Polygon", "coordinates": [[[130, 30], [131, 30], [131, 31], [130, 30]]]}}]},
    {"features": [{"geometry": {"type": "Point", "coordinates": [130, 30]}}]},
    {"features": [{"geometry": {"type": "Polygon", "coordinates": [[[-45, 85], [315, 85], [315, -85], [-45, 85]], [[130, 30], [131, 30], [131, 31], [130, 30]]]}},
                  {"geometry": {"type": "Point", "coordinates": [130, 30]}}]},
])
def test_an_outline_of_any_other_shape_is_refused(doc):
    with pytest.raises(ValueError):
        jma.parse_outline(json.dumps(doc).encode())


def test_an_unpacked_outline_past_the_cap_is_refused(monkeypatch):
    monkeypatch.setattr(jma, "MAX_OUTLINE_UNPACKED", 1000)
    with pytest.raises(ValueError, match="too large"):
        jma.parse_outline(gzip.compress(b" " * 5000))


# ---------------------------------------------------------------- JMA: a frame
def test_a_frame_is_rain_dry_and_uncovered():
    web = jma_web()
    codes, note = R.jma_frame("japan", SLOT, web)
    s, w, n, e = R.box("japan")
    assert codes.shape == ((n - s) * 100, (e - w) * 100)
    assert set(np.unique(codes).tolist()) <= {0, 1} | set(jma.CLASS_CODES)
    assert (codes >= 2).any() and (codes == 1).any() and (codes == 0).any()
    assert "1 of 42 tiles" in note


def test_a_tile_answering_404_means_the_frame_is_not_there():
    web = jma_web((r"/hrpns/6/58/28\.png", 404))
    with pytest.raises(R.Missing):
        R.jma_frame("japan", SLOT, web)


def test_a_frame_not_yet_out_costs_one_request():
    web = Web([(r"hrpns_nd", OUTLINE), (r"/hrpns/6/56/25\.png", 404), (r"/hrpns/", DRY)])
    with pytest.raises(R.Missing):
        R.jma_frame("japan", SLOT, web)
    assert len(web.asked) == 1 and web.asked[0].endswith("/56/25.png")


def test_a_refusal_is_down_not_never_published():
    web = jma_web((r"/hrpns/6/58/28\.png", 500))
    with pytest.raises(R.Down):
        R.jma_frame("japan", SLOT, web)


def test_an_opaque_rgba_tile_stops_the_frame_as_a_changed_feed():
    a = np.zeros((256, 256, 4), dtype=np.uint8)
    a[3, 3] = (9, 9, 9, 255)
    buf = io.BytesIO()
    Image.fromarray(a, "RGBA").save(buf, "PNG")
    web = jma_web((r"/hrpns/6/57/24\.png", buf.getvalue()))
    with pytest.raises(ValueError, match="opaque"):
        R.jma_frame("japan", SLOT, web)


def test_a_body_that_is_no_picture_is_a_damaged_file():
    web = jma_web((r"/hrpns/6/57/24\.png", b"<html>maintenance</html>"))
    with pytest.raises(ValueError, match="JMA tile"):
        R.jma_frame("japan", SLOT, web)


def test_a_whole_dry_box_is_a_valid_frame():
    web = Web([(r"hrpns_nd", OUTLINE), (r"/hrpns/", DRY)])
    codes, note = R.jma_frame("japan", SLOT, web)
    assert set(np.unique(codes).tolist()) == {0, 1} and "0 of 42" in note


def test_the_outline_is_read_once_a_tick():
    web = jma_web()
    R.jma_frame("japan", SLOT, web)
    R.jma_frame("japan", SLOT - timedelta(minutes=10), web)
    assert sum("hrpns_nd" in u for u in web.asked) == 1
    assert sum("/hrpns/" in u for u in web.asked) == 84


def test_a_tile_over_its_size_limit_is_a_changed_feed():
    web = jma_web((r"/hrpns/6/57/24\.png", b"x" * (jma.MAX_TILE_BYTES + 1)))
    with pytest.raises(ValueError, match="more than"):
        R.jma_frame("japan", SLOT, web)


def test_a_rain_pixel_outside_the_data_area_is_logged():
    outline = gzip.decompress(OUTLINE)
    doc = json.loads(outline)
    # a data area over the far north-east only: the rain tile (56, 25) falls outside it
    doc["features"] = [{"geometry": {"type": "Polygon", "coordinates": [
        [[-45, 85], [315, 85], [315, -85], [-45, -85], [-45, 85]],
        [[141, 20], [149, 20], [149, 47], [141, 47], [141, 20]]]}}]
    web = Web([(r"hrpns_nd", json.dumps(doc).encode()), (r"/hrpns/6/56/25\.png", RAIN), (r"/hrpns/", DRY)])
    _codes, note = R.jma_frame("japan", SLOT, web)
    assert "rain pixels outside JMA's data area" in note


def test_a_frame_past_its_time_is_down(monkeypatch):
    t = iter([0.0] + [R.JMA_FRAME_S + 1.0] * 100)
    monkeypatch.setattr(R.time, "monotonic", lambda: next(t))
    with pytest.raises(R.Down):
        R.jma_frame("japan", SLOT, jma_web())


def test_the_stamp_is_the_slot_in_utc():
    web = jma_web()
    R.jma_frame("japan", datetime(2026, 11, 7, 4, 10, tzinfo=timezone.utc), web)
    assert all("/20261107041000/none/20261107041000/" in u for u in web.asked)


def test_nothing_of_a_partial_frame_is_stored():
    store = Store()
    web = Web([(r"hrpns_nd", OUTLINE), (r"/hrpns/6/58/28\.png", 404), (r"/hrpns/", DRY)])
    assert R.main(["--sources", "jma", "--slots", "1"], get=web, store=store, now=NOW) == 0
    assert not [k for k in store.objects if "/japan/" in k]


# ---------------------------------------------------------------- CWA
def cwa_body(values=None, when="2026-11-07T12:00:00+08:00", head=None):
    if values is None:
        values = np.full((cwa.ROWS, cwa.COLS), -999.0)
    text = ",".join("%.3E" % v for v in np.asarray(values).ravel()).encode()
    h = head if head is not None else CWA_HEAD
    h = re.sub(rb"<DateTime>[^<]*</DateTime>", b"<DateTime>" + when.encode() + b"</DateTime>", h)
    i = h.index(b"</content>")
    return h[:i] + text + h[i:]


CWA_SLOT = datetime(2026, 11, 7, 4, 0, tzinfo=timezone.utc)


def test_the_key_is_taiwan_time_across_midnight():
    assert cwa.url_of(datetime(2026, 11, 6, 16, 10, tzinfo=timezone.utc)).endswith("/202611070010compref_mosaic.xml")


def test_a_real_head_reads_with_row_zero_in_the_south():
    v = np.full((cwa.ROWS, cwa.COLS), -999.0)
    v[0, 0], v[880, 920] = 11.5, 42.0
    got = cwa.read(cwa_body(v))
    assert got["time"] == "202611070400"
    assert got["values"].shape == (881, 921)
    assert got["values"][0, 0] == 11.5 and got["values"][880, 920] == 42.0


@pytest.mark.parametrize("old,new", [
    (rb"<GridDimensionX>921<", b"<GridDimensionX>920<"), (rb"<GridDimensionY>881<", b"<GridDimensionY>880<"),
    (rb"<StartPointLongitude>115.0<", b"<StartPointLongitude>115.5<"), (rb"<StartPointLatitude>18.0<", b"<StartPointLatitude>17.0<"),
    (rb"<GridResolution>0.0125<", b"<GridResolution>0.01<"), (rb"<Reflectivity>dBZ<", b"<Reflectivity>mm<"),
])
def test_another_grid_or_unit_is_a_changed_file(old, new):
    assert re.search(old, CWA_HEAD)
    with pytest.raises(ValueError):
        cwa.read(cwa_body(head=re.sub(old, new, CWA_HEAD)))


def test_a_file_without_a_zone_or_a_time_is_refused():
    with pytest.raises(ValueError):
        cwa.read(cwa_body(when="2026-11-07T12:00:00"))
    with pytest.raises(ValueError):
        cwa.read(re.sub(rb"<DateTime>[^<]*</DateTime>", b"", cwa_body()))


def test_too_few_values_or_a_stray_word_is_refused():
    body = cwa_body()
    with pytest.raises(ValueError, match="values"):
        cwa.read(body.replace(b"-9.990E+02,", b"", 1))
    with pytest.raises(ValueError):
        cwa.read(body.replace(b"-9.990E+02,", b"oops,", 1))
    with pytest.raises(ValueError):
        cwa.read(b"<html>not the file</html>")


def test_a_frame_says_coverage_no_echo_and_echo():
    v = np.full((cwa.ROWS, cwa.COLS), -999.0)
    v[int((25.0 - 18) / .0125), int((121.5 - 115) / .0125)] = 33.0
    v[int((24.0 - 18) / .0125), int((120.0 - 115) / .0125)] = -99.0
    v[int((23.0 - 18) / .0125), int((120.0 - 115) / .0125)] = 2.5          # below the floor: covered, no echo
    web = Web([(r"202611071200compref", cwa_body(v))])
    codes = R.cwa_frame("taiwan", CWA_SLOT, web)
    s, w, n, e = R.box("taiwan")
    at = lambda lat, lon: codes[int((n - lat - 1e-6) * 100), int((lon - w + 1e-6) * 100)]   # noqa: E731
    assert codes.shape == ((n - s) * 100, (e - w) * 100)
    assert at(25.0, 121.5) == 2 + round((33.0 - 5) / .5)
    assert at(24.0, 120.0) == 1 and at(23.0, 120.0) == 1 and at(22.0, 120.0) == 0
    assert (codes >= 2).sum() in (1, 2, 4)                                 # one source node on 1.25 lattice pixels


def test_past_the_grid_east_of_126_5_is_no_coverage():
    v = np.full((cwa.ROWS, cwa.COLS), 40.0)
    codes = R.cwa_frame("taiwan", CWA_SLOT, Web([(r"compref", cwa_body(v))]))
    s, w, n, e = R.box("taiwan")
    assert (codes[:, :int((126.5 - w) * 100)] >= 2).all()
    assert (codes[:, int((126.51 - w) * 100):] == 0).all()


def test_every_source_point_reaches_the_lattice():
    s, w, n, e = R.box("taiwan")
    rows, cols = cwa.lattice_index(s, w, n, e, 100)
    assert set(rows[rows >= 0].tolist()) == set(range(cwa.ROWS)) or len(set(rows[rows >= 0].tolist())) >= cwa.ROWS - 1
    assert len(set(cols[cols >= 0].tolist())) >= cwa.COLS - 1


def test_lattice_pixels_read_the_nearest_source_point():
    s, w, n, e = R.box("taiwan")
    rows, cols = cwa.lattice_index(s, w, n, e, 100)
    lats = n - (np.arange((n - s) * 100) + 0.5) / 100
    lons = w + (np.arange((e - w) * 100) + 0.5) / 100
    for lat, got in zip(lats, rows):
        assert got == int(np.argmin(np.abs(18.0 + 0.0125 * np.arange(cwa.ROWS) - lat)))
    near = np.argmin(np.abs(115.0 + 0.0125 * np.arange(cwa.COLS)[None, :] - lons[:, None]), axis=1)
    inside = lons <= 115.0 + 0.0125 * (cwa.COLS - 1) + 0.00625
    assert (cols[inside] == near[inside]).all() and (cols[~inside] == -1).all()


def test_lattice_pixels_beyond_the_grid_are_marked():
    rows, cols = cwa.lattice_index(17, 114, 31, 128, 10)
    lat = 31 - (np.arange(140) + 0.5) / 10
    lon = 114 + (np.arange(140) + 0.5) / 10
    assert (rows[(lat < 17.99) | (lat > 29.01)] == -1).all() and (rows[(lat > 18.01) & (lat < 28.99)] >= 0).all()
    assert (cols[(lon < 114.99) | (lon > 126.51)] == -1).all() and (cols[(lon > 115.01) & (lon < 126.49)] >= 0).all()


def test_a_file_of_another_time_is_refused():
    web = Web([(r"compref", cwa_body(when="2026-11-07T11:50:00+08:00"))])
    with pytest.raises(ValueError, match="says"):
        R.cwa_frame("taiwan", CWA_SLOT, web)


def test_an_absent_key_is_missing():
    with pytest.raises(R.Missing):
        R.cwa_frame("taiwan", CWA_SLOT, Web([]))


def test_a_hung_read_is_down():
    with pytest.raises(R.Down):
        R.cwa_frame("taiwan", CWA_SLOT, Web([(r"compref", TimeoutError("slow"))]))


def test_the_read_limits_are_the_sources_own():
    seen = []

    def get(url, timeout=60):
        seen.append((url, timeout))
        return Web([(r"compref", cwa_body()), (r"hrpns_nd", OUTLINE), (r"/hrpns/", DRY)])(url)

    R.cwa_frame("taiwan", CWA_SLOT, get)
    R.jma_frame("japan", SLOT, get)
    assert {t for u, t in seen if "compref" in u} == {R.CWA_READ_S}
    assert {t for u, t in seen if "jmatile" in u} == {R.JMA_READ_S}


# ---------------------------------------------------------------- the table, the tiers, the pointer
def test_the_sources_run_after_stage_ones_with_their_own_clocks():
    assert R.SOURCES == ("mrms", "opera", "jma", "cwa")
    assert R.SOURCE["jma"]["ready_s"] == 90 and R.SOURCE["cwa"]["ready_s"] == 450
    assert R.SOURCE["jma"]["spend_s"] == 45 and R.SOURCE["jma"]["backfill_s"] == 20
    assert R.SOURCE["cwa"]["spend_s"] == 60 and R.SOURCE["cwa"]["backfill_s"] == 30


def test_taiwan_waits_for_its_file():
    names = [x[0] for x in R.plan(R.empty_pointer(), datetime(2026, 11, 7, 4, 6, tzinfo=timezone.utc), "cwa")]
    assert "202611070400" not in names and names[0] == "202611070350"
    names = [x[0] for x in R.plan(R.empty_pointer(), datetime(2026, 11, 7, 4, 8, tzinfo=timezone.utc), "cwa")]
    assert names[0] == "202611070400"


def test_the_pointer_says_japans_scale():
    block = R.regions_block()
    assert block["japan"]["scale"] == "jma8" and block["japan"]["src"] == "jma"
    assert "scale" not in block["taiwan"] and all("scale" not in v for k, v in block.items() if k != "japan")


def test_a_speck_stays_on_the_fine_tiers_only():
    s, w, n, e = R.box("japan")
    t2 = np.ones(((n - s) * 100, (e - w) * 100), dtype=np.uint8)
    t2[1000:1002, 1000:1003] = 49                                          # one 2-km source pixel: 6 lattice pixels
    objects, _flags, t0 = R.tiers_of(t2, "japan")
    assert (t0 >= 2).sum() == 0
    t2[1000:1006, 1000:1006] = 49                                          # a real shower: 36 pixels
    _o, _f, t0 = R.tiers_of(t2, "japan")
    assert (t0 >= 2).sum() >= 1
    assert R.COARSE_K_REGION["taiwan"] == {R.PPD_T1: 4, R.PPD_T0: 12}
    assert "conus" not in R.COARSE_K_REGION                                # stage one's frames do not move


def test_a_class_stays_a_class_through_the_tiers():
    s, w, n, e = R.box("japan")
    t2 = np.ones(((n - s) * 100, (e - w) * 100), dtype=np.uint8)
    t2[1000:1100, 1000:1100] = 49
    t2[1040:1060, 1040:1060] = 89
    objects, _f, t0 = R.tiers_of(t2, "japan")
    assert set(np.unique(t0).tolist()) <= {0, 1} | set(jma.CLASS_CODES)
    for (tier, _la, _lo), pic in objects:
        assert set(np.unique(pic).tolist()) <= {0, 1} | set(jma.CLASS_CODES), tier


def test_a_tick_publishes_both_regions_with_the_others_untouched():
    store = Store()
    v = np.full((cwa.ROWS, cwa.COLS), -99.0)
    v[500:520, 500:520] = 30.0
    web = Web([(r"compref", cwa_body(v, when=None or "2026-11-07T11:50:00+08:00")), (r"hrpns_nd", OUTLINE), (r"/hrpns/6/56/25\.png", RAIN), (r"/hrpns/", DRY)])
    now = datetime(2026, 11, 7, 4, 2, 0, tzinfo=timezone.utc)                 # slot 04:00 for JMA, 03:50 for Taiwan
    assert R.main(["--sources", "jma,cwa", "--slots", "1"], get=web, store=store, now=now) == 0
    p = store.get_json(POINTER)
    assert R.good_pointer(p)
    assert p["newest"] == {"jma": "202611070400", "cwa": "202611070350"}
    by = {s["t"]: s["r"] for s in p["slots"]}
    assert list(by["202611070400"]) == ["japan"] and list(by["202611070350"]) == ["taiwan"]
    assert p["began"]["japan"] == R.A.utc(now) and "t2" in by["202611070350"]["taiwan"]
    t0 = np.asarray(Image.open(io.BytesIO(store.objects["radar/v1/202611070400/japan/t0.png"])))
    assert t0.shape == (270, 270)


def test_a_failing_japan_leaves_taiwan_published():
    store = Store()
    web = Web([(r"compref", cwa_body(when="2026-11-07T11:50:00+08:00")), (r"hrpns_nd", b"not a file"), (r"/hrpns/", DRY)])
    now = datetime(2026, 11, 7, 4, 2, 0, tzinfo=timezone.utc)
    assert R.main(["--sources", "jma,cwa", "--slots", "1"], get=web, store=store, now=now) == 0
    p = store.get_json(POINTER)
    assert p["newest"] == {"cwa": "202611070350"}


def test_a_new_region_does_not_alarm_on_its_first_failures():
    store = Store()
    web = Web([])
    assert R.main(["--sources", "jma"], get=web, store=store, now=NOW) == 0
    assert store.get_json("radar/v1/stale-alarm.json") is None


def test_every_box_fits_the_live_clients_caps():
    src = open(os.path.join(ROOT, "static_overlay", "overlay.js"), encoding="utf-8").read()
    m = re.search(r"RADAR_REGION_AREA = (\d+), RADAR_TOTAL_AREA = (\d+)", src)
    region, total = int(m.group(1)), int(m.group(2))
    areas = {k: (v[5] - v[3]) * (v[6] - v[4]) for k, v in R.REGIONS.items()}
    assert max(areas.values()) <= region and sum(areas.values()) <= total
    for k, v in R.REGIONS.items():
        assert all(isinstance(x, int) for x in v[3:7]) and re.fullmatch(r"[a-z]{2,12}", k), k


def test_the_hosts_are_on_the_tls_check():
    src = open(os.path.join(ROOT, "tools", "tls_check.py"), encoding="utf-8").read()
    assert "www.jma.go.jp" in src and "cwaopendata.s3.ap-northeast-1.amazonaws.com" in src
