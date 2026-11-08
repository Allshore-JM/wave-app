import re
"""Rain, mix and snow in the radar job (plan section 89): NOAA's PrecipFlag file of the reflectivity's own minute says snow or a rain
class, NOAA's model wet-bulb temperature marks the estimated mix, and the codes keep the type through every tier. The real files are a
1 x 2 degree cut of CONUS of 2026-11-08 18:52 UTC (tests/capture_radar_types_fixtures.py); their expected values come from eccodes and
the expected codes from a rule written apart from radar.py. Nothing reaches the network."""
import gzip
import hashlib
import io
import json
import os
import struct
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest
from PIL import Image

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mrms        # noqa: E402
import radar as R  # noqa: E402
from test_radar import FakeStore, Web, e1_frame, grib, tick  # noqa: E402

FIX = os.path.join(ROOT, "tests", "fixtures", "radar")
EXP = json.load(open(os.path.join(FIX, "types_expect.json"), encoding="utf-8"))
NOW = datetime(2026, 11, 6, 12, 6, 0, tzinfo=timezone.utc)
SLOT = datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc)
CROP = ("Crop", "mrms", "CONUS", 49, -124, 50, -122, 1)


# ---------------------------------------------------------------- helpers
def real(name, identity, bits):
    return mrms.read(gzip.open(os.path.join(FIX, f"types_{name}.grib2.gz")).read(), identity, bits)


def real_trio():
    return (real("hsr", mrms.IDENTITY, (16,)), real("flag", mrms.FLAG_IDENTITY, (8, 16)),
            real("tw", mrms.TW_IDENTITY, (8, 16, 24)))


def packed(x, lat1, lon1, step, ident, bits, ref, dscale, png_bits=None):
    """A one-field GRIB2 message from packed values x (the 8-, 16- or 24-bit PNG NOAA writes), value = (ref + x) / 10^dscale."""
    x = np.asarray(x, dtype=np.int64)
    nj, ni = x.shape
    pb = png_bits or bits
    if pb == 8:
        im = Image.fromarray(x.astype(np.uint8))
    elif pb == 16:
        im = Image.fromarray(x.astype(np.uint16))
    else:
        im = Image.fromarray(np.stack([(x >> 16) & 255, (x >> 8) & 255, x & 255], -1).astype(np.uint8))
    buf = io.BytesIO()
    im.save(buf, "PNG")
    pic = buf.getvalue()
    sec1 = struct.pack(">IB", 21, 1) + bytes(16)
    s3 = bytearray(72)
    s3[:5] = struct.pack(">IB", 72, 3)
    s3[30:38] = struct.pack(">II", ni, nj)
    s3[46:54] = struct.pack(">iI", round(lat1 * 1e6), round(((lon1 + 360) % 360) * 1e6))
    s3[63:71] = struct.pack(">II", round(step * 1e6), round(step * 1e6))
    s4 = bytearray(34)
    s4[:5] = struct.pack(">IB", 34, 4)
    s4[9], s4[10] = ident[1], ident[2]
    s5 = bytearray(21)
    s5[:5] = struct.pack(">IB", 21, 5)
    s5[5:9] = struct.pack(">I", nj * ni)
    s5[9:11] = struct.pack(">H", 41)
    s5[11:15] = struct.pack(">f", float(ref))
    s5[15:19] = struct.pack(">HH", 0, dscale)
    s5[19] = bits
    s7 = struct.pack(">IB", 5 + len(pic), 7) + pic
    body = sec1 + bytes(s3) + bytes(s4) + bytes(s5) + struct.pack(">IBB", 6, 6, 255) + s7 + b"7777"
    return b"GRIB" + bytes([0, 0, ident[0], 2]) + struct.pack(">Q", 16 + len(body)) + body


def flag_grib(flags, lat1=21.995, lon1=-159.995, step=0.01):
    return packed(np.asarray(flags) + 3, lat1, lon1, step, mrms.FLAG_IDENTITY, 8, -3, 0)


def tw_grib(tw, lat1=21.995, lon1=-159.995, step=0.01, bits=24):
    """The wet-bulb as NOAA writes it (0.01 C; -999 = no value): 24-bit by default, 16 / 8 bits with a smaller range."""
    tw = np.asarray(tw, dtype=np.float64)
    if bits == 24:
        return packed(np.round(tw * 100) + 99900, lat1, lon1, step, mrms.TW_IDENTITY, 24, -99900, 2)
    if bits == 16:
        return packed(np.round(tw * 100) + 1000, lat1, lon1, step, mrms.TW_IDENTITY, 16, -1000, 2)
    return packed(np.round(tw * 10) + 100, lat1, lon1, step, mrms.TW_IDENTITY, 8, -100, 1)


@pytest.fixture
def small(monkeypatch):
    """One MRMS region (200 x 400 at 0.01 degrees) and one European one, both small."""
    monkeypatch.setattr(R, "REGIONS", {"r1": ("One", "mrms", "D1", 20, -160, 22, -156, 1),
                                       "e1": ("Eur", "opera", None, 50, 0, 52, 4, 1)})
    monkeypatch.setattr(R.time, "sleep", lambda s: None)
    R._TW.clear()
    R._TW_ORDER.clear()
    yield
    R._TW.clear()
    R._TW_ORDER.clear()


def r1_values():
    v = np.full((200, 400), -99.0)
    v[10:60, 10:60] = 30.0
    v[150:, :] = -999.0
    return v


def r1_flags():
    f = np.zeros((200, 400), np.int64)
    f[10:60, 10:35] = 3            # snow
    f[10:60, 35:60] = 1            # warm stratiform rain
    f[150:, :] = -3
    return f


def r1_tw():
    t = np.full((200, 400), 5.0)
    t[10:60, 30:45] = 0.5          # in the band: the snow cols 30-34 and the rain cols 35-44 are mix
    t[150:, :] = -999.0
    return t


def gz(b):
    return gzip.compress(b, mtime=0)


def r1_files(when, flag=True, tw_hour="same", flags=None, tw=None, tw_kw=None):
    files = {mrms.url_of("D1", when): gz(grib(r1_values(), 21.995, -159.995, 0.01))}
    if flag:
        files[mrms.url_of("D1", when, mrms.FLAG_PRODUCT)] = gz(flag_grib(r1_flags() if flags is None else flags))
    if tw_hour is not None:
        hour = when.replace(minute=0, second=0) if tw_hour == "same" else tw_hour
        files[mrms.url_of("D1", hour, mrms.TW_PRODUCT)] = gz(tw_grib(r1_tw() if tw is None else tw, **(tw_kw or {})))
    return files


def counts(a):
    return (int(((a >= 2) & (a <= R.MAX_CODE)).sum()), int(((a >= R.SNOW_CODE0) & (a <= R.SNOW_MAX)).sum()),
            int(((a >= R.MIX_CODE0) & (a <= R.MIX_MAX)).sum()))


def code30(kind):
    if kind == "rain":
        return int(R.code_table([30.0])[0])
    c0, step, cmax = (R.SNOW_CODE0, R.SNOW_STEP, R.SNOW_MAX) if kind == "snow" else (R.MIX_CODE0, R.MIX_STEP, R.MIX_MAX)
    return int(R.typed_codes([30.0], c0, step, cmax)[0])


# ---------------------------------------------------------------- the real files
def test_the_real_files_read_as_eccodes_does():
    hsr, flag, tw = real_trio()
    assert (hsr["bits"], flag["bits"], tw["bits"]) == (16, 8, 24)
    assert mrms.same_grid(hsr, flag) and mrms.same_grid(hsr, tw)
    assert [hsr["nj"], hsr["ni"]] == EXP["shape"] and hsr["lat1"] == 49.995 and hsr["lon1"] == -123.995
    for name, msg, tol in (("hsr", hsr, 0.002), ("flag", flag, 0.0), ("tw", tw, 0.002)):
        v = mrms.values(msg)
        for r, c, want in EXP["samples"][name]:
            got = round(float(v[r, c])) if name == "flag" else float(v[r, c])
            assert abs(got - want) <= tol, (name, r, c, got, want)


def test_the_real_files_make_the_expected_codes(monkeypatch):
    monkeypatch.setitem(R.REGIONS, "crop", CROP)
    hsr, flag, tw = real_trio()
    t = mrms.values(tw)
    band = (t >= EXP["band"][0]) & (t <= EXP["band"][1])
    snow, mix, unknown = R.flag_types(hsr, flag, band)
    lat = R.mrms_lattice("crop", hsr, (snow, mix))
    want = np.array(Image.open(os.path.join(FIX, "types_expect.png")))
    assert unknown == 0
    assert np.array_equal(lat, want)
    assert hashlib.sha256(lat.tobytes()).hexdigest() == EXP["codes_sha256"]
    assert counts(lat) == (EXP["counts"]["rain"], EXP["counts"]["snow"], EXP["counts"]["mix"])
    assert min(counts(lat)) > 500                                      # all three types in the cut


def test_the_real_reflectivity_untyped_is_rain_only(monkeypatch):
    monkeypatch.setitem(R.REGIONS, "crop", CROP)
    hsr = real("hsr", mrms.IDENTITY, (16,))
    v = mrms.values(hsr).astype(np.float64)
    want = np.ones(v.shape, np.int64)
    want[v < -900] = 0
    e = v >= 5.0
    want[e] = np.clip(2 + np.round((v[e] - 5) / 0.5), 2, 142)
    lat = R.mrms_lattice("crop", hsr)
    assert np.array_equal(lat, want) and lat.max() <= R.MAX_CODE


def brute_kth(a, b, k):
    """Each block's k-th strongest echo by (0.1-dB strength, then code); else covered when any pixel is."""
    out = np.zeros((a.shape[0] // b, a.shape[1] // b), np.int64)
    for i in range(out.shape[0]):
        for j in range(out.shape[1]):
            blk = a[i * b:(i + 1) * b, j * b:(j + 1) * b].ravel()
            echo = sorted(((round((R.CODE_DBZ[c] - 5) * 10), int(c)) for c in blk if c >= 2), reverse=True)
            out[i, j] = echo[k - 1][1] if len(echo) >= k else min(int(blk.max()), 1)
    return out


@pytest.mark.parametrize("b,k", [(2, 1), (4, 2), (10, 3)])
def test_typed_pooling_on_the_real_lattice_equals_a_brute_force(b, k):
    want = np.array(Image.open(os.path.join(FIX, "types_expect.png")))
    got = R.pool_kth(want, b, k)
    assert np.array_equal(got, brute_kth(want, b, k))
    if k == 1:
        assert np.array_equal(R.pool_max(want, b), got)


# ---------------------------------------------------------------- the code layout
def test_the_code_layout():
    assert (R.SNOW_CODE0, R.SNOW_MAX, R.MIX_CODE0, R.MIX_MAX) == (143, 213, 214, 249)
    assert R.MAX_CODE == 142 and R.MAX_CODE < R.SNOW_CODE0 and R.SNOW_MAX < R.MIX_CODE0 and R.MIX_MAX < 256
    for c0, c1 in ((2, R.MAX_CODE), (R.SNOW_CODE0, R.SNOW_MAX), (R.MIX_CODE0, R.MIX_MAX)):
        assert R.CODE_DBZ[c0] == 5.0 and R.CODE_DBZ[c1] == 75.0
    assert np.isnan(R.CODE_DBZ[[0, 1]]).all() and np.isnan(R.CODE_DBZ[R.MIX_MAX + 1:]).all()
    assert R.type_codes() == {"snow": [143, 213, 1.0], "mix": [214, 249, 2.0]}
    assert R.empty_pointer()["type_codes"] == R.type_codes()


@pytest.mark.parametrize("dbz,snow,mix", [
    (4.99, 0, 0), (5.0, 143, 214), (5.4, 143, 214), (5.6, 144, 214), (7.2, 145, 215), (75.0, 213, 249), (90.0, 213, 249),
    (-99.0, 0, 0), (-999.0, 0, 0), (float("nan"), 0, 0),
])
def test_typed_codes_at_their_boundaries(dbz, snow, mix):
    assert R.typed_codes([dbz], R.SNOW_CODE0, R.SNOW_STEP, R.SNOW_MAX)[0] == snow
    assert R.typed_codes([dbz], R.MIX_CODE0, R.MIX_STEP, R.MIX_MAX)[0] == mix


def test_a_typed_code_reads_back_within_half_a_step():
    for c0, step, cmax in ((R.SNOW_CODE0, R.SNOW_STEP, R.SNOW_MAX), (R.MIX_CODE0, R.MIX_STEP, R.MIX_MAX)):
        v = np.arange(5.0, 75.0, 0.13)
        c = R.typed_codes(v, c0, step, cmax)
        assert np.all(np.abs(R.CODE_DBZ[c] - v) <= step / 2 + 1e-6)


def test_the_pooling_key():
    codes = np.arange(R.MIX_MAX + 1)
    assert np.array_equal(R.KEY[codes] & 255, codes)
    echo = ~np.isnan(R.CODE_DBZ)
    assert R.KEY[echo].min() > R.KEY[~echo].max()
    rain25, snow25, mix25 = 2 + 40, R.SNOW_CODE0 + 20, R.MIX_CODE0 + 10              # 25 dBZ each
    assert R.KEY[rain25] < R.KEY[snow25] < R.KEY[mix25] < R.KEY[rain25 + 1]          # a stronger echo beats any type
    assert R.STRONG[12] and not R.STRONG[11]                                          # rain 10 / 9.5 dBZ
    assert R.STRONG[R.SNOW_CODE0 + 5] and not R.STRONG[R.SNOW_CODE0 + 4]             # snow 10 / 9
    assert R.STRONG[R.MIX_CODE0 + 3] and not R.STRONG[R.MIX_CODE0 + 2]               # mix 11 / 9
    assert not R.STRONG[0] and not R.STRONG[1] and not R.STRONG[R.MIX_MAX + 1:].any()


def test_rain_only_pooling_is_unchanged():
    a = np.random.default_rng(89).integers(0, R.MAX_CODE + 1, (60, 80)).astype(np.uint8)
    assert np.array_equal(R.pool_max(a, 4), a.reshape(15, 4, 20, 4).max(axis=(1, 3)))
    for b, k in ((2, 2), (4, 3), (10, 3)):
        blocks = a.reshape(60 // b, b, 80 // b, b).transpose(0, 2, 1, 3).reshape(60 // b, 80 // b, b * b)
        kth = np.sort(blocks, axis=2)[:, :, b * b - k]
        want = np.where(kth >= 2, kth, np.minimum(blocks.max(axis=2), 1))
        assert np.array_equal(R.pool_kth(a, b, k), want)


def test_typed_pooling_keeps_the_type():
    snow30, rain20, mix25, snow25 = R.SNOW_CODE0 + 25, 2 + 30, R.MIX_CODE0 + 10, R.SNOW_CODE0 + 20
    a = np.array([[snow30, rain20, mix25, snow25],
                  [1, 0, 1, 1]], np.uint8)
    assert R.pool_max(a, 2).tolist() == [[snow30, mix25]]
    assert R.pool_kth(a, 2, 2).tolist() == [[rain20, snow25]]
    assert R.pool_kth(a, 2, 3).tolist() == [[1, 1]]
    assert R.inner_echo(np.pad(np.array([[snow25, R.MIX_CODE0 + 2], [rain20, 1]], np.uint8), 1)) == 2


# ---------------------------------------------------------------- the flag and the band
def test_flag_types_marks_snow_mix_and_unknown_values():
    hsr = mrms.read(grib(np.full((3, 4), 30.0), 21.0, -160.0, 0.01))
    flag = mrms.read(flag_grib([[-3, 0, 1, 3], [6, 7, 10, 91], [96, 2, 5, 3]], 21.0, -160.0, 0.01),
                     mrms.FLAG_IDENTITY, (8, 16))
    band = np.array([[True, True, True, False], [True, False, True, True], [True, True, True, True]])
    snow, mix, unknown = R.flag_types(hsr, flag, band)
    assert unknown == 2
    assert snow.tolist() == [[False, False, False, True], [False] * 4, [False, False, False, True]]
    assert mix.tolist() == [[False, False, True, False], [True, False, True, True], [True, False, False, True]]
    s2, m2, _ = R.flag_types(hsr, flag, None)
    assert np.array_equal(s2, snow) and not m2.any()
    with pytest.raises(ValueError):
        R.flag_types(hsr, flag, band[:, :3])
    other = mrms.read(flag_grib(np.zeros((3, 4), int), 21.01, -160.0, 0.01), mrms.FLAG_IDENTITY, (8, 16))
    with pytest.raises(ValueError):
        R.flag_types(hsr, other, band)


def test_mix_wins_over_snow_and_only_echo_takes_a_type(small):
    msg = mrms.read(grib(r1_values(), 21.995, -159.995, 0.01))
    snow = np.zeros((200, 400), bool)
    mix = np.zeros((200, 400), bool)
    snow[:, :35] = True
    mix[:, 30:45] = True
    lat = R.mrms_lattice("r1", msg, (snow, mix))
    assert (lat[10:60, 10:30] == code30("snow")).all() and (lat[10:60, 30:45] == code30("mix")).all()
    assert (lat[10:60, 45:60] == code30("rain")).all()
    assert (lat[60:150, :35] == 1).all() and (lat[150:, :35] == 0).all()             # no echo, no coverage: untouched
    with pytest.raises(ValueError):
        R.mrms_lattice("r1", msg, (snow[:, :399], mix[:, :399]))


@pytest.mark.parametrize("bits,values,want", [
    (24, [-999.0, -0.51, -0.5, 0.0, 1.5, 1.51, 5.0], [False, False, True, True, True, False, False]),
    (16, [-1.0, -0.51, -0.5, 0.7, 1.5, 1.51, 20.0], [False, False, True, True, True, False, False]),
    (8, [-1.0, -0.6, -0.5, 0.7, 1.5, 1.6, 9.0], [False, False, True, True, True, False, False]),
])
def test_the_band_is_inclusive_and_never_holds_the_missing_value(small, bits, values, want):
    hour = datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc)
    web = Web({mrms.url_of("D1", hour, mrms.TW_PRODUCT): gz(tw_grib([values], 22.0, -160.0, 0.01, bits=bits))})
    grid, band = R._read_tw("D1", hour, web)
    assert grid["ni"] == len(values) and band.tolist() == [want]
    assert R.TW_BAND == (-0.5, 1.5)


def test_a_wet_bulb_file_of_another_parameter_is_unreadable(small):
    hour = datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc)
    web = Web({mrms.url_of("D1", hour, mrms.TW_PRODUCT): gz(flag_grib([[0, 1]], 22.0, -160.0, 0.01))})
    got = R._read_tw("D1", hour, web)
    assert isinstance(got, str) and "cannot be read" in got


def test_the_reader_refuses_a_bit_depth_it_was_not_asked_for():
    raw24 = tw_grib([[0.0, 1.0]], 22.0, -160.0, 0.01)
    with pytest.raises(ValueError):
        mrms.read(raw24, mrms.TW_IDENTITY, (8, 16))
    with pytest.raises(ValueError):
        mrms.value_table(mrms.read(raw24, mrms.TW_IDENTITY, (24,)))
    raw8 = flag_grib([[0, 1]], 22.0, -160.0, 0.01)
    with pytest.raises(ValueError):
        mrms.read(raw8, mrms.FLAG_IDENTITY, (16,))
    with pytest.raises(ValueError):                                    # the header says 16 bits, the picture holds 8
        mrms.read(packed([[3, 4]], 22.0, -160.0, 0.01, mrms.FLAG_IDENTITY, 16, -3, 0, png_bits=8),
                  mrms.FLAG_IDENTITY, (8, 16))
    with pytest.raises(ValueError):                                    # the header says 24 bits, the picture holds 16
        mrms.read(packed([[3, 4]], 22.0, -160.0, 0.01, mrms.TW_IDENTITY, 24, -3, 0, png_bits=16),
                  mrms.TW_IDENTITY, (8, 16, 24))
    assert mrms.read(raw24, mrms.TW_IDENTITY, (8, 16, 24))["x"].dtype == np.uint32


def test_values_of_a_24_bit_message_are_exact():
    t = np.array([[-999.0, -12.34, 0.0, 0.01, 27.99]])
    v = mrms.values(mrms.read(tw_grib(t, 22.0, -160.0, 0.01), mrms.TW_IDENTITY, (24,)))
    assert np.abs(v.astype(np.float64) - t).max() < 1e-4


# ---------------------------------------------------------------- the wet-bulb hours
def tw_only(hours, domain="D1"):
    return {mrms.url_of(domain, h, mrms.TW_PRODUCT): gz(tw_grib(r1_tw())) for h in hours}


def tw_asks(web):
    return [u for u in web.asked if mrms.TW_PRODUCT in u]


def test_the_newest_wet_bulb_hour_within_three_hours(small):
    when = datetime(2026, 11, 6, 12, 34, tzinfo=timezone.utc)
    h11 = datetime(2026, 11, 6, 11, 0, tzinfo=timezone.utc)
    web = Web(tw_only([h11]))
    _grid, band, hour = R.tw_band("D1", when, web)
    assert hour == h11 and band.shape == (200, 400)
    assert web.asked == [mrms.url_of("D1", h, mrms.TW_PRODUCT) for h in (h11 + timedelta(hours=1), h11)]


def test_no_wet_bulb_hour_older_than_three_hours(small):
    when = datetime(2026, 11, 6, 12, 34, tzinfo=timezone.utc)
    web = Web(tw_only([datetime(2026, 11, 6, 9, 0, tzinfo=timezone.utc)]))
    grid, band, why = R.tw_band("D1", when, web)
    assert grid is None and band is None and "within 3 h" in why
    assert len(web.asked) == 3                                         # 12, 11 and 10 UTC; 9 UTC is 3:34 before


def test_guam_has_a_wet_bulb_file_every_six_hours(small):
    when = datetime(2026, 11, 6, 13, 20, tzinfo=timezone.utc)
    h6 = datetime(2026, 11, 6, 6, 0, tzinfo=timezone.utc)
    web = Web(tw_only([h6], "GUAM"))
    _g, _b, hour = R.tw_band("GUAM", when, web)
    assert hour == h6
    assert web.asked == [mrms.url_of("GUAM", h, mrms.TW_PRODUCT) for h in (h6 + timedelta(hours=6), h6)]
    R._TW.clear()
    R._TW_ORDER.clear()
    web = Web(tw_only([datetime(2026, 11, 6, 0, 0, tzinfo=timezone.utc)], "GUAM"))
    assert R.tw_band("GUAM", when, web)[0] is None                    # 00 UTC is 13:20 before: past 8 h


@pytest.mark.parametrize("trouble", ["down", "bad"])
def test_a_wet_bulb_that_fails_means_no_mix_and_is_not_asked_again(small, trouble):
    when = datetime(2026, 11, 6, 12, 34, tzinfo=timezone.utc)
    h12 = datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc)
    web = Web(tw_only([h12]), down=(mrms.TW_PRODUCT,) if trouble == "down" else (),
              bad=(mrms.TW_PRODUCT,) if trouble == "bad" else ())
    grid, _band, why = R.tw_band("D1", when, web)
    assert grid is None and ("did not answer" in why if trouble == "down" else "cannot be read" in why)
    asked = len(web.asked)
    assert R.tw_band("D1", when, web)[0] is None and len(web.asked) == asked


def test_three_hours_are_kept_and_a_fourth_drops_the_oldest(small):
    hours = [datetime(2026, 11, 6, h, 0, tzinfo=timezone.utc) for h in (8, 9, 10, 11)]
    web = Web(tw_only(hours))
    for h in hours:
        assert isinstance(R._tw_get("D1", h, web), tuple)
    assert R._TW[("D1", hours[0])] == "evicted" and len(R._TW_ORDER) == R.TW_CACHE == 3
    asked = len(web.asked)
    assert isinstance(R._tw_get("D1", hours[3], web), tuple) and len(web.asked) == asked         # still held
    assert isinstance(R._tw_get("D1", hours[0], web), tuple) and len(web.asked) == asked + 1     # read again
    assert R._TW[("D1", hours[1])] == "evicted"


def test_a_wet_bulb_hour_exactly_three_hours_old_is_used(small):
    when = datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc)          # frames come at whole minutes: 12:00 can meet 09 UTC
    h9 = datetime(2026, 11, 6, 9, 0, tzinfo=timezone.utc)
    web = Web(tw_only([h9]))
    assert R.tw_band("D1", when, web)[2] == h9


def test_a_hit_moves_the_hour_to_the_newest_end(small):
    hours = [datetime(2026, 11, 6, h, 0, tzinfo=timezone.utc) for h in (8, 9, 10, 11)]
    web = Web(tw_only(hours))
    for h in hours[:3]:
        R._tw_get("D1", h, web)
    R._tw_get("D1", hours[0], web)                                     # 08 used again: 09 is now the least recent
    R._tw_get("D1", hours[3], web)
    assert isinstance(R._TW[("D1", hours[0])], tuple) and R._TW[("D1", hours[1])] == "evicted"

def test_a_tick_starts_with_no_wet_bulb_held(small):
    R._TW[("D1", "sentinel")] = "missing"
    R._TW_ORDER.append(("D1", "sentinel"))
    R.main(["--dry-run", "--sources", "mrms", "--slots", "1"], get=Web(), store=FakeStore(), now=NOW)
    assert ("D1", "sentinel") not in R._TW and ("D1", "sentinel") not in R._TW_ORDER


# ---------------------------------------------------------------- a frame
def flag_asks(web):
    return [u for u in web.asked if mrms.FLAG_PRODUCT in u]


def test_a_frame_is_typed_by_the_flag_of_its_own_minute(small):
    web = Web(r1_files(SLOT + timedelta(minutes=4)))
    lat, off, info = R.mrms_frame("r1", SLOT, NOW, web)
    assert off == 4 and info["ty"] == "rms" and "wet-bulb 12Z" in info["note"]
    assert (lat[10:60, 10:30] == code30("snow")).all() and (lat[10:60, 30:45] == code30("mix")).all()
    assert (lat[10:60, 45:60] == code30("rain")).all()
    assert counts(lat) == (50 * 15, 50 * 20, 50 * 15)
    assert len([u for u in web.asked if mrms.PRODUCT in u]) == 1 and len(flag_asks(web)) == 1


def test_a_missing_flag_takes_the_older_pair(small):
    files = {**r1_files(SLOT + timedelta(minutes=2)), **r1_files(SLOT + timedelta(minutes=4), flag=False)}
    lat, off, info = R.mrms_frame("r1", SLOT, NOW, Web(files))
    assert off == 2 and info["ty"] == "rms" and counts(lat)[1] == 50 * 20


def test_no_flag_at_any_minute_keeps_the_newest_frame_as_rain(small):
    files = {**r1_files(SLOT + timedelta(minutes=2), flag=False), **r1_files(SLOT + timedelta(minutes=4), flag=False)}
    web = Web(files)
    lat, off, info = R.mrms_frame("r1", SLOT, NOW, web)
    assert off == 4 and info["ty"] is None and "no PrecipFlag" in info["note"]
    assert counts(lat) == (50 * 50, 0, 0)
    assert not tw_asks(web)


def test_an_unreadable_flag_leaves_the_frame_as_rain(small):
    web = Web(r1_files(SLOT + timedelta(minutes=4)), bad=(mrms.FLAG_PRODUCT,))
    lat, off, info = R.mrms_frame("r1", SLOT, NOW, web)
    assert off == 4 and info["ty"] is None and "cannot be read" in info["note"] and counts(lat) == (50 * 50, 0, 0)


def test_a_flag_on_another_grid_leaves_the_frame_as_rain(small):
    files = r1_files(SLOT + timedelta(minutes=4))
    files[mrms.url_of("D1", SLOT + timedelta(minutes=4), mrms.FLAG_PRODUCT)] = gz(flag_grib(r1_flags(), lat1=21.985))
    lat, _off, info = R.mrms_frame("r1", SLOT, NOW, Web(files))
    assert info["ty"] is None and counts(lat) == (50 * 50, 0, 0)


def test_a_flag_that_does_not_answer_is_down(small):
    web = Web(r1_files(SLOT + timedelta(minutes=4)), down=(mrms.FLAG_PRODUCT,))
    with pytest.raises(R.Down):
        R.mrms_frame("r1", SLOT, NOW, web)


def test_no_wet_bulb_hour_means_rain_and_snow_only(small):
    web = Web(r1_files(SLOT + timedelta(minutes=4), tw_hour=None))
    lat, _off, info = R.mrms_frame("r1", SLOT, NOW, web)
    assert info["ty"] == "rs" and "no mix" in info["note"]
    assert counts(lat) == (50 * 25, 50 * 25, 0)


def test_an_older_wet_bulb_hour_still_marks_the_mix(small):
    web = Web(r1_files(SLOT + timedelta(minutes=4), tw_hour=SLOT - timedelta(hours=2)))
    lat, _off, info = R.mrms_frame("r1", SLOT, NOW, web)
    assert info["ty"] == "rms" and "wet-bulb 10Z" in info["note"] and counts(lat)[2] == 50 * 15


def test_a_wet_bulb_on_another_grid_means_no_mix(small):
    files = r1_files(SLOT + timedelta(minutes=4), tw_kw={"lat1": 21.985})
    lat, _off, info = R.mrms_frame("r1", SLOT, NOW, Web(files))
    assert info["ty"] == "rs" and "another grid" in info["note"] and counts(lat)[2] == 0


def test_an_unknown_flag_value_is_drawn_as_rain_and_said(small):
    flags = r1_flags()
    flags[10:20, 10:60] = 2                                            # a value NOAA's table does not have
    lat, _off, info = R.mrms_frame("r1", SLOT, NOW, Web(r1_files(SLOT + timedelta(minutes=4), flags=flags)))
    assert "unknown flag value" in info["note"] and (lat[10:20, 10:60] == code30("rain")).all()


def test_the_same_wet_bulb_hour_serves_every_slot_of_a_tick(small):
    files = {}
    for m in (4, 14):
        files.update(r1_files(SLOT + timedelta(minutes=m)))
    web = Web(files)
    later = NOW + timedelta(minutes=10)
    R.mrms_frame("r1", SLOT, later, web)
    R.mrms_frame("r1", SLOT + timedelta(minutes=10), later, web)
    assert len(tw_asks(web)) == 1


# ---------------------------------------------------------------- the pointer and a whole tick
def test_the_pointer_says_which_regions_hold_types():
    block = R.regions_block()
    for k, v in R.REGIONS.items():
        assert (block[k].get("types") == "rms") == (v[1] == "mrms"), k
    assert {k for k, v in block.items() if "types" in v} == {"conus", "alaska", "hawaii", "carib", "guam"}


def decoded(store, name, region):
    out = []
    for k, body in store.objects.items():
        if k.startswith(f"radar/v1/{name}/{region}/") and k.endswith(".png"):
            out.append(np.array(Image.open(io.BytesIO(body))).ravel())
    return np.concatenate(out)


def slot_entry(store, name):
    return [x for x in store.get_json("radar/v1/latest.json")["slots"] if x["t"] == name][0]


def test_a_tick_writes_typed_frames_with_their_word(small, monkeypatch):
    e1_frame(monkeypatch, {R.slot_name(SLOT)})
    store, web = FakeStore(), Web(r1_files(SLOT + timedelta(minutes=4)))
    assert tick(store, web) == 0
    p = store.get_json("radar/v1/latest.json")
    name = R.slot_name(SLOT)
    s = slot_entry(store, name)
    assert s["r"]["r1"]["ty"] == "rms" and "ty" not in s["r"]["e1"]
    assert p["regions"]["r1"]["types"] == "rms" and "types" not in p["regions"]["e1"]
    assert p["type_codes"] == R.type_codes()
    allc = decoded(store, name, "r1")
    assert ((allc >= R.SNOW_CODE0) & (allc <= R.SNOW_MAX)).any() and ((allc >= R.MIX_CODE0) & (allc <= R.MIX_MAX)).any()
    assert ((allc >= 2) & (allc <= R.MAX_CODE)).any() and allc.max() <= R.MIX_MAX
    assert R.good_pointer(p)


def test_a_tick_without_a_flag_file_writes_an_untyped_entry(small, monkeypatch):
    e1_frame(monkeypatch, {R.slot_name(SLOT)})
    store = FakeStore()
    assert tick(store, Web(r1_files(SLOT + timedelta(minutes=4), flag=False))) == 0
    p = store.get_json("radar/v1/latest.json")
    s = slot_entry(store, R.slot_name(SLOT))
    assert "ty" not in s["r"]["r1"] and p["regions"]["r1"]["types"] == "rms"
    assert decoded(store, R.slot_name(SLOT), "r1").max() <= R.MAX_CODE


def test_a_tick_without_a_wet_bulb_hour_writes_rs(small, monkeypatch):
    e1_frame(monkeypatch, {R.slot_name(SLOT)})
    store = FakeStore()
    assert tick(store, Web(r1_files(SLOT + timedelta(minutes=4), tw_hour=None))) == 0
    assert slot_entry(store, R.slot_name(SLOT))["r"]["r1"]["ty"] == "rs"


def test_the_client_reads_the_bands_the_job_writes():
    """overlay.js RADAR_TYPE_CODES = R.type_codes(): a client that read other bands would refuse every type (radarTypeCodesOk)."""
    js = open(os.path.join(os.path.dirname(__file__), "..", "..", "static_overlay", "overlay.js"), encoding="utf-8").read()
    m = re.search(r"RADAR_TYPE_CODES = \{ snow: \[(\d+), (\d+), (\d+)\], mix: \[(\d+), (\d+), (\d+)\] \}", js)
    assert m, "RADAR_TYPE_CODES not found in overlay.js"
    v = [int(x) for x in m.groups()]
    assert {"snow": v[0:3], "mix": v[3:6]} == {k: [int(x) for x in b] for k, b in R.type_codes().items()}
    assert int(re.search(r"RADAR_TYPE_BASE = (\d+)", js).group(1)) == R.MAX_CODE + 1
