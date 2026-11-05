"""Gridded satellite wave height job (plan section 82): offline tests (no network, no R2; h5py writes the L4 file in
the layout verified on the real file of 2026-11-03).

A fake STAC / CloudFerro listing / file host (`FakeWeb` of test_alt.py) and a fake bucket (`FakeClient` of
test_job.py) drive altgrid.main end to end: the reader's checks, the pole padding onto the model's half grid, the
frames / stats / manifest / pointer order, the heartbeat, a re-produced day, prune, the stale alarm, the failure
back-off, --local / --file, and the workflow pins.
"""
import io
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import alt as A             # noqa: E402
import altgrid as G         # noqa: E402
import encode as E          # noqa: E402
import publish as P         # noqa: E402
from test_alt import FakeWeb, listing_xml   # noqa: E402
from test_job import FakeClient   # noqa: E402

h5py = pytest.importorskip("h5py")
from PIL import Image       # noqa: E402

NOW = datetime(2026, 11, 5, 8, 47, tzinfo=timezone.utc)
DID = "cmems_obs-wave_glo_phy-swh_nrt_multi-l4-0.5deg_PT6H-i_202607"
DID2 = "cmems_obs-wave_glo_phy-swh_nrt_multi-l4-2deg_P1D-m_202411"
NATIVE = f"{A.S3}/{A.BUCKET}/native/{G.STAC_ID}/{DID}"
LAT = -89.5 + 0.5 * np.arange(359)
LON = -180.0 + 0.5 * np.arange(720)


def l4_name(day, prod):
    return f"cmems_obs-wave_glo_phy-swh_nrt_multi_l4_0.5deg_PT6H-i_{day}T120000Z_P{prod}Z.nc"


def write_l4(path, day, values=None, flags=None, scale=0.001, fill=-32767, tunits="days since 1950-01-01",
             lat=None, lon=None, hours=(0, 6, 12, 18), lat_units="degrees_north"):
    """An L4 file in the real layout: float32 days since 1950, ascending latitude, VAVH_INST int16 x scale with the
    fill, VAVH_INST_FLAG int64. `values` (4, len(lat), len(lon)) in metres, NaN = the fill."""
    lat = LAT if lat is None else np.asarray(lat, np.float64)
    lon = LON if lon is None else np.asarray(lon, np.float64)
    d0 = datetime.strptime(day, "%Y%m%d").replace(tzinfo=timezone.utc)
    t = np.array([((d0 + timedelta(hours=h)) - G.EPOCH_1950).total_seconds() / 86400 for h in hours], np.float32)
    if values is None:
        values = np.full((len(hours), len(lat), len(lon)), np.nan)
        values[:, 100:200, 100:300] = 2.5
    raw = np.where(np.isnan(values), fill, np.rint(np.nan_to_num(values) / scale)).astype(np.int16)
    with h5py.File(path, "w") as f:
        d = f.create_dataset("time", data=t); d.attrs["units"] = tunits
        d = f.create_dataset("latitude", data=lat.astype(np.float32)); d.attrs["units"] = lat_units
        d = f.create_dataset("longitude", data=lon.astype(np.float32)); d.attrs["units"] = "degrees_east"
        d = f.create_dataset("VAVH_INST", data=raw)
        d.attrs["scale_factor"] = np.array([scale]); d.attrs["_FillValue"] = np.array([fill], np.int16); d.attrs["units"] = "m"
        f.create_dataset("VAVH_INST_FLAG", data=np.zeros(raw.shape, np.int64) if flags is None else np.asarray(flags, np.int64))
    return path


def web_for(tmp_path, files, stac=True):
    """A fake web serving the STAC collection, the listing of this month and the last, and the files
    {(day, prod): path}."""
    web = FakeWeb()
    base = f"{A.STAC}/{G.STAC_ID}"
    if stac:
        web.urls[f"{base}/product.stac.json"] = {"links": [{"rel": "item", "href": f"{DID}/dataset.stac.json"}, {"rel": "item", "href": f"{DID2}/dataset.stac.json"}]}
        web.urls[f"{base}/{DID}/dataset.stac.json"] = {"id": DID, "assets": {"native": {"href": NATIVE}}}
        web.urls[f"{base}/{DID2}/dataset.stac.json"] = {"id": DID2, "assets": {"native": {"href": NATIVE.replace(DID, DID2)}}}
    set_listing(web, files)
    return web


def set_listing(web, files):
    entries = {}
    for (day, prod), path in files.items():
        key = f"native/{G.STAC_ID}/{DID}/{day[:4]}/{day[4:6]}/{l4_name(day, prod)}"
        web.urls[f"{A.S3}/{A.BUCKET}/{key}"] = path
        entries.setdefault((day[:4], day[4:6]), []).append((key, os.path.getsize(path)))
    for month in A.months_touching(NOW - timedelta(days=40), NOW + timedelta(days=5)):
        web.urls[f"{A.S3}/{A.BUCKET}/?list-type=2&prefix=native/{G.STAC_ID}/{DID}/{month:%Y}/{month:%m}/&max-keys=1000"] = \
            listing_xml(entries.get((f"{month:%Y}", f"{month:%m}"), []))


def big(tmp_path, day, prod, **kw):
    """A file over MIN_FILE_BYTES (the listing refuses a tiny product file): noise keeps the deflate honest."""
    rng = np.random.default_rng(int(day) % 997)
    v = rng.uniform(0.5, 8.0, size=(4, 359, 720))
    v[:, :20, :] = np.nan
    return write_l4(str(tmp_path / f"{day}-{prod}.nc"), day, values=v, **kw)


def puts(fc, prefix="altgrid/v1/"):
    return [k for op, k in fc.log if op == "put" and k.startswith(prefix)]


def dq(png_bytes):
    return np.asarray(Image.open(io.BytesIO(png_bytes)))


# ------------------------------------------------------------------ names
def test_parse_file_and_the_dataset_pattern_take_the_half_degree_6_hourly_product_only():
    day, prod_dt, prod = G.parse_file(l4_name("20261103", "20261104T073010"))
    assert day == "20261103" and prod == "20261104T073010Z" and prod_dt == datetime(2026, 11, 4, 7, 30, 10, tzinfo=timezone.utc)
    assert G.parse_file(l4_name("20261103", "20261104T073010") + ".part") is None
    assert G.parse_file("cmems_obs-wave_glo_phy-swh_nrt_multi_l4_2deg_P1D-m_20261103T000000Z_P20261104T073010Z.nc") is None
    assert G.parse_file(l4_name("20261399", "20261104T073010")) is None
    assert A._dataset_token(G.PRODUCT, DID) == "multi" and A._dataset_token(G.PRODUCT, DID2) is None, "the 2-degree daily dataset is left out"
    tok, t0, t1, prod_dt = G.PRODUCT.parse_file(l4_name("20261103", "20261104T073010"), "multi")
    assert (tok, t0, t1 - t0) == ("l4", datetime(2026, 11, 3, tzinfo=timezone.utc), timedelta(hours=24))
    assert G.frame_key("20261103", "20261104T073010Z", 6) == "altgrid/v1/20261103/20261104T073010Z/hs-06.png"
    assert G.manifest_key("20261103", "2026-11-05T08:47:00Z") == "altgrid/v1/20261103/manifest-20261105T084700Z.json"


# ------------------------------------------------------------------ the reader
def test_read_l4_checks_the_axes_scale_fill_units_and_steps_and_returns_north_up_steps_flags_as_absent(tmp_path):
    p = str(tmp_path / "a.nc")
    v = np.full((4, 359, 720), np.nan)
    j, i = int(np.where(np.isclose(LAT, 10.0))[0][0]), int(np.where(np.isclose(LON, -150.0))[0][0])
    v[:, j, i] = [1.0, 2.0, 3.0, 4.0]
    v[0, 5, 5], v[0, 6, 6] = 31.0, 0.0                                   # out of range -> absent (counted); 0 m is a value
    flags = np.zeros((4, 359, 720)); v[1, 7, 7] = 0.0; flags[1, 7, 7] = 1  # invalid, thresholded to 0: absent
    write_l4(p, "20261103", values=v, flags=flags)
    d = G.read_l4(p, "20261103")
    assert [x.hour for x in d["valid"]] == [0, 6, 12, 18] and d["valid"][0] == datetime(2026, 11, 3, tzinfo=timezone.utc), "the float32 axis rounds to the hour"
    assert d["hs"].shape == (4, 359, 720) and d["hs"].dtype == np.float32
    row = 358 - j                                                      # north-up
    assert d["hs"][:, row, i].tolist() == pytest.approx([1.0, 2.0, 3.0, 4.0])
    assert np.isnan(d["hs"][0, 358 - 5, 5]) and d["hs"][0, 358 - 6, 6] == 0.0 and np.isnan(d["hs"][1, 358 - 7, 7])
    assert (d["dropped"], d["flagged"]) == (1, 1)
    for kw, msg in ((dict(scale=0.01), "scale_factor"), (dict(tunits="hours since 1950-01-01"), "days since"),
                    (dict(lat=LAT[:-1]), "latitude"), (dict(lat=LAT[::-1]), "latitude"), (dict(hours=(0, 3, 12, 18)), "00 / 06"),
                    (dict(hours=(0, 6, 12)), "time steps"), (dict(lat_units="degrees"), "degrees_north"), (dict(fill=-9999), "_FillValue")):
        vv = np.full((len(kw.get("hours", (0, 6, 12, 18))), len(kw.get("lat", LAT)), 720), np.nan)
        write_l4(p, "20261103", values=vv, **kw)
        with pytest.raises(ValueError, match=msg):
            G.read_l4(p, "20261103")
    write_l4(p, "20261103")
    with pytest.raises(ValueError, match="00 / 06"):
        G.read_l4(p, "20261104"), "the steps must be the named day's"


def test_place_half_puts_row_r_at_latitude_90_minus_half_r_with_absent_poles_on_the_model_half_grid():
    import run as R
    assert G.GRID == {k: v for k, v in R.GRID_HALF.items() if k != "derivation"}, "the client draws it as the model's half grid"
    step = np.full((359, 720), np.nan)
    step[0, 0], step[358, 719], step[100, 360] = 1.0, 2.0, 25.0          # north-up row 0 = 89.5 N
    g = G.place_half(step)
    assert g.shape == (361, 720) and np.isnan(g[0]).all() and np.isnan(g[360]).all()
    assert g[1, 0] == 1.0 and g[359, 719] == 2.0 and 90 - 0.5 * 1 == 89.5 and 90 - 0.5 * 359 == -89.5
    png, st = G.encode_step(g)
    q = dq(png)
    assert q.shape == (361, 720) and q[0].max() == 0 and q[360].max() == 0
    assert q[101, 360] == 255 and st["clamped_high"] == 1, "above the scale's top: clamped, counted"
    v = E.dequantize(q[1:2, 0:1], E.FIELDS["hs"]["lo"], E.FIELDS["hs"]["hi"])[0, 0]
    assert abs(v - 1.0) <= (E.FIELDS["hs"]["hi"] - E.FIELDS["hs"]["lo"]) / 254 / 2 + 1e-9
    assert st["filled_points"] == 0, "no coastal fill"
    with pytest.raises(ValueError):
        G.place_half(np.zeros((361, 720)))


# ------------------------------------------------------------------ main
def test_main_publishes_four_steps_the_stats_the_manifest_then_the_pointer_last(tmp_path):
    p = big(tmp_path, "20261103", "20261104T073010")
    web = web_for(tmp_path, {("20261103", "20261104T073010"): p})
    fc = FakeClient(); store = P.Store(fc, "b")
    assert G.main([], get=web, store=store, now=NOW) == 0
    ks = puts(fc)
    assert ks[-1] == "altgrid/v1/latest.json" and ks[:4] == [G.frame_key("20261103", "20261104T073010Z", h) for h in (0, 6, 12, 18)]
    assert re.fullmatch(r"altgrid/v1/20261103/stats-\d{8}T\d{6}Z\.json", ks[4]) and re.fullmatch(r"altgrid/v1/20261103/manifest-\d{8}T\d{6}Z\.json", ks[5])
    assert all(fc.objects[k]["cc"] == P.IMMUTABLE for k in ks[:-1]) and fc.objects[ks[-1]]["cc"] == P.POINTER
    ptr = json.loads(fc.objects["altgrid/v1/latest.json"]["body"])
    assert {k: ptr[k] for k in ("format", "day", "run", "complete", "newest_utc", "production", "production_utc")} == {
        "format": "altgrid-v1", "day": "2026-11-03", "run": "20261103", "complete": True, "newest_utc": "2026-11-03T18:00:00Z",
        "production": "20261104T073010Z", "production_utc": "2026-11-04T07:30:10Z"}
    assert ptr["manifest"] == ks[5] and DID in ptr["datasets"] and DID2 not in ptr["datasets"] and "moi-00180" in ptr["credit"]
    m = json.loads(fc.objects[ptr["manifest"]]["body"])
    assert m["grid"] == G.GRID and m["encoding"] == E.ENCODING and m["fields"]["hs"]["hi"] == E.FIELDS["hs"]["hi"] and m["fill"] is None
    assert [f["step"] for f in m["frames"]] == [0, 6, 12, 18] and m["frames"][3]["valid_utc"] == "2026-11-03T18:00:00Z"
    assert all(f["valid_cells"] == 339 * 720 for f in m["frames"]) and m["stats"] == ks[4] and m["source"]["doi"] == "10.48670/moi-00180"
    q = dq(fc.objects[m["frames"][0]["file"]]["body"])
    assert q.shape == (361, 720) and q[340:361].max() == 0 and q[1:340].min() >= 1 and q[0].max() == 0, \
        "the file's first 20 rows (the southernmost: latitude ascends) land north-up at rows 340-359, absent; the poles absent"
    assert not [u for u in web.log if DID2 in u and "list-type" in u], "the 2-degree dataset is never listed"


def test_main_heartbeats_rebuilds_a_re_produced_day_keeps_the_old_production_for_the_grace_then_prunes_it(tmp_path):
    p1 = big(tmp_path, "20261103", "20261104T073010")
    web = web_for(tmp_path, {("20261103", "20261104T073010"): p1})
    fc = FakeClient(); store = P.Store(fc, "b")
    assert G.main([], get=web, store=store, now=NOW) == 0
    n = len(puts(fc))
    assert G.main([], get=web, store=store, now=NOW + timedelta(minutes=30)) == 0
    assert puts(fc)[n:] == ["altgrid/v1/latest.json"], "nothing newer: the pointer's checked_utc only"
    ptr = json.loads(fc.objects["altgrid/v1/latest.json"]["body"])
    assert ptr["checked_utc"] == A.utc(NOW + timedelta(minutes=30)) and ptr["published_utc"] == A.utc(NOW)
    # the day re-produced at 09:10: new keys, the pointer flips, the old production kept through the grace
    p2 = big(tmp_path, "20261103", "20261104T091000")
    set_listing(web, {("20261103", "20261104T091000"): p2})
    t2 = NOW + timedelta(hours=1)
    assert G.main([], get=web, store=store, now=t2) == 0
    ptr = json.loads(fc.objects["altgrid/v1/latest.json"]["body"])
    assert ptr["production"] == "20261104T091000Z" and G.frame_key("20261103", "20261104T073010Z", 0) in fc.objects
    assert G.main([], get=web, store=store, now=t2 + timedelta(hours=3)) == 0
    left = [k for k in fc.objects if k.startswith("altgrid/v1/20261103/")]
    assert not [k for k in left if "20261104T073010Z" in k] and len([k for k in left if k.endswith(".json")]) == 2, \
        "after 2 h the superseded production and its manifest / stats go; the live ones stay"
    # an older day listed again never regresses the pointer
    p0 = big(tmp_path, "20261102", "20261104T120000")
    set_listing(web, {("20261102", "20261104T120000"): p0, ("20261103", "20261104T091000"): p2})
    assert G.main([], get=web, store=store, now=t2 + timedelta(hours=4)) == 0
    assert json.loads(fc.objects["altgrid/v1/latest.json"]["body"])["run"] == "20261103"


def test_prune_keeps_three_complete_days_the_live_one_and_in_progress_days_and_never_touches_other_prefixes():
    fc = FakeClient(); store = P.Store(fc, "b")
    for d in ("20261029", "20261030", "20261031", "20261101", "20261102"):
        fc.objects[f"altgrid/v1/{d}/manifest-x.json"] = {"body": b"{}", "ct": "", "cc": ""}
        fc.objects[f"altgrid/v1/{d}/P/hs-00.png"] = {"body": b"", "ct": "", "cc": ""}
    fc.objects["altgrid/v1/20261103/P/hs-00.png"] = {"body": b"", "ct": "", "cc": ""}     # a build in progress
    fc.objects["altgrid/v1/failed/20261028.json"] = {"body": b"{}", "ct": "", "cc": ""}
    fc.objects["altgrid/v1/failed/20261102.json"] = {"body": b"{}", "ct": "", "cc": ""}
    fc.objects["sst/v1/20261029/x"] = {"body": b"", "ct": "", "cc": ""}
    G.prune(store, {"run": "20261029", "published_utc": A.utc(NOW)}, keep=3, now=NOW)
    days = sorted({k.split("/")[2] for k in fc.objects if k.startswith("altgrid/v1/2")})
    assert days == ["20261029", "20261031", "20261101", "20261102", "20261103"], "3 newest complete + the live + in progress"
    assert "altgrid/v1/failed/20261028.json" not in fc.objects and "altgrid/v1/failed/20261102.json" in fc.objects
    assert "sst/v1/20261029/x" in fc.objects
    with pytest.raises(ValueError):
        G.prune(store, {}, keep=0)


def test_the_stale_alarm_fires_once_per_12_h_on_scheduled_runs_only(tmp_path):
    p = big(tmp_path, "20261103", "20261104T073010")
    web = web_for(tmp_path, {("20261103", "20261104T073010"): p})
    fc = FakeClient(); store = P.Store(fc, "b")
    assert G.main([], get=web, store=store, now=NOW) == 0, "18Z of Nov 3 is 14.8 h old"
    later = datetime(2026, 11, 5, 18, 30, tzinfo=timezone.utc)               # 48.5 h after the newest step, nothing newer
    assert G.main([], get=web, store=store, now=later) == 1 and "altgrid/v1/stale-alarm.json" in fc.objects
    assert G.main([], get=web, store=store, now=later + timedelta(hours=1)) == 0, "once per 12 h"
    assert G.main(["--day", "20261103"], get=web, store=store, now=later + timedelta(hours=2)) == 0, "a manual run never alarms"
    assert G.main([], get=web, store=store, now=later + timedelta(hours=13)) == 1
    assert (G.STALE_H, G.PRODUCT.stale_h, G.KEEP_DAYS, G.SUPERSEDED_H) == (48, 48, 3, 2)


def test_failures_are_recorded_and_backed_off_and_dry_run_local_and_file_builds_touch_no_bucket(tmp_path):
    p = big(tmp_path, "20261103", "20261104T073010")
    web = web_for(tmp_path, {("20261103", "20261104T073010"): p})
    url = f"{A.S3}/{A.BUCKET}/native/{G.STAC_ID}/{DID}/2026/11/{l4_name('20261103', '20261104T073010')}"
    fc = FakeClient(); store = P.Store(fc, "b")
    bad = lambda path, day: (_ for _ in ()).throw(ValueError("VAVH_INST: units 'cm' are not 'm'"))   # noqa: E731
    for k in range(3):
        assert G.main([], get=web, store=store, reader=bad, now=NOW + timedelta(minutes=k)) == 1
    rec = json.loads(fc.objects["altgrid/v1/failed/20261103.json"]["body"])
    assert rec["attempts"] == 3 and "units" in rec["last_error"] and "altgrid/v1/latest.json" not in fc.objects
    n = len(web.log)
    assert G.main([], get=web, store=store, reader=bad, now=NOW + timedelta(minutes=30)) == 1, "backed off: the stale check (nothing published)"
    assert url not in web.log[n:], "no download while backed off"
    assert G.main([], get=web, store=store, now=NOW + timedelta(hours=3)) == 0, "retried after 2 h"
    # a dry run and a local build write nothing to the bucket; --file builds a local file without the network
    fc2 = FakeClient(); s2 = P.Store(fc2, "b")
    assert G.main(["--dry-run"], get=web, store=s2, now=NOW) == 0 and not puts(fc2)
    out = tmp_path / "local"
    assert G.main(["--local", str(out)], get=web, store=s2, now=NOW) == 0 and not puts(fc2)
    assert (out / "altgrid" / "v1" / "latest.json").exists() and len(list((out / "altgrid" / "v1" / "20261103").rglob("hs-*.png"))) == 4
    named = tmp_path / "named"; named.mkdir()
    fp = named / l4_name("20261103", "20261104T073010")
    fp.write_bytes(open(p, "rb").read())
    n = len(web.log)
    assert G.main(["--file", str(fp), "--local", str(tmp_path / "local2")], get=web, now=NOW) == 0 and len(web.log) == n
    assert G.main(["--file", str(fp), "--day", "20261102", "--local", str(tmp_path / "x")], get=web, now=NOW) == 2
    assert G.main(["--file", str(tmp_path / "nope.nc")], get=web, now=NOW) == 2
    assert G.main(["--day", "2026-11-03"], get=web, store=s2, now=NOW) == 2


# ------------------------------------------------------------------ the workflow and the pins
def test_the_workflow_is_pinned_scheduled_kept_alive_and_not_dispatched_by_the_trigger():
    wf = os.path.join(ROOT, ".github", "workflows")
    text = open(os.path.join(wf, "model-altgrid.yml"), encoding="utf-8").read()
    assert "python tools/model_frames/altgrid.py" in text and "group: model-altgrid" in text and "timeout-minutes: 30" in text
    assert 'cron: "17,47 7-10 * * *"' in text and 'cron: "17 */3 * * *"' in text
    uses = re.findall(r"uses:[ ]*([^ \n]+)", text)
    assert uses and all(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40}", u) for u in uses)
    pkgs = text.split("create-args: >-", 1)[1].split("cache-environment", 1)[0].split()
    assert all(re.fullmatch(r"[a-z0-9-]+=[0-9][0-9A-Za-z.]*", p) for p in pkgs) and any(p.startswith("h5py=") for p in pkgs)
    assert "eccodes" not in text and "dry_run" in text and "force" in text and "day" in text
    keep = open(os.path.join(wf, "model-frames-keepalive.yml"), encoding="utf-8").read()
    assert "gh workflow enable model-altgrid.yml" in keep
    tests = open(os.path.join(wf, "model-frames-tests.yml"), encoding="utf-8").read()
    assert tests.count('".github/workflows/model-altgrid.yml"') == 2
    cfg = open(os.path.join(ROOT, "tools", "model_frames", "trigger", "wrangler.jsonc"), encoding="utf-8").read()
    assert "model-altgrid.yml" not in cfg, "a daily product: the cron suffices (the SST rule)"
