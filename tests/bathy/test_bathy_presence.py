"""Plan section 98 step 6: a lidar region reads only the blocks where the PUBLISHED base map has water (its `presence`).
Most of a lidar survey's footprint is land, so without this rule the mainland regions would read tens of thousands of
empty blocks. The presence comes from the bucket as the site reads it: regions.json -> each base region's index.json ->
its idx files (the builder's own writer, `presence`, makes them)."""
import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "bathy"))

import bathy_sources as S  # noqa: E402
import bathy_tiles as T  # noqa: E402
import build_bathy as B  # noqa: E402

ROOTURL = "https://bucket.test"


class Bucket:
    """A fake public bucket: published base regions written with the builder's own `presence` writer."""

    def __init__(self):
        self.files = {}
        self.asked = []
        self.regions = {}

    def publish(self, name, version, found, zmax, zooms=None):
        _, idx = B.presence(found, 8, zmax)
        base = "%s/%s/%s/v%d" % (ROOTURL, B.PREFIX, name, version)
        for (cx, cy), data in idx.items():
            self.files["%s/idx/%d_%d.bin" % (base, cx, cy)] = data
        index = {"zmax": zmax, "t10": sorted("%d_%d" % k for k in idx),
                 "idx": {"dir": "idx", "zooms": zooms or [11, zmax]} if idx else None}
        self.files[base + "/index.json"] = json.dumps(index).encode("utf-8")
        self.regions[name] = {"v": version, "zmax": zmax, "prio": 10}
        self.files["%s/%s" % (ROOTURL, B.REGIONS_KEY)] = json.dumps(
            {"format": "bathy-regions-v1", "regions": self.regions}).encode("utf-8")
        return base

    def get(self, url):
        self.asked.append(url)
        if url not in self.files:
            raise OSError("404 " + url)
        return self.files[url]


def _pp(bucket, names, z):
    return B.published_presence(names, z, fetch=bucket.get, root=ROOTURL, workers=2)


def test_idx_offsets_match_the_writer():
    assert B.idx_offsets(15) == {11: (0, 1), 12: (1, 2), 13: (3, 8), 14: (11, 32), 15: (43, 128)}
    offs = B.idx_offsets(17)
    assert offs[16] == (171, 512) and offs[17] == (683, 2048)
    # the writer's own files are exactly that long
    _, idx = B.presence({17: {(2 ** 17 // 2, 2 ** 17 // 2)}}, 8, 17)
    (data,) = idx.values()
    assert len(data) == sum(n for (_, n) in offs.values())


def test_presence_reads_the_published_tiles_at_the_asked_zoom_and_finer():
    b = Bucket()
    # zoom 14: two tiles in one zoom-10 cell; zoom 15: one tile whose parent is NOT among the zoom-14 tiles, and one in
    # another zoom-10 cell (a finer tile marks its parent)
    cx, cy = 100, 200
    z14 = {(cx * 16 + 3, cy * 16 + 5), (cx * 16 + 15, cy * 16 + 0)}
    z15 = {(cx * 32 + 0, cy * 32 + 31), ((cx + 1) * 32 + 7, cy * 32 + 9)}
    b.publish("base", 3, {14: z14, 15: z15}, 15)
    got = _pp(b, ["base"], 14)
    assert got == z14 | {(x >> 1, y >> 1) for (x, y) in z15}
    # the asked URLs: the pointer, the index of the published version, then each idx file of it
    assert b.asked[0] == "%s/static/bathy/regions.json" % ROOTURL
    assert b.asked[1] == "%s/static/bathy/base/v3/index.json" % ROOTURL
    assert sorted(b.asked[2:]) == sorted("%s/static/bathy/base/v3/idx/%d_%d.bin" % (ROOTURL, x, y)
                                         for (x, y) in ((cx, cy), (cx + 1, cy)))
    # a coarser zoom folds every finer tile onto its parent
    assert _pp(b, ["base"], 12) == {(x >> 2, y >> 2) for (x, y) in z14} | {(x >> 3, y >> 3) for (x, y) in z15}
    # the zoom itself only: a zoom-15 question sees the zoom-15 tiles alone
    assert _pp(b, ["base"], 15) == z15


def test_presence_joins_several_regions():
    b = Bucket()
    b.publish("a", 1, {14: {(10, 10)}}, 15)
    b.publish("b", 2, {15: {(4000, 4000)}}, 15)
    assert _pp(b, ["a", "b"], 14) == {(10, 10), (2000, 2000)}


def test_presence_refuses_what_it_cannot_trust():
    b = Bucket()
    b.publish("base", 1, {14: {(10, 10)}}, 15)
    with pytest.raises(ValueError, match="not published"):
        _pp(b, ["nope"], 14)
    with pytest.raises(ValueError, match="ends at zoom 15"):
        _pp(b, ["base"], 16)
    with pytest.raises(ValueError, match="11-20"):
        _pp(b, ["base"], 10)
    # an idx file of the wrong length
    key = next(k for k in b.files if k.endswith(".bin"))
    b.files[key] = b.files[key][:-1]
    with pytest.raises(ValueError, match="bytes, expected"):
        _pp(b, ["base"], 14)
    # an index whose idx files hold other zooms
    b2 = Bucket()
    b2.publish("base", 1, {14: {(10, 10)}}, 15, zooms=[12, 15])
    with pytest.raises(ValueError, match="no idx files"):
        _pp(b2, ["base"], 14)
    # an unreadable file is the fetch's error, never an empty set
    b3 = Bucket()
    b3.publish("base", 1, {14: {(10, 10)}}, 15)
    del b3.files[next(k for k in b3.files if k.endswith(".bin"))]
    with pytest.raises(OSError):
        _pp(b3, ["base"], 14)


def test_block_keys_keeps_only_present_footprint_blocks_then_grows_the_ring():
    box = (-158.5, 21.0, -157.5, 21.8)
    own = (-158.10, 21.30, -158.00, 21.40)
    reg = dict(zmax=17, box=box, srcs=[dict(fill=False, rects=[own]), dict(fill=True, rects=[box])])
    bz = 17 - B.BLOCK_LEVELS
    x0, y0, x1, y1 = T.tiles_in_box(own, bz)
    allk = {(x, y) for x in range(x0, x1 + 1) for y in range(y0, y1 + 1)}
    assert set(B.block_keys(reg)) == allk
    assert set(B.block_keys(reg, None)) == allk
    keep = {(x0, y0), (x1, y1), (10, 10)}                  # (10, 10): present but outside the footprint
    assert set(B.block_keys(reg, keep)) == {(x0, y0), (x1, y1)}
    reg["ring"] = 1
    grown = {(x + dx, y + dy) for (x, y) in {(x0, y0), (x1, y1)} for dx in (-1, 0, 1) for dy in (-1, 0, 1)}
    assert set(B.block_keys(reg, keep)) == grown           # the ring grows from the kept blocks, present or not
    assert B.block_keys(reg, set()) == []                  # nothing present: nothing read, ring or not


def test_region_config_checks_the_presence_list(monkeypatch):
    monkeypatch.setitem(S.SOURCES, "lo", dict(S.LIDAR, id=5, path="a.vrt", year="2099", name="x", credit="NOAA x",
                                             datum="t", nodata=-1.0))
    monkeypatch.setitem(S.SOURCES, "hi", dict(S.CUDEM, id=10, path="b.tif", year="2099"))
    monkeypatch.setitem(S.SOURCES, "coarse", dict(S.CUDEM, id=11, path="c.tif", year="2099", zmax=13))
    monkeypatch.setitem(S.REGIONS, "pbase", dict(box=(0, 0, 1, 1), sources=["hi"], mhhw=0.4, zmin=8, prio=10))
    monkeypatch.setitem(S.REGIONS, "pcoarse", dict(box=(0, 0, 1, 1), sources=["coarse"], mhhw=0.4, zmin=8, prio=10))
    base = dict(box=(0, 0, 1, 1), sources=["lo"], fill=["hi"], mhhw=0.4, zmin=16, prio=20, margin=72)
    for bad in ("pbase", [], ["nope"], ["plid"], ["pbase", "pbase"], ["pcoarse"]):
        monkeypatch.setitem(S.REGIONS, "plid", dict(base, presence=bad))
        with pytest.raises(ValueError):
            B.region_config("plid")
    monkeypatch.setitem(S.REGIONS, "plid", dict(base, presence=("pbase",)))
    assert B.region_config("plid")["presence"] == ["pbase"]
    monkeypatch.setitem(S.REGIONS, "plid", dict(base))
    assert B.region_config("plid").get("presence") is None


def test_a_build_reads_only_the_present_blocks(tmp_path, monkeypatch):
    pytest.importorskip("scipy")
    pytest.importorskip("rasterio")
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import test_bathy_lidar as TL
    local = TL._surveys(tmp_path, monkeypatch)
    monkeypatch.setitem(S.REGIONS, "lidbase", dict(box=S.REGIONS["lid"]["box"], sources=["lid-fill"], mhhw=0.4, zmin=8,
                                                   prio=10))
    monkeypatch.setitem(S.REGIONS, "lid", dict(S.REGIONS["lid"], presence=["lidbase"]))
    reg = B.region_config("lid", local=local)
    for s in reg["srcs"]:
        s["rects"] = B.footprint(s)
    reg["ring"] = 0
    (block,) = B.block_keys(reg)                           # the survey's footprint is one block
    zmax = reg["zmax"]
    ring = {(block[0] + dx, block[1] + dy) for dx in (-1, 0, 1) for dy in (-1, 0, 1)}
    for version, keep in ((1, {block}), (2, set()), (3, {(block[0] + 5, block[1])})):
        asked = []

        def fake(names, z, keep=keep, asked=asked):
            asked.append((list(names), z))
            return set(keep)

        monkeypatch.setattr(B, "published_presence", fake)
        B._core.cache_clear()
        B.build("lid", str(tmp_path / "out"), version=version, workers=1, local=local)
        B._core.cache_clear()
        assert asked == [(["lidbase"], zmax - B.BLOCK_LEVELS)]
        found = B.scan_tiles(os.path.join(str(tmp_path / "out"), "lid", "v%d" % version))
        tiles = set()
        for z, ts in found.items():
            if int(z) == zmax:
                tiles |= set(ts)
        if keep == {block}:
            assert tiles and all((x >> B.BLOCK_LEVELS, y >> B.BLOCK_LEVELS) in ring for (x, y) in tiles)
        else:                                              # the survey's block is not present: nothing is read
            assert not found
