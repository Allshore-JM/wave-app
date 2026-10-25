"""The GFS-Wave bulletin archive reader and its index (plan section 53 A): gfs_bulletins.py and
tools/build_bull_index.py, on a synthetic archive written the way NOAA writes gfswave.tHHz.bull_tar. No network."""
import io
import json
import os
import sys
import tarfile

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
for p in (ROOT, HERE, os.path.join(ROOT, "tools")):
    sys.path.insert(0, p)
import gfs_bulletins as GB  # noqa: E402
import build_bull_index as B  # noqa: E402
import bull_archive as A  # noqa: E402

RUN = "2026102512"
IDS = ["13130", "41113", "51201", "NW-HFO", "Alaska_NS3", "TPC00"]


@pytest.fixture
def arch():
    data, texts = A.make_archive(RUN, IDS)
    doc = A.index_doc(RUN, data)
    return data, texts, doc, GB.parse_index(doc)


def ranged(data, calls=None):
    def get(url, start, end):
        if calls is not None:
            calls.append((url, start, end))
        return data[start:end + 1]
    return get


def test_the_index_tool_reads_the_archive_like_noaa_writes_it(arch):
    data, texts, doc, index = arch
    assert doc["format"] == "bull-index-v1" and doc["run"] == RUN and doc["archive_bytes"] == len(data)
    assert [m[0] for m in doc["members"]] == IDS
    assert index[IDS[0]][0] == 1536, "a pax header (1,024 B) and the ustar header before the first text, as NOAA's"
    with tarfile.open(fileobj=io.BytesIO(data)) as tf:              # the data offsets tarfile itself reports
        want = {m.name[len("./gfswave."):-len(".bull")]: (m.offset_data, m.size) for m in tf if m.isreg()}
    assert index == want
    for sid, (off, size) in index.items():
        assert data[off:off + size].decode("ascii") == texts[sid]
        assert data[off - 512:off - 412].split(b"\0")[0] == f"./gfswave.{sid}.bull".encode()


@pytest.mark.parametrize("bad", ["path", "dir", "name", "empty", "size"])
def test_the_index_tool_refuses_a_layout_the_reader_cannot_trust(bad):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as tf:
        if bad != "empty":
            body = A.bulletin("51201", RUN).encode()
            ti = tarfile.TarInfo({"name": "./other.txt"}.get(bad, "./gfswave.51201.bull"))
            ti.size = len(body)
            if bad == "path":
                ti.pax_headers = {"path": "./gfswave.51201.bull", "atime": "1"}
            if bad == "dir":
                ti.type, ti.size, body = tarfile.DIRTYPE, 0, b""
            if bad == "size":
                body = b"x" * (GB.MAX_SIZE + 1)
                ti.size = len(body)
            tf.addfile(ti, io.BytesIO(body))
    with pytest.raises(ValueError):
        B.index_from_tar(io.BytesIO(buf.getvalue()), RUN, total=len(buf.getvalue()))


def test_one_bulletin_is_one_range_read_with_its_header(arch):
    data, texts, doc, index = arch
    calls = []
    text, reason, detail = GB.archive_bulletin(RUN[:8], RUN[8:], "41113", ranged(data, calls), index=index,
                                               stats=GB.Stats())
    assert (text, reason, detail) == (texts["41113"], "ok", None)
    off, size = index["41113"]
    assert calls == [(GB.archive_url(RUN[:8], RUN[8:]), off - 512, off + size - 1)]
    assert calls[0][0].endswith("/gfs.20261025/12/wave/station/gfswave.t12z.bull_tar")


def test_the_last_member_reads_to_its_own_end(arch):
    data, texts, doc, index = arch
    text, reason, _ = GB.archive_bulletin(RUN[:8], RUN[8:], IDS[-1], ranged(data), index=index, stats=GB.Stats())
    assert reason == "ok" and text == texts[IDS[-1]]


def test_another_members_bytes_at_the_offset_are_a_layout_change(arch):
    data, texts, doc, index = arch
    moved = A.plant(data, index, "41113", "51201")                   # NOAA reordered the archive
    st = GB.Stats()
    text, reason, detail = GB.archive_bulletin(RUN[:8], RUN[8:], "41113", ranged(moved), index=index, stats=st)
    assert text is None and reason == "layout" and "another member" in detail
    assert st.snapshot()["layout_suspect"] and st.snapshot()["reads"]["layout"] == 1


@pytest.mark.parametrize("where,value,expect", [
    (124, b"00000000001\0", "another size"),
    (148, b"0000000\0", "bad checksum"),
    (257, b"xxxxx\0", "no ustar magic"),
    (156, b"5", "not a regular file"),
    (124, b"0000000009x\0", "bad number field"),
])
def test_a_damaged_header_is_refused(arch, where, value, expect):
    data, texts, doc, index = arch
    off, _ = index["51201"]
    out = bytearray(data)
    out[off - 512 + where:off - 512 + where + len(value)] = value
    if expect != "bad checksum" and expect != "bad number field":       # the checksum still right: only `expect` fails
        h = out[off - 512:off]
        h[148:156] = b" " * 8
        out[off - 512 + 148:off - 512 + 156] = f"{sum(h):06o}\0 ".encode()
    text, reason, detail = GB.archive_bulletin(RUN[:8], RUN[8:], "51201", ranged(bytes(out)), index=index,
                                               stats=GB.Stats())
    assert reason == "layout" and expect in detail


@pytest.mark.parametrize("text_of,expect", [
    ("other station", "another station's text"),
    ("other run", "another run's text"),
    ("cut", "too few rows"),
])
def test_the_text_must_be_the_stations_bulletin_of_that_run(text_of, expect):
    real = A.bulletin("51201", RUN)
    bad = {"other station": A.bulletin("51202", RUN), "other run": A.bulletin("51201", "2026102506"),
           "cut": real[:3000] + " " * (len(real) - 3000)}[text_of]
    assert len(bad) == len(real)
    data, _ = A.make_archive(RUN, ["51201"], texts={"51201": bad})
    index = GB.parse_index(A.index_doc(RUN, data))
    text, reason, detail = GB.archive_bulletin(RUN[:8], RUN[8:], "51201", ranged(data), index=index, stats=GB.Stats())
    assert text is None and reason == "content" and expect in detail


def test_the_cycle_hour_may_be_space_padded():
    t = A.bulletin("51201", "2026102506").replace("20261025 06 UTC", "20261025  6 UTC")
    assert GB.check_text(t, "51201", "20261025", "06") is None


class _Status(Exception):
    def __init__(self, status):
        super().__init__(f"HTTP {status}")
        self.status = status


@pytest.mark.parametrize("exc,reason", [(_Status(404), "missing"), (_Status(403), "missing"),
                                        (_Status(500), "transport"), (ConnectionError("down"), "transport")])
def test_a_failed_read_says_why(arch, exc, reason):
    data, texts, doc, index = arch

    def get(url, a, b):
        raise exc
    text, why, _ = GB.archive_bulletin(RUN[:8], RUN[8:], "51201", get, index=index, stats=GB.Stats())
    assert text is None and why == reason


def test_a_short_answer_is_a_transport_failure(arch):
    data, texts, doc, index = arch
    text, why, _ = GB.archive_bulletin(RUN[:8], RUN[8:], "51201", lambda u, a, b: data[a:b], index=index,
                                       stats=GB.Stats())
    assert text is None and why == "transport"


def test_a_station_not_in_the_index_is_unknown(arch):
    data, texts, doc, index = arch
    st = GB.Stats()
    assert GB.archive_bulletin(RUN[:8], RUN[8:], "99999", ranged(data), index=index, stats=st)[1] == "unknown"
    assert st.snapshot()["reads"]["unknown"] == 1


@pytest.mark.parametrize("change", ["format", "overlap", "repeat", "beyond", "unaligned", "bool", "empty"])
def test_a_damaged_index_document_is_refused(arch, change):
    data, texts, doc, index = arch
    doc = json.loads(json.dumps(doc))
    m = doc["members"]
    if change == "format":
        doc["format"] = "bull-index-v0"
    elif change == "overlap":
        m[2][1] = m[1][1] + 100
    elif change == "repeat":
        m[2][0] = m[1][0]
    elif change == "beyond":
        doc["archive_bytes"] = m[-1][1] + m[-1][2] - 1
    elif change == "unaligned":
        m[3][1] += 1
    elif change == "bool":
        m[0][2] = True
    elif change == "empty":
        doc["members"] = []
    with pytest.raises(ValueError):
        GB.parse_index(doc)


def test_a_missing_or_damaged_index_file_gives_an_empty_index(tmp_path):
    assert GB.load_index(str(tmp_path / "none.json"))[0] == {}
    p = tmp_path / "bad.json"
    p.write_text("{not json")
    index, total, err = GB.load_index(str(p))
    assert index == {} and total is None and err


def test_the_committed_index_serves_every_listed_station():
    index, total, err = GB.load_index()
    assert err is None and len(index) == 918 and total == 49356800
    with open(GB.INDEX_PATH, encoding="utf-8") as f:
        doc = json.load(f)
    assert doc["stride"] == 53760 and doc["member_size"] == 52063
    offs = [off for off, _ in index.values()]
    assert offs == sorted(offs) and offs[0] == 1536
    with open(os.path.join(ROOT, "station_list.json"), encoding="utf-8") as f:
        listed = json.load(f)
    with open(os.path.join(ROOT, "station_blank.json"), encoding="utf-8") as f:
        removed = json.load(f)["removed"]
    assert [s for s in list(listed) + list(removed) if s not in index] == []


def test_the_counters_snapshot():
    st = GB.Stats()
    st.note("ok", run="2026102512")
    st.note("missing", "x")
    st.nomads_used()
    snap = st.snapshot()
    assert snap["reads"]["ok"] == 1 and snap["reads"]["missing"] == 1 and snap["nomads_fallbacks"] == 1
    assert snap["last_run"] == "2026102512" and snap["last_error"] == "missing: x" and snap["layout_suspect"] is None


def test_a_long_answer_is_a_transport_failure_too(arch):
    data, texts, doc, index = arch
    text, why, _ = GB.archive_bulletin(RUN[:8], RUN[8:], "51201", lambda u, a, b: data[a:b + 2], index=index,
                                       stats=GB.Stats())
    assert text is None and why == "transport"
