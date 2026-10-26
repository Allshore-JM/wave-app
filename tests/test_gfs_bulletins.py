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
    snap = st.snapshot()
    assert snap["reads"]["layout"] == 1 and "another member" in snap["last_layout_read"]
    assert snap["last_layout_read"][:2] == "20" and snap["last_layout_read"].split(" ")[0].endswith("Z")
    assert snap["layout_suspect"] is None, "the probe's verdict on the archive's size is kept apart"


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


# ---------------- G36 fixes (plan section 53, step 7) ----------------

import urllib.error  # noqa: E402


def hdr(name="./gfswave.41113.bull", size=52063, patch=()):
    """A ustar header block as tarfile writes it, with byte patches and the checksum made right again."""
    ti = tarfile.TarInfo(name)
    ti.size = size
    b = bytearray(ti.tobuf(format=tarfile.USTAR_FORMAT))
    assert len(b) == 512
    for at, value in patch:
        b[at:at + len(value)] = value
    b[148:156] = b" " * 8
    b[148:156] = f"{sum(b):06o}\0 ".encode()
    return bytes(b)


def test_a_header_tarfile_writes_passes():
    assert GB.check_header(hdr(), "41113", 52063) is None


@pytest.mark.parametrize("block,sid,size,expect", [
    (hdr(patch=[(345, b"x")]), "41113", 52063, "name with a prefix"),
    (hdr("./gfswave.141113.bull"), "41113", 52063, "another member"),        # the name is compared whole
    (hdr("./gfswave.41113.bull"), "1113", 52063, "another member"),
    (hdr("../gfswave.41113.bull"), "41113", 52063, "another member"),
    (hdr(size=52064), "41113", 52063, "another size"),                         # larger
    (hdr(size=52062), "41113", 52063, "another size"),                         # smaller
    (hdr(patch=[(124, b"00000145539\0")]), "41113", 52063, "bad number field"),   # a 9 is no octal digit
    (hdr(patch=[(124, b"+0000145537\0")]), "41113", 52063, "bad number field"),   # int() would take the sign
    (hdr(patch=[(124, b"0o000145537\0")]), "41113", 52063, "bad number field"),   # and a 0o prefix
])
def test_every_header_field_is_checked(block, sid, size, expect):
    assert expect in (GB.check_header(block, sid, size) or "")


def test_a_checksum_with_a_decimal_digit_is_refused():
    b = bytearray(hdr())
    b[148:156] = b"0000008\0"
    assert GB.check_header(bytes(b), "41113", 52063) == "bad number field"


def test_the_location_is_compared_whole():
    t = A.bulletin("41113", RUN)
    assert GB.check_text(t, "4111", RUN[:8], RUN[8:]) == "another station's text"
    assert GB.check_text(A.bulletin("411130", RUN), "41113", RUN[:8], RUN[8:]) == "another station's text"
    assert GB.check_text(t, "41113", RUN[:8], RUN[8:]) is None


@pytest.mark.parametrize("change", ["gap", "half", "size0", "four", "big"])
def test_more_damaged_index_documents_are_refused(arch, change):
    data, texts, doc, index = arch
    doc = json.loads(json.dumps(doc))
    m = doc["members"]
    if change == "gap":                                  # aligned, but inside the previous member's 512-B header room
        m[2][1] = ((m[1][1] + m[1][2] + 511) // 512) * 512
    elif change == "half":
        m[3][1] += 256
    elif change == "size0":
        m[0][2] = 0
    elif change == "four":
        m[0].append(1)
    elif change == "big":
        m[-1][2] = GB.MAX_SIZE + 1
        doc["archive_bytes"] = m[-1][1] + m[-1][2]
    with pytest.raises(ValueError):
        GB.parse_index(doc)


def test_an_unknown_station_is_counted_but_is_no_error():
    """G36 B-3a: an id typed into the address must not replace the last real error."""
    st = GB.Stats()
    st.note("missing", "x")
    st.note("unknown", "99999")
    snap = st.snapshot()
    assert snap["reads"]["unknown"] == 1 and snap["last_error"] == "missing: x"


def test_the_probes_verdict_on_the_size_clears():
    """G36 A P3-1: layout_suspect is the last probe's verdict; probe_layouts how many saw another size."""
    st = GB.Stats()
    st.probe_layout("archive 12 B")
    assert st.snapshot()["layout_suspect"] == "archive 12 B" and st.snapshot()["probe_layouts"] == 1
    st.probe_layout(None)
    snap = st.snapshot()
    assert snap["layout_suspect"] is None and snap["probe_layouts"] == 1 and snap["reads"]["layout"] == 0
    st.probe_layout("again")
    assert st.snapshot()["probe_layouts"] == 2


def test_a_size_zero_member_is_refused_by_the_tool():
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as tf:
        ti = tarfile.TarInfo("./gfswave.51201.bull")
        ti.size = 0
        tf.addfile(ti, io.BytesIO(b""))
    with pytest.raises(ValueError):
        B.index_from_tar(io.BytesIO(buf.getvalue()), RUN, total=len(buf.getvalue()))


def test_the_tool_records_one_size_and_stride_when_the_layout_has_them():
    data, _ = A.make_archive(RUN, ["13130", "41113", "51201"])          # ids of one length: equal members
    doc = A.index_doc(RUN, data)
    m = doc["members"]
    assert doc["member_size"] == m[0][2] and doc["stride"] == m[1][1] - m[0][1] == m[2][1] - m[1][1]
    assert doc["stride"] == 1536 + ((m[0][2] + 511) // 512) * 512
    data, _ = A.make_archive(RUN, IDS)                                  # NW-HFO: a byte longer, the same blocks
    doc = A.index_doc(RUN, data)
    assert doc["member_size"] is None and doc["stride"] == 1536 + ((doc["members"][0][2] + 511) // 512) * 512
    long = A.bulletin("41113", RUN) + " " * 600                          # one more 512-B block: no one stride
    data, _ = A.make_archive(RUN, ["13130", "41113", "51201"], texts={"41113": long})
    doc = A.index_doc(RUN, data)
    assert doc["member_size"] is None and doc["stride"] is None


def _index_file(tmp_path, doc):
    p = tmp_path / "index.json"
    p.write_text(json.dumps(doc))
    return str(p)


def test_the_tools_check_passes_on_a_run_of_the_same_layout(arch, tmp_path, capsys):
    data, texts, doc, index = arch
    calls = []
    B.check([RUN], _index_file(tmp_path, doc), head_length=lambda url: len(data), get_range=ranged(data, calls))
    out = capsys.readouterr().out
    assert f"{RUN}: 3 of 3 sampled members as indexed" in out and "= index" in out
    assert [c[1] for c in calls] == [index[s][0] - 512 for s in (IDS[0], IDS[1], IDS[-1])]


def test_the_tools_check_names_a_run_of_another_length(arch, tmp_path, capsys):
    data, texts, doc, index = arch
    with pytest.raises(SystemExit):
        B.check([RUN], _index_file(tmp_path, doc), head_length=lambda url: len(data) + 512, get_range=ranged(data))
    assert "DIFFERS" in capsys.readouterr().out


def test_the_tools_check_says_which_run_nomads_has_not_written(arch, tmp_path, capsys):
    """G36 A P3-5: one line naming the run and the HTTP status, then exit 1; no traceback."""
    data, texts, doc, index = arch

    def head(url):
        raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)
    with pytest.raises(SystemExit) as got:
        B.check(["2026102518", RUN], _index_file(tmp_path, doc), head_length=head, get_range=ranged(data))
    out = capsys.readouterr().out
    assert "2026102518: not readable on AWS (404)" in out and f"{RUN}: not readable on AWS (404)" in out
    assert "2 problem(s)" in str(got.value)


def test_the_tools_check_counts_a_member_it_cannot_read(arch, tmp_path, capsys):
    data, texts, doc, index = arch

    def get(url, a, b):
        raise urllib.error.URLError("reset")
    with pytest.raises(SystemExit):
        B.check([RUN], _index_file(tmp_path, doc), head_length=lambda url: len(data), get_range=get)
    assert f"{RUN}: 0 of 3 sampled members" in capsys.readouterr().out


@pytest.mark.parametrize("argv,expect", [
    (["2026102513"], "not YYYYMMDDHH"),
    (["20261025"], "not YYYYMMDDHH"),
    (["2026102512", "2026102506"], "build from one run"),
])
def test_the_tools_command_line_refuses_bad_runs(argv, expect, monkeypatch):
    def network(*a, **k):
        raise AssertionError("a refused run reached the network")
    monkeypatch.setattr(B, "build", network)
    monkeypatch.setattr(B, "check", network)
    with pytest.raises(SystemExit) as got:
        B.main(argv)
    assert expect in str(got.value)


def test_the_tools_command_line_checks_or_builds(monkeypatch):
    seen = []
    monkeypatch.setattr(B, "check", lambda runs, path: seen.append(("check", runs, path)))
    monkeypatch.setattr(B, "build", lambda run, path: seen.append(("build", run, path)))
    B.main(["--check", "2026102512", "2026102506"])
    B.main(["2026102512", "--out", "x.json"])
    assert seen == [("check", ["2026102512", "2026102506"], GB.INDEX_PATH), ("build", "2026102512", "x.json")]


# ---------------- G36 re-check fixes (plan section 53, fix round 2) ----------------

import http.client  # noqa: E402


def test_a_content_refusal_is_no_layout_read():
    st = GB.Stats()
    st.note("content", "another station's text")
    snap = st.snapshot()
    assert snap["reads"]["content"] == 1 and snap["last_layout_read"] is None and snap["last_error"]


def test_the_tools_check_survives_a_cut_body_and_a_bad_length(arch, tmp_path, capsys):
    """Re-check F-5: http.client's IncompleteRead (a body cut short) and a ValueError (a Content-Length that is not a
    number) are one printed line each, never a traceback."""
    data, texts, doc, index = arch

    def cut(url, a, b):
        raise http.client.IncompleteRead(b"x" * 10, b - a - 9)
    with pytest.raises(SystemExit):
        B.check([RUN], _index_file(tmp_path, doc), head_length=lambda url: len(data), get_range=cut)
    assert f"{RUN}: 0 of 3 sampled members" in capsys.readouterr().out

    def bad_length(url):
        raise ValueError("invalid literal for int() with base 10: 'abc'")
    with pytest.raises(SystemExit) as got:
        B.check([RUN], _index_file(tmp_path, doc), head_length=bad_length, get_range=ranged(data))
    assert f"{RUN}: not readable on AWS" in capsys.readouterr().out and "1 problem(s)" in str(got.value)


def test_the_tools_check_survives_a_timeout_at_a_member(arch, tmp_path, capsys):
    data, texts, doc, index = arch

    def slow(url, a, b):
        raise TimeoutError("timed out")
    with pytest.raises(SystemExit):
        B.check([RUN], _index_file(tmp_path, doc), head_length=lambda url: len(data), get_range=slow)
    assert "timed out" in capsys.readouterr().out
