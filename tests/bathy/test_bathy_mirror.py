"""The source-archive mirror (plan section 111): the manifest checks, the hash / size / zip checks before anything is
written, a key named by the content's hash that is never written twice, the read-back, the README beside the archives."""
import hashlib
import io
import json
import os
import sys
import zipfile

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "bathy"))
import bathy_mirror as M  # noqa: E402

MEMBER = "NSW Government - DCCEEW/DEM/5 Metre/NSW_Marine_5m_TopoBathy_DEM.tif"


def _zip(member=MEMBER, body=b"tif bytes"):
    b = io.BytesIO()
    with zipfile.ZipFile(b, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(member, body)
        z.writestr("readme.html", b"<html></html>")
    return b.getvalue()


def _manifest(tmp_path, blobs, **over):
    files = [dict(url="https://elvis-downloads.s3.amazonaws.com/DATA_%d.zip" % i, bytes=len(b),
                  sha256=hashlib.sha256(b).hexdigest(), member=MEMBER) for i, b in enumerate(blobs)]
    m = dict(set="nsw-marine-2018", credit="NSW DPIE", licence="CC BY 4.0", files=files)
    m.update(over)
    p = tmp_path / "m.json"
    p.write_text(json.dumps(m))
    return str(p)


class FakeClient:
    def __init__(self, store):
        self.store = store

    def head_object(self, Bucket, Key):
        return {"ContentLength": len(self.store.objects[Key][0])}


class FakeStore:
    bucket = "b"

    def __init__(self, objects=None):
        self.objects = dict(objects or {})
        self.client = FakeClient(self)
        self.puts = []

    def list_keys(self, prefix):
        return [k for k in self.objects if k.startswith(prefix)]

    def put(self, key, body, ct, cache):
        self.puts.append(key)
        self.objects[key] = (body, ct, cache)


def _getter(blobs_by_url, served=None):
    def get(url, dest, want):
        body = (served or {}).get(url, blobs_by_url[url])
        with open(dest, "wb") as f:
            f.write(body)
        return len(body), hashlib.sha256(body).hexdigest()
    return get


def test_the_committed_nsw_manifest_is_valid():
    m = M.load(os.path.join(ROOT, "tools", "bathy", "mirror_nsw.json"))
    assert m["set"] == "nsw-marine-2018" and len(m["files"]) == 10
    assert all(f["member"].endswith("/NSW_Marine_5m_TopoBathy_DEM.tif") for f in m["files"])
    assert "Department of Planning, Industry and Environment" in m["licence"] and "by/4.0" in m["licence"]
    assert sum(f["bytes"] for f in m["files"]) == 1040022345


@pytest.mark.parametrize("bad", [
    dict(set="NSW"), dict(credit=""), dict(files=[]),
    dict(files=[dict(url="http://elvis-downloads.s3.amazonaws.com/a.zip", bytes=1, sha256="0" * 64, member="m")]),
    dict(files=[dict(url="https://evil.example/a.zip", bytes=1, sha256="0" * 64, member="m")]),
    dict(files=[dict(url="https://u:p@elvis-downloads.s3.amazonaws.com/a.zip", bytes=1, sha256="0" * 64, member="m")]),
    dict(files=[dict(url="https://elvis-downloads.s3.amazonaws.com/a.zip", bytes=0, sha256="0" * 64, member="m")]),
    dict(files=[dict(url="https://elvis-downloads.s3.amazonaws.com/a.zip", bytes=1, sha256="xyz", member="m")]),
    dict(files=[dict(url="https://elvis-downloads.s3.amazonaws.com/a.zip", bytes=1, sha256="0" * 64, member="")]),
    dict(files=[dict(url="https://elvis-downloads.s3.amazonaws.com/a.zip", bytes=1, sha256="0" * 64, member="m"),
                dict(url="https://elvis-downloads.s3.amazonaws.com/b.zip", bytes=1, sha256="0" * 64, member="m")]),
])
def test_a_bad_manifest_is_refused(tmp_path, bad):
    with pytest.raises(ValueError):
        M.load(_manifest(tmp_path, [_zip()], **bad))


def test_each_archive_is_written_once_under_its_hash_with_a_readme(tmp_path):
    blobs = [_zip(body=b"a"), _zip(body=b"b")]
    m = M.load(_manifest(tmp_path, blobs))
    by_url = {f["url"]: b for f, b in zip(m["files"], blobs)}
    store = FakeStore()
    keys = M.mirror(m, store, str(tmp_path), get=_getter(by_url), log=lambda *a: None)
    want = ["static/bathy-src/nsw-marine-2018/%s.zip" % f["sha256"][:16] for f in m["files"]]
    assert keys == want + ["static/bathy-src/nsw-marine-2018/README.txt"]
    for k, b in zip(want, blobs):
        assert store.objects[k] == (b, "application/zip", M.IMMUTABLE)
    assert b"CC BY 4.0" in store.objects[keys[-1]][0]
    # a second run writes nothing: the keys are there (the hash names the content)
    again = M.mirror(m, store, str(tmp_path), get=_getter(by_url), log=lambda *a: None)
    assert again == [] and len(store.puts) == 3


def test_a_changed_or_damaged_archive_is_never_written(tmp_path):
    good = _zip()
    m = M.load(_manifest(tmp_path, [good]))
    url = m["files"][0]["url"]
    store = FakeStore()
    with pytest.raises(ValueError, match="sha256"):
        M.mirror(m, store, str(tmp_path), get=_getter({url: good}, served={url: _zip(body=b"other")}),
                 log=lambda *a: None)
    assert store.puts == []
    # the right hash but a zip without the member the builder reads
    other = _zip(member="something/else.tif")
    m2 = M.load(_manifest(tmp_path, [other]))
    m2["files"][0]["member"] = MEMBER
    with pytest.raises(ValueError, match="no member"):
        M.mirror(m2, store, str(tmp_path), get=_getter({m2["files"][0]["url"]: other}), log=lambda *a: None)
    assert store.puts == []


def test_a_short_read_back_stops_the_run(tmp_path):
    blob = _zip()
    m = M.load(_manifest(tmp_path, [blob]))
    store = FakeStore()
    store.client.head_object = lambda Bucket, Key: {"ContentLength": 1}
    with pytest.raises(RuntimeError, match="bucket holds"):
        M.mirror(m, store, str(tmp_path), get=_getter({m["files"][0]["url"]: blob}), log=lambda *a: None)
