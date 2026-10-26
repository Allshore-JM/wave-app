"""A synthetic GFS-Wave bulletin archive and a fake NOAA for the bulletin tests (plan section 53).

`make_archive(run, ids)` writes a tar the way NOAA's gfswave.tHHz.bull_tar is written: every member a regular file
`./gfswave.<id>.bull` behind a pax header holding only atime and ctime, its text the repo's real 51201 bulletin with
the Location id and the Cycle line rewritten. `FakeNoaa` answers HEADs and GETs (ranged or not) for the AWS archive
and NOMADS's per-station files, and records every call. No network.
"""
import io
import os
import re
import tarfile

import gfs_bulletins as GB

FIX = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
with open(os.path.join(FIX, "gfswave_51201_2026100112.bull"), encoding="ascii") as _f:
    REAL = _f.read()
S3, NOMADS = "noaa-gfs-bdp-pds.s3.amazonaws.com", "nomads.ncep.noaa.gov"


def bulletin(sid, run):
    """The real 51201 bulletin as station `sid`'s of run YYYYMMDDHH."""
    text = REAL.replace("Location : 51201", f"Location : {sid}", 1)
    return re.sub(r"Cycle\s*:\s*\d{8} \d{2} UTC", f"Cycle    : {run[:8]} {run[8:]} UTC", text, count=1)


def make_archive(run, ids, texts=None, pax=True):
    """(bytes, {sid: text}) of an archive like NOAA's."""
    texts = dict(texts or {})
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as tf:
        for sid in ids:
            body = texts.setdefault(sid, bulletin(sid, run)).encode("ascii")
            ti = tarfile.TarInfo(f"./gfswave.{sid}.bull")
            ti.size = len(body)
            ti.mtime = 1792948800
            if pax:
                ti.pax_headers = {"atime": "1792948809", "ctime": "1792948869"}
            tf.addfile(ti, io.BytesIO(body))
    return buf.getvalue(), texts


def index_doc(run, data):
    import build_bull_index as B
    return B.index_from_tar(io.BytesIO(data), run, total=len(data))


class Resp:
    def __init__(self, status, body=b"", headers=None):
        self.status_code = status
        self.content = body
        self.text = body.decode("ascii", "replace")
        self.headers = dict(headers or {})
        self.raw = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def iter_content(self, n=65536, **k):
        for i in range(0, len(self.content), n):
            yield self.content[i:i + n]

    def iter_lines(self, **k):
        return iter(())


class FakeNoaa:
    """`archives[(date, run)]` = archive bytes on AWS; `nomads[(date, run, sid)]` = text on NOMADS. `s3` / `nomads_all`
    override every answer of a host with a status or an exception; `range_mode` "ok" | "200" | "short" | "other range" |
    "no range header" changes how ranged GETs are answered (a 206 carries the Content-Range S3 sends); `archive_length`
    overrides a HEAD's Content-Length."""

    def __init__(self, archives=None, nomads=None, s3=None, nomads_all=None, range_mode="ok", archive_length=None):
        self.archives = dict(archives or {})
        self.nomads = dict(nomads or {})
        self.s3, self.nomads_all = s3, nomads_all
        self.range_mode = range_mode
        self.archive_length = archive_length
        self.calls = []

    def _forced(self, host):
        a = self.s3 if host == S3 else self.nomads_all
        if isinstance(a, Exception):
            raise a
        return a

    def _archive(self, url):
        m = re.search(r"/gfs\.(\d{8})/(\d{2})/wave/station/gfswave\.t(\d{2})z\.bull_tar$", url)
        return self.archives.get((m.group(1), m.group(2))) if m and m.group(2) == m.group(3) else None

    def _nomads(self, url):
        m = re.search(r"/gfs\.(\d{8})/(\d{2})/wave/station/bulls\.t\d{2}z/gfswave\.([^/]+)\.bull$", url)
        return self.nomads.get((m.group(1), m.group(2), m.group(3))) if m else None

    def head(self, url, **k):
        host = url.split("/")[2]
        self.calls.append(("HEAD", host, url, None))
        forced = self._forced(host)
        if forced is not None:
            return Resp(forced)
        if host == S3:
            data = self._archive(url)
            if data is None:
                return Resp(404)
            n = self.archive_length if self.archive_length is not None else len(data)
            return Resp(200, headers={"Content-Length": str(n)})
        return Resp(200) if self._nomads(url) is not None else Resp(404)

    def get(self, url, headers=None, **k):
        host = url.split("/")[2]
        rng = (headers or {}).get("Range")
        self.calls.append(("GET", host, url, rng))
        forced = self._forced(host)
        if forced is not None:
            return Resp(forced)
        if host == S3:
            data = self._archive(url)
            if data is None:
                return Resp(404)
            if not rng:
                return Resp(200, data)
            a, b = (int(v) for v in rng[len("bytes="):].split("-"))
            part = data[a:b + 1]
            if self.range_mode == "200":
                return Resp(200, data)
            if self.range_mode == "short":
                part = part[:-10]
            if self.range_mode == "no range header":
                return Resp(206, part)
            if self.range_mode == "other range":
                a, b = a + 512, b + 512
            return Resp(206, part, {"Content-Range": f"bytes {a}-{b}/{len(data)}"})
        text = self._nomads(url)
        return Resp(404) if text is None else Resp(200, text.encode("ascii"))

    def urls(self, host, method=None):
        return [c[2] for c in self.calls if c[1] == host and (method is None or c[0] == method)]


def plant(data, index, at_sid, from_sid):
    """The archive bytes with `from_sid`'s header and text copied over `at_sid`'s (same sizes needed)."""
    a, size_a = index[at_sid]
    b, size_b = index[from_sid]
    assert size_a == size_b
    out = bytearray(data)
    out[a - GB.HEADER:a + size_a] = data[b - GB.HEADER:b + size_b]
    return bytes(out)
