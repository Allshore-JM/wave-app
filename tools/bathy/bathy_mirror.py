"""Copy a set of source archives into the frames bucket, once (plan section 111): static/bathy-src/<set>/<sha16>.zip.

Some sources are handed out as download links that expire (NSW's ELVIS orders: 48 h after the e-mail; Allen Coral Atlas:
an e-mailed link), so the builder cannot read them where they came from. A committed manifest (tools/bathy/mirror_*.json)
lists each archive's link, size and SHA-256 (measured on the owner's own download); this tool fetches each link, checks
the size, the hash and the zip's members (zipfile CRC test), and writes it to the bucket under a key named by its hash.
A key that is already there is never written again (the hash names the content), and the bucket is read back after each
write (its size). A README.txt with the set's credit and licence is written beside the archives (once).

    python tools/bathy/bathy_mirror.py tools/bathy/mirror_nsw.json            (R2_* environment, as bathy-build)
    python tools/bathy/bathy_mirror.py tools/bathy/mirror_nsw.json --check    (the manifest only: no network)

Only links on ALLOWED_HOSTS are fetched (https), and an archive larger than MAX_BYTES is refused before it is read.
"""
import argparse
import hashlib
import json
import os
import re
import shutil
import ssl
import sys
import tempfile
import urllib.parse
import urllib.request
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

PREFIX = "static/bathy-src"
ALLOWED_HOSTS = ("elvis-downloads.s3.amazonaws.com",)
MAX_BYTES = 4 * 1024 ** 3          # one archive
NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]{2,40}$")
SHA_RE = re.compile(r"^[0-9a-f]{64}$")
USER_AGENT = "allshoresurf-bathy-mirror/1.0"
IMMUTABLE = "public, max-age=31536000, immutable"
CHUNK = 1 << 22


def load(path):
    """The manifest, checked: {set, credit, licence, files: [{url, bytes, sha256, member}]}. Raises ValueError."""
    with open(path, encoding="utf-8") as f:
        m = json.load(f)
    if not NAME_RE.match(str(m.get("set", ""))):
        raise ValueError("set %r: lower-case letters, digits and '-', 3-41 characters" % m.get("set"))
    for k in ("credit", "licence"):
        if not isinstance(m.get(k), str) or not m[k].strip():
            raise ValueError("the manifest needs a %s" % k)
    files = m.get("files")
    if not isinstance(files, list) or not files:
        raise ValueError("the manifest lists no files")
    seen = set()
    for f in files:
        u = urllib.parse.urlsplit(str(f.get("url", "")))
        if u.scheme != "https" or u.hostname not in ALLOWED_HOSTS or u.username or u.password or u.port:
            raise ValueError("%s: only https links on %s" % (f.get("url"), ", ".join(ALLOWED_HOSTS)))
        if not SHA_RE.match(str(f.get("sha256", ""))):
            raise ValueError("%s: sha256 must be 64 hex digits" % f["url"])
        n = f.get("bytes")
        if isinstance(n, bool) or not isinstance(n, int) or not 0 < n <= MAX_BYTES:
            raise ValueError("%s: bytes %r must be 1..%d" % (f["url"], n, MAX_BYTES))
        if not isinstance(f.get("member"), str) or not f["member"]:
            raise ValueError("%s: name the archive member the builder reads" % f["url"])
        if f["sha256"] in seen:
            raise ValueError("%s: the same archive twice" % f["url"])
        seen.add(f["sha256"])
    return m


def key_of(m, f):
    return "%s/%s/%s.zip" % (PREFIX, m["set"], f["sha256"][:16])


def fetch(url, dest, want_bytes, timeout=120):
    """Stream `url` into `dest`, refusing a body longer than want_bytes. Returns (bytes, sha256)."""
    import certifi
    ctx = ssl.create_default_context(cafile=certifi.where())
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    h, n = hashlib.sha256(), 0
    with urllib.request.urlopen(req, context=ctx, timeout=timeout) as r, open(dest, "wb") as out:
        if r.status != 200:
            raise ValueError("%s: HTTP %d" % (url, r.status))
        cl = r.headers.get("Content-Length")
        if cl is not None and int(cl) != want_bytes:
            raise ValueError("%s: %s bytes offered, the manifest says %d" % (url, cl, want_bytes))
        while True:
            b = r.read(CHUNK)
            if not b:
                break
            n += len(b)
            if n > want_bytes:
                raise ValueError("%s: longer than the manifest's %d bytes" % (url, want_bytes))
            h.update(b)
            out.write(b)
    return n, h.hexdigest()


def check_zip(path, member):
    """The archive's CRC test, and the member the builder reads present as a regular file. Raises ValueError."""
    with zipfile.ZipFile(path) as z:
        bad = z.testzip()
        if bad is not None:
            raise ValueError("%s: member %s fails its CRC" % (path, bad))
        names = [i.filename for i in z.infolist() if not i.is_dir()]
    if member not in names:
        raise ValueError("%s: no member %s" % (path, member))


def readme(m):
    return ("Source archives kept for the bathymetry build of allshoresurf.com (set %s), as downloaded, unchanged.\n\n"
            "%s\nLicence: %s\n" % (m["set"], m["credit"], m["licence"]))


def mirror(m, store, work, get=fetch, log=print):
    """Each archive of manifest `m` into the bucket (store: build_bathy's R2 Store). Returns the keys written."""
    have = set(store.list_keys("%s/%s/" % (PREFIX, m["set"])))
    written = []
    for f in m["files"]:
        key = key_of(m, f)
        if key in have:
            log("%s: already in the bucket (%s)" % (f["url"], key))
            continue
        tmp = os.path.join(work, f["sha256"][:16] + ".zip")
        n, sha = get(f["url"], tmp, f["bytes"])
        if n != f["bytes"] or sha != f["sha256"]:
            raise ValueError("%s: %d bytes, sha256 %s; the manifest says %d, %s" % (
                f["url"], n, sha[:16], f["bytes"], f["sha256"][:16]))
        check_zip(tmp, f["member"])
        with open(tmp, "rb") as fi:
            store.put(key, fi.read(), "application/zip", IMMUTABLE)
        head = store.client.head_object(Bucket=store.bucket, Key=key)
        if int(head["ContentLength"]) != f["bytes"]:
            raise RuntimeError("%s: the bucket holds %s bytes, not %d" % (key, head["ContentLength"], f["bytes"]))
        os.remove(tmp)
        written.append(key)
        log("%s -> %s (%d bytes, sha256 %s)" % (f["url"], key, n, sha[:16]))
    rk = "%s/%s/README.txt" % (PREFIX, m["set"])
    if rk not in have:
        store.put(rk, readme(m).encode("utf-8"), "text/plain; charset=utf-8", IMMUTABLE)
        written.append(rk)
    return written


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("manifest")
    ap.add_argument("--check", action="store_true", help="check the manifest only (no network)")
    a = ap.parse_args(argv)
    m = load(a.manifest)
    print("%s: %d archives, %.1f MB" % (m["set"], len(m["files"]), sum(f["bytes"] for f in m["files"]) / 1e6))
    if a.check:
        return 0
    sys.path.insert(0, os.path.join(os.path.dirname(HERE), "model_frames"))
    import publish as P
    store = P.Store(P.r2_client(os.environ["R2_ACCOUNT_ID"], os.environ["R2_ACCESS_KEY_ID"],
                                os.environ["R2_SECRET_ACCESS_KEY"]), os.environ["R2_BUCKET"])
    work = tempfile.mkdtemp(prefix="mirror_")
    try:
        keys = mirror(m, store, work)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    print("wrote %d keys" % len(keys))
    return 0


if __name__ == "__main__":
    sys.exit(main())
