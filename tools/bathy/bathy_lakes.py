"""The Great Lakes bathymetry grids of NOAA NCEI (plan section 98): one tar.gz per lake, about 3 arc-seconds (~90 m),
depths and the land around them in metres relative to the lake's Low Water Datum (IGLD 1985; positive up, water
negative), NAD83 geographic, nodata -9999.

A build downloads its lake's archive once into its work directory (a new empty folder), checks the archive against the
pinned SHA-256, and takes out the one GeoTIFF it needs: a regular file named exactly <lake>_lld/<lake>_lld.tif (or ./ before it), written
under its base name only (nothing else in the archive is unpacked). The GeoTIFF's CRS, nodata and bounds are checked
against the pinned values before the build reads it.

Sizes and hashes measured 2026-11-15 (www.ngdc.noaa.gov, the files of the NGDC 1996-1999 compilations; Lake Superior's
grid is the compilation's draft and carries no DOI).
"""

import hashlib
import os
import shutil
import ssl
import tarfile
import urllib.request

URL = "https://www.ngdc.noaa.gov/mgg/greatlakes/%s/data/geotiff/%s_lld.geotiff.tar.gz"
NODATA = -9999.0
BOUNDS_TOL = 0.01          # degrees: the GeoTIFF's bounds against the pinned ones
MAX_TIF = 1 << 30          # bytes: a member larger than this is refused (the largest, Superior, is ~0.4 GB)
CHUNK = 1 << 20

# lake -> pinned archive and GeoTIFF facts. bounds = (west, south, east, north) degrees.
GRIDS = {
    "superior": dict(sha256="273908d7d4c6f5ae4741603fa7dbe78d259b719e73244d0f8d16e558bdbcd407", bytes=100260013,
                     bounds=(-92.2004, 45.9996, -83.9996, 49.5004)),
    "michigan": dict(sha256="7e42ece50a4a8614b630745aefaa46dfb759b4077ad754e3d790960094494ad8", bytes=47101293,
                     bounds=(-88.0004, 41.6196, -84.4996, 46.0904)),
    "huron": dict(sha256="c67bf76db8f58f3f069c30207e96e9ab061d14a80539a690e510417912152089", bytes=40393520,
                  bounds=(-84.5004, 42.9996, -79.6796, 46.5004)),
    "erie": dict(sha256="730f9209900cd7a671011bf793860c643b7e2c9faf4c7c3f97b08a3d5c5e6715", bytes=22548055,
                 bounds=(-84.0004, 40.9996, -77.9996, 43.0004)),
    "ontario": dict(sha256="80c9fa04065f1b5754304c96747d34d8e266caebdfc1e452e7eb7fb5f979141e", bytes=10600614,
                    bounds=(-79.9004, 43.1496, -76.0496, 44.2504)),
}


def url(lake):
    return URL % (lake, lake)


def stem(lake):
    return "%s_lld" % lake


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(CHUNK), b""):
            h.update(b)
    return h.hexdigest()


def download(src_url, path, user_agent, timeout=120):
    """Stream `src_url` into `path` (verified TLS with certifi's list, a named user agent)."""
    import certifi
    ctx = ssl.create_default_context(cafile=certifi.where())
    req = urllib.request.Request(src_url, headers={"User-Agent": user_agent})
    tmp = path + ".part"
    with urllib.request.urlopen(req, context=ctx, timeout=timeout) as r, open(tmp, "wb") as f:
        shutil.copyfileobj(r, f, CHUNK)
    os.replace(tmp, path)


def extract_tif(archive, lake, dest):
    """The lake's GeoTIFF taken out of `archive` into `dest` under its base name; nothing else is unpacked. Raises
    ValueError when the member is missing, not a regular file, or too large."""
    want = "%s/%s.tif" % (stem(lake), stem(lake))
    with tarfile.open(archive, "r:gz") as tf:
        member = None
        for m in tf:
            if (m.name[2:] if m.name.startswith("./") else m.name) == want:
                member = m
                break
        if member is None:
            raise ValueError("%s: the archive holds no %s" % (lake, want))
        if not member.isreg():
            raise ValueError("%s: %s is not a regular file" % (lake, want))
        if not 0 < member.size <= MAX_TIF:
            raise ValueError("%s: %s is %d bytes (allowed 1..%d)" % (lake, want, member.size, MAX_TIF))
        out = os.path.join(dest, os.path.basename(want))
        src = tf.extractfile(member)
        with open(out + ".part", "wb") as f:
            shutil.copyfileobj(src, f, CHUNK)
        os.replace(out + ".part", out)
    if os.path.getsize(out) != member.size:
        raise ValueError("%s: %s came out %d bytes, the archive says %d" % (lake, want, os.path.getsize(out),
                                                                             member.size))
    return out


def check_tif(path, lake):
    """The GeoTIFF's CRS (geographic), nodata and bounds against the pinned ones. Raises ValueError."""
    import rasterio
    want = GRIDS[lake]["bounds"]
    with rasterio.open(path) as d:
        if d.crs is None or not d.crs.is_geographic:
            raise ValueError("%s: the grid's CRS %s is not geographic" % (lake, d.crs))
        if d.nodata is None or float(d.nodata) != NODATA:
            raise ValueError("%s: the grid's nodata %s is not %g" % (lake, d.nodata, NODATA))
        if d.count != 1:
            raise ValueError("%s: %d bands (want 1)" % (lake, d.count))
        got = (d.bounds.left, d.bounds.bottom, d.bounds.right, d.bounds.top)
    if any(abs(a - b) > BOUNDS_TOL for a, b in zip(got, want)):
        raise ValueError("%s: the grid's bounds %s are not the pinned %s" % (lake, got, want))
    return path


def fetch(lake, work, user_agent, archive=None, get=download):
    """The checked GeoTIFF of `lake`, in a new folder of `work` (lake_<lake>). `archive` = a local copy of the tar.gz
    (still checked by hash); else downloaded with `get(url, path, user_agent)`."""
    if lake not in GRIDS:
        raise ValueError("no Great Lakes grid %r (have %s)" % (lake, sorted(GRIDS)))
    spec = GRIDS[lake]
    folder = os.path.join(work, "lake_" + lake)
    os.makedirs(folder, exist_ok=True)
    tif = os.path.join(folder, stem(lake) + ".tif")
    if os.path.exists(tif):
        return check_tif(tif, lake)
    if archive is None:
        archive = os.path.join(folder, stem(lake) + ".geotiff.tar.gz")
        if not os.path.exists(archive):
            get(url(lake), archive, user_agent)
    size = os.path.getsize(archive)
    if size != spec["bytes"]:
        raise ValueError("%s: the archive is %d bytes, not the pinned %d" % (lake, size, spec["bytes"]))
    got = sha256_of(archive)
    if got != spec["sha256"]:
        raise ValueError("%s: the archive's hash %s is not the pinned %s" % (lake, got[:16], spec["sha256"][:16]))
    return check_tif(extract_tif(archive, lake, folder), lake)
