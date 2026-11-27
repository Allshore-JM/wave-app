"""The GeoTIFFs of a mirrored archive set (plan section 111a; kind "tilezip"): each archive of the set's committed
manifest (tools/bathy/mirror_*.json, written to the bucket by bathy_mirror.py) downloaded from the site's public host into
the build's work folder, checked against the manifest's size and SHA-256, its one GeoTIFF (the manifest's `member`, a
regular file) taken out under the archive's hash, and checked: one band, the pinned CRS, a nodata value. Nothing else in an
archive is unpacked, and the archive is deleted once its GeoTIFF is out.
"""
import hashlib
import os
import shutil
import zipfile

import bathy_lakes as L
import bathy_mirror as M

PUBLIC = "https://models.allshoresurf.com"
HERE = os.path.dirname(os.path.abspath(__file__))
USER_AGENT = "allshoresurf-bathy-build/1.0"
MAX_TIF = 2 * 1024 ** 3
CHUNK = 1 << 22


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(CHUNK), b""):
            h.update(b)
    return h.hexdigest()


def extract_member(archive, member, dest):
    """The member (a regular file of at most MAX_TIF bytes) copied out of the zip to `dest`. Raises ValueError."""
    with zipfile.ZipFile(archive) as z:
        try:
            info = z.getinfo(member)
        except KeyError:
            raise ValueError("%s: no member %s" % (archive, member)) from None
        if info.is_dir() or not 0 < info.file_size <= MAX_TIF:
            raise ValueError("%s: member %s is %d bytes (allowed 1..%d)" % (archive, member, info.file_size, MAX_TIF))
        with z.open(info) as src, open(dest + ".part", "wb") as out:
            shutil.copyfileobj(src, out, CHUNK)     # zipfile checks the member's CRC as it is read to the end
    os.replace(dest + ".part", dest)
    if os.path.getsize(dest) != info.file_size:
        raise ValueError("%s: %s came out %d bytes, the archive says %d" % (archive, member, os.path.getsize(dest),
                                                                             info.file_size))
    return dest


def check_tif(path, crs):
    """One band, CRS `crs` (an "EPSG:<n>" string), a nodata value. Raises ValueError."""
    import rasterio
    with rasterio.open(path) as d:
        if d.count != 1:
            raise ValueError("%s: %d bands (want 1)" % (path, d.count))
        if d.crs is None or d.crs.to_string() != crs:
            raise ValueError("%s: CRS %s, not the pinned %s" % (path, d.crs, crs))
        if d.nodata is None:
            raise ValueError("%s: no nodata value" % path)
    return path


def fetch_mirror(manifest, work, crs, get=None, root=PUBLIC, log=print):
    """[local GeoTIFF path] of every archive of `manifest` (a file name beside this module, or a path), in its order.
    get(url, path, user_agent) downloads a file (default: bathy_lakes.download)."""
    get = get or L.download
    m = M.load(manifest if os.path.isabs(manifest) else os.path.join(HERE, manifest))
    folder = os.path.join(work, "src_" + m["set"])
    os.makedirs(folder, exist_ok=True)
    out = []
    for f in m["files"]:
        stem = f["sha256"][:16]
        tif = os.path.join(folder, stem + ".tif")
        if not os.path.exists(tif):
            arc = os.path.join(folder, stem + ".zip")
            get("%s/%s" % (root, M.key_of(m, f)), arc, USER_AGENT)
            n = os.path.getsize(arc)
            if n != f["bytes"] or sha256_of(arc) != f["sha256"]:
                raise ValueError("%s: %d bytes / hash differ from the manifest (%d, %s)" % (
                    M.key_of(m, f), n, f["bytes"], f["sha256"][:16]))
            extract_member(arc, f["member"], tif)
            os.remove(arc)
        out.append(check_tif(tif, crs))
        log("%s: %s ready" % (m["set"], stem))
    return out
