"""Prepare a European source set once for the bathymetry build (plan section 111b) and mirror it to the bucket.

The publishers' packages are too big to read at every build (Litto3D Nouvelle-Aquitaine: 324 packs, 219 GB of 7z, mostly
point clouds; INFOMAR survey grids up to 8 GB each), so each archive of a committed prep list (tools/bathy/prep_*.json)
is downloaded once on a runner, checked (size, and the publisher's MD5 or our SHA-256 where the list has one), and the
part the builder needs is written as one compressed GeoTIFF (DEFLATE, floating-point predictor, 512-px tiles, overviews
4 and 16) inside a zip:

  litto3d   the pack's 1-m DEM tiles (MNT1m/*.asc, Lambert-93, IGN69): the 1-km tiles holding a value at or below
            `low_m` (IGN69) mosaicked into one grid; a pack with none is left out (dry land only).
  infomar   the survey's GeoTIFF (heights above LAT), pixels deeper than `deep_m` below LAT dropped and the grid cut to
            what is left; a grid in another CRS is warped to the list's CRS (bilinear, its own resolution).
  emodnet   the tile's mean-sea-level ESRI ASCII grid (EPSG:4326), cut to each of the list's boxes it meets.

Each zip goes to static/bathy-src/<set>/<sha16>.zip (never overwritten: the hash names the content) and the part's
manifest rows to static/bathy-src/<set>/parts/<i>-of-<n>.json; `merge` joins the parts into the committed
tools/bathy/mirror_<...>.json the builder reads (kind "tilezip", bathy_fetch.fetch_mirror).

    python tools/bathy/bathy_prep.py run tools/bathy/prep_litto3d_naq.json --part 3/11 [--upload] [--limit 2]
    python tools/bathy/bathy_prep.py merge tools/bathy/prep_litto3d_naq.json <parts dir or URL root> <out manifest>

Only https links on bathy_mirror.ALLOWED_HOSTS are fetched; nothing in an archive but the files named above is unpacked.
"""
import argparse
import glob
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.parse
import zipfile

import numpy as np

try:
    import zipfile_deflate64  # noqa: F401: teaches zipfile Deflate64 (INFOMAR's survey zips use it)
except ImportError:          # the build never needs it; only bathy_prep's INFOMAR step does
    zipfile_deflate64 = None

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import bathy_mirror as M   # noqa: E402

KINDS = ("litto3d", "infomar", "emodnet")
MAX_ARCHIVE = 6 * 1024 ** 3        # the largest publisher archive fetched (Litto3D NAQ's biggest pack: 2.66 GB)
CHUNK = 1 << 22
ASC_RE = re.compile(r"/MNT1m/[^/]+\.asc$", re.I)
PROFILE = dict(driver="GTiff", dtype="float32", count=1, compress="deflate", predictor=3, tiled=True, blockxsize=512,
               blockysize=512, bigtiff="IF_SAFER")
OVERVIEWS = (4, 16)


# ------------------------------------------------------------------------------------------------ the prep list

def load_list(path):
    """The checked prep list: {set, kind, credit, licence, derived, crs, files: [{url, ...}], ...}. Raises ValueError."""
    with open(path, encoding="utf-8") as f:
        p = json.load(f)
    if not M.NAME_RE.match(str(p.get("set", ""))):
        raise ValueError("set %r: lower-case letters, digits and '-', 3-41 characters" % p.get("set"))
    if p.get("kind") not in KINDS:
        raise ValueError("kind %r must be one of %s" % (p.get("kind"), KINDS))
    for k in ("credit", "licence", "derived", "crs", "mirror"):
        if not isinstance(p.get(k), str) or not p[k].strip():
            raise ValueError("the prep list needs a %s" % k)
    files = p.get("files")
    if not isinstance(files, list) or not files:
        raise ValueError("the prep list names no archives")
    seen = set()
    for f in files:
        u = urllib.parse.urlsplit(str(f.get("url", "")))
        if u.scheme != "https" or u.hostname not in M.ALLOWED_HOSTS or u.username or u.password or u.port:
            raise ValueError("%s: only https links on %s" % (f.get("url"), ", ".join(M.ALLOWED_HOSTS)))
        if f["url"] in seen:
            raise ValueError("%s: listed twice" % f["url"])
        seen.add(f["url"])
        n = f.get("bytes")
        if n is not None and (isinstance(n, bool) or not isinstance(n, int) or not 0 < n <= MAX_ARCHIVE):
            raise ValueError("%s: bytes %r must be 1..%d or absent" % (f["url"], n, MAX_ARCHIVE))
        for k, n in (("md5", 32), ("sha256", 64)):
            if f.get(k) is not None and not re.match(r"^[0-9a-f]{%d}$" % n, str(f[k])):
                raise ValueError("%s: %s must be %d hex digits" % (f["url"], k, n))
    if p["kind"] == "litto3d" and not isinstance(p.get("low_m"), (int, float)):
        raise ValueError("a litto3d list needs low_m (IGN69 metres)")
    if p["kind"] == "infomar" and not isinstance(p.get("deep_m"), (int, float)):
        raise ValueError("an infomar list needs deep_m (metres below LAT kept)")
    if p["kind"] == "emodnet":
        b = p.get("boxes")
        if not isinstance(b, list) or not b or any(len(x) != 4 for x in b):
            raise ValueError("an emodnet list needs boxes [[west, south, east, north], ...]")
    return p


def part_of(files, part):
    """The files of part "i/n" (1-based; every n-th file from the i-th, so each part gets a mix of sizes)."""
    i, n = (int(v) for v in part.split("/"))
    if not 1 <= i <= n:
        raise ValueError("part %s: i must be 1..n" % part)
    return files[i - 1::n]


# ------------------------------------------------------------------------------------------------ download

def fetch(url, dest, want_bytes=None, md5=None, sha256=None, timeout=300, log=print):
    """Stream `url` into `dest` (at most MAX_ARCHIVE bytes) and check it against the list. Returns (bytes, sha256)."""
    import ssl
    import urllib.request
    import certifi
    ctx = ssl.create_default_context(cafile=certifi.where())
    req = urllib.request.Request(url, headers={"User-Agent": M.USER_AGENT})
    hs, hm, n = hashlib.sha256(), hashlib.md5(), 0
    t0 = time.time()
    with urllib.request.urlopen(req, context=ctx, timeout=timeout) as r, open(dest, "wb") as out:
        cl = r.headers.get("Content-Length")
        if want_bytes is not None and cl is not None and int(cl) != want_bytes:
            raise ValueError("%s: %s bytes offered, the list says %d" % (url, cl, want_bytes))
        while True:
            b = r.read(CHUNK)
            if not b:
                break
            n += len(b)
            if n > MAX_ARCHIVE:
                raise ValueError("%s: longer than %d bytes" % (url, MAX_ARCHIVE))
            hs.update(b)
            hm.update(b)
            out.write(b)
    if want_bytes is not None and n != want_bytes:
        raise ValueError("%s: %d bytes, the list says %d" % (url, n, want_bytes))
    if md5 and hm.hexdigest() != md5:
        raise ValueError("%s: MD5 %s, the list says %s" % (url, hm.hexdigest(), md5))
    if sha256 and hs.hexdigest() != sha256:
        raise ValueError("%s: SHA-256 %s, the list says %s" % (url, hs.hexdigest()[:16], sha256[:16]))
    log("  %s: %.0f MB in %.0f s" % (url.rsplit("/", 1)[-1], n / 1e6, time.time() - t0))
    return n, hs.hexdigest()


def fetch_retry(url, dest, tries=3, **kw):
    for t in range(tries):
        try:
            return fetch(url, dest, **kw)
        except Exception as e:  # noqa: BLE001: a dropped connection or a short body; a wrong hash fails the same way
            if t + 1 == tries:
                raise
            kw.get("log", print)("  %s: %s; again in %d s" % (url, str(e)[:200], 30 * (t + 1)))
            time.sleep(30 * (t + 1))


# ------------------------------------------------------------------------------------------------ writing

def write_tif(path, a, transform, crs, nodata):
    import rasterio
    from rasterio.enums import Resampling
    prof = dict(PROFILE, width=a.shape[1], height=a.shape[0], transform=transform, crs=crs, nodata=nodata)
    with rasterio.open(path, "w", **prof) as d:
        d.write(a.astype(np.float32), 1)
        levels = [f for f in OVERVIEWS if min(a.shape) // f >= 1]
        if levels:
            d.build_overviews(levels, Resampling.average)
    return path


def zip_one(tif, zpath):
    """A zip (stored: the GeoTIFF is compressed already) holding the one GeoTIFF; returns (bytes, sha256, member)."""
    member = os.path.basename(tif)
    # a fixed date: the same GeoTIFF zips to the same bytes, so a part run again finds its keys already written
    info = zipfile.ZipInfo(member, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_STORED
    info.external_attr = 0o644 << 16
    with zipfile.ZipFile(zpath, "w", zipfile.ZIP_STORED, allowZip64=True) as z, open(tif, "rb") as fi, \
            z.open(info, "w", force_zip64=os.path.getsize(tif) > 2 ** 31) as fo:
        shutil.copyfileobj(fi, fo, CHUNK)
    h = hashlib.sha256()
    with open(zpath, "rb") as f:
        for b in iter(lambda: f.read(CHUNK), b""):
            h.update(b)
    return os.path.getsize(zpath), h.hexdigest(), member


def lonlat_bbox(tif):
    import rasterio
    from rasterio.warp import transform_bounds
    with rasterio.open(tif) as d:
        b = transform_bounds(d.crs, "EPSG:4326", *d.bounds, densify_pts=21)
    return [round(b[0] - 0.0005, 4), round(b[1] - 0.0005, 4), round(b[2] + 0.0005, 4), round(b[3] + 0.0005, 4)]


# ------------------------------------------------------------------------------------------------ litto3d

def read_asc(path):
    """(values float32 with NaN for nodata, left, top, cell) of an ESRI ASCII grid (xll/yll corner or centre), read by
    GDAL's AAIGrid driver (EMODnet's tiles are 600 MB of text)."""
    import rasterio
    with rasterio.open(path) as d:
        if d.driver != "AAIGrid" or d.count != 1:
            raise ValueError("%s: not a one-band ESRI ASCII grid (%s)" % (path, d.driver))
        t = d.transform
        if abs(t.b) > 0 or abs(t.d) > 0 or abs(abs(t.e) - t.a) > 1e-9 * max(1.0, t.a):
            raise ValueError("%s: not a north-up grid of square cells" % path)
        a = d.read(1, out_dtype="float32")
        nd = d.nodata
    if nd is not None:
        a[a == np.float32(nd)] = np.nan
    return a, t.c, t.f, t.a


def unpack_7z(archive, dest, pattern=ASC_RE):
    """The members matching `pattern` written under dest (7-Zip's 7z when installed: fast; else py7zr). Returns their
    paths. Member names are checked: no absolute path, no '..'."""
    names = list_7z(archive)
    want = [n for n in names if pattern.search(n)]
    for n in want:
        if n.startswith(("/", "\\")) or ".." in n.replace("\\", "/").split("/"):
            raise ValueError("%s: unsafe member name %r" % (archive, n))
    if not want:
        return []
    seven = shutil.which("7z") or shutil.which("7za")
    if seven:
        lst = os.path.join(dest, "_want.txt")
        with open(lst, "w", encoding="utf-8") as f:
            f.write("\n".join(want) + "\n")
        subprocess.run([seven, "x", "-y", "-bd", "-o" + dest, archive, "@" + lst], check=True,
                       stdout=subprocess.DEVNULL)
        os.remove(lst)
    else:
        import py7zr
        with py7zr.SevenZipFile(archive) as z:
            z.extract(path=dest, targets=want)
    out = [os.path.join(dest, *n.split("/")) for n in want]
    missing = [p for p in out if not os.path.isfile(p)]
    if missing:
        raise ValueError("%s: %d members not unpacked, e.g. %s" % (archive, len(missing), missing[0]))
    return out


def list_7z(archive):
    seven = shutil.which("7z") or shutil.which("7za")
    if seven:
        r = subprocess.run([seven, "l", "-slt", "-ba", archive], check=True, capture_output=True, text=True,
                           encoding="utf-8", errors="replace")
        names, isdir = [], {}
        cur = None
        for line in r.stdout.splitlines():
            if line.startswith("Path = "):
                cur = line[7:].replace("\\", "/")
                names.append(cur)
            elif line.startswith("Folder = ") and cur is not None:
                isdir[cur] = line[9:].strip() == "+"
            elif line.startswith("Attributes = ") and cur is not None and line[13:].startswith("D"):
                isdir[cur] = True
        return [n for n in names if not isdir.get(n)]
    import py7zr
    with py7zr.SevenZipFile(archive) as z:
        return [f.filename for f in z.list() if not f.is_directory]


def prep_litto3d(arc, work, plist, stem, log=print):
    """One Litto3D pack -> one GeoTIFF of its 1-km tiles holding low ground (or None: dry land only)."""
    from rasterio.transform import from_origin
    x = os.path.join(work, "x_" + stem)
    os.makedirs(x, exist_ok=True)
    try:
        tiles = []
        for p in unpack_7z(arc, x):
            a, left, top, cs = read_asc(p)
            os.remove(p)
            if abs(cs - 1.0) > 1e-6:
                raise ValueError("%s: cell %s m, not 1 m" % (p, cs))
            with np.errstate(invalid="ignore"):
                if np.isfinite(a).any() and (a <= plist["low_m"]).any():
                    tiles.append((a, left, top))
        if not tiles:
            log("  %s: no 1-km tile at or below %g m IGN69: left out" % (stem, plist["low_m"]))
            return None
        left = min(t[1] for t in tiles)
        top = max(t[2] for t in tiles)
        right = max(t[1] + t[0].shape[1] for t in tiles)
        bottom = min(t[2] - t[0].shape[0] for t in tiles)
        w, h = int(round(right - left)), int(round(top - bottom))
        out = np.full((h, w), np.nan, np.float32)
        for a, l, t in tiles:
            r0, c0 = int(round(top - t)), int(round(l - left))
            sub = out[r0:r0 + a.shape[0], c0:c0 + a.shape[1]]
            take = np.isnan(sub) & np.isfinite(a)
            sub[take] = a[take]
        nd = -99999.0
        out[np.isnan(out)] = nd
        tif = os.path.join(work, stem + ".tif")
        write_tif(tif, out, from_origin(left, top, 1.0, 1.0), plist["crs"], nd)
        log("  %s: %d 1-km tiles kept, %d x %d px" % (stem, len(tiles), w, h))
        return tif
    finally:
        shutil.rmtree(x, ignore_errors=True)


# ------------------------------------------------------------------------------------------------ infomar

def zip_tif_member(path):
    """The largest .tif/.tiff member of a zip (a regular file, a safe name)."""
    with zipfile.ZipFile(path) as z:
        tifs = [i for i in z.infolist() if not i.is_dir() and i.filename.lower().endswith((".tif", ".tiff"))]
    if not tifs:
        raise ValueError("%s: no GeoTIFF inside" % path)
    m = max(tifs, key=lambda i: i.file_size)
    if m.filename.startswith(("/", "\\")) or ".." in m.filename.replace("\\", "/").split("/"):
        raise ValueError("%s: unsafe member name %r" % (path, m.filename))
    return m.filename


def unzip_member(arc, member, dest):
    """One member of a zip written to dest, its CRC checked as it is read (7-Zip's 7z when installed: INFOMAR's zips
    are Deflate64, which Python reads slowly; else zipfile, with zipfile_deflate64 when the archive needs it)."""
    with zipfile.ZipFile(arc) as z:
        info = z.getinfo(member)
    seven = shutil.which("7z") or shutil.which("7za")
    if seven:
        tmp = dest + ".d"
        os.makedirs(tmp, exist_ok=True)
        try:
            subprocess.run([seven, "e", "-y", "-bd", "-o" + tmp, arc, member], check=True, stdout=subprocess.DEVNULL)
            got = os.path.join(tmp, os.path.basename(member))
            if os.path.getsize(got) != info.file_size:
                raise ValueError("%s: %s came out %d bytes, the archive says %d" % (arc, member, os.path.getsize(got),
                                                                                   info.file_size))
            os.replace(got, dest)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        return dest
    with zipfile.ZipFile(arc) as z, z.open(member) as fi, open(dest, "wb") as fo:
        shutil.copyfileobj(fi, fo, CHUNK)              # zipfile checks the CRC when the member is read to its end
    return dest


def keep_window(d, keep, rows=1024):
    """(row0, row1, col0, col1) bounding the pixels keep(values) is True for, read in row bands (None: none)."""
    from rasterio.windows import Window
    r0 = r1 = c0 = c1 = None
    for y in range(0, d.height, rows):
        h = min(rows, d.height - y)
        a = d.read(1, window=Window(0, y, d.width, h), masked=False).astype(np.float32)
        m = keep(a)
        if not m.any():
            continue
        rr = np.flatnonzero(m.any(axis=1))
        cc = np.flatnonzero(m.any(axis=0))
        r0 = y + rr[0] if r0 is None else r0
        r1 = y + rr[-1] + 1
        c0 = cc[0] if c0 is None else min(c0, cc[0])
        c1 = cc[-1] + 1 if c1 is None else max(c1, cc[-1] + 1)
    return None if r0 is None else (int(r0), int(r1), int(c0), int(c1))


FIT_PAD = 0.45      # degrees: a prepared grid's lon/lat outline may reach this far past its list box (the UTM rectangle
                    # round a box 11-5.3 W reaches 0.40 past it in the east); the regions' boxes and reach cover it


def cut_rect(plist):
    """(x0, y0, x1, y1) in the list's CRS: the rectangle round the union of the list's lon/lat boxes, or None."""
    if not plist.get("boxes"):
        return None
    from rasterio.warp import transform_bounds
    rs = [transform_bounds("EPSG:4326", plist["crs"], *b, densify_pts=101) for b in plist["boxes"]]
    return (min(r[0] for r in rs), min(r[1] for r in rs), max(r[2] for r in rs), max(r[3] for r in rs))


def check_fit(tif, plist):
    """The prepared grid's lon/lat outline must lie within FIT_PAD of one of the list's boxes (else the build's
    sheets_in_regions would stop on it)."""
    if not plist.get("boxes"):
        return tif
    b = lonlat_bbox(tif)
    if not any(w - FIT_PAD <= b[0] and b[2] <= e + FIT_PAD and s - FIT_PAD <= b[1] and b[3] <= n + FIT_PAD
               for (w, s, e, n) in plist["boxes"]):
        raise ValueError("%s: outline %s reaches past the list's boxes" % (tif, b))
    return tif


def same_horizontal(crs, want):
    """True when `crs` (a rasterio CRS, possibly compound: INFOMAR's grids are "UTM zone 29N + LAT depth") has the
    horizontal CRS `want` ("EPSG:<n>")."""
    from pyproj import CRS
    c = CRS.from_wkt(crs.to_wkt())
    if c.is_compound:
        c = c.sub_crs_list[0]
    return c.to_epsg() == CRS.from_user_input(want).to_epsg()


def prep_infomar(arc, work, plist, stem, log=print):
    """One survey zip -> one GeoTIFF in the list's CRS, pixels deeper than deep_m below LAT dropped, cut to the rest
    and to the rectangle round the list's boxes (cut_rect)."""
    import rasterio
    from rasterio.enums import Resampling
    from rasterio.warp import calculate_default_transform, reproject
    from rasterio.windows import Window
    member = zip_tif_member(arc)
    src = unzip_member(arc, member, os.path.join(work, "in_" + stem + ".tif"))
    nd_out = -9999.0
    deep = -float(plist["deep_m"])
    try:
        with rasterio.open(src) as d:
            if d.count != 1:
                raise ValueError("%s: %d bands" % (member, d.count))
            nd = d.nodata

            def keep(a):
                ok = np.isfinite(a) & (np.abs(a) < 12000.0)
                if nd is not None:
                    ok &= a != np.float32(nd)
                return ok & (a >= deep)

            if d.crs is None:
                raise ValueError("%s: no CRS" % member)
            if not same_horizontal(d.crs, plist["crs"]):
                # another UTM zone or lon/lat: warped to the list's CRS at its own resolution (bilinear)
                tr, w, h = calculate_default_transform(d.crs, plist["crs"], d.width, d.height, *d.bounds)
                a = np.full((h, w), np.nan, np.float32)
                reproject(rasterio.band(d, 1), a, src_nodata=nd, dst_nodata=np.nan, dst_transform=tr,
                          dst_crs=plist["crs"], resampling=Resampling.bilinear)
                ok = keep(a)
                rect = cut_rect(plist)
                if rect is not None:
                    xs = tr.c + (np.arange(w) + 0.5) * tr.a
                    ys = tr.f + (np.arange(h) + 0.5) * tr.e
                    ok &= ((ys >= rect[1]) & (ys <= rect[3]))[:, None] & ((xs >= rect[0]) & (xs <= rect[2]))[None, :]
                if not ok.any():
                    log("  %s: nothing within %g m of LAT: left out" % (stem, plist["deep_m"]))
                    return None
                rr, cc = np.flatnonzero(ok.any(1)), np.flatnonzero(ok.any(0))
                a = np.where(ok, a, nd_out)[rr[0]:rr[-1] + 1, cc[0]:cc[-1] + 1]
                t2 = tr @ tr.translation(cc[0], rr[0])
                tif = os.path.join(work, stem + ".tif")
                write_tif(tif, a, t2, plist["crs"], nd_out)
                log("  %s: warped from %s, %d x %d px" % (stem, d.crs.to_string(), a.shape[1], a.shape[0]))
                return check_fit(tif, plist)
            win = keep_window(d, keep)
            rect = cut_rect(plist)
            if win is not None and rect is not None:
                inv = ~d.transform
                cx0, ry0 = inv @ (rect[0], rect[3])
                cx1, ry1 = inv @ (rect[2], rect[1])
                r0, r1 = max(win[0], int(math.floor(ry0))), min(win[1], int(math.ceil(ry1)))
                c0, c1 = max(win[2], int(math.floor(cx0))), min(win[3], int(math.ceil(cx1)))
                win = (r0, r1, c0, c1) if r0 < r1 and c0 < c1 else None
            if win is None:
                log("  %s: nothing within %g m of LAT: left out" % (stem, plist["deep_m"]))
                return None
            r0, r1, c0, c1 = win
            tr = d.window_transform(Window(c0, r0, c1 - c0, r1 - r0))
            tif = os.path.join(work, stem + ".tif")
            prof = dict(PROFILE, width=c1 - c0, height=r1 - r0, transform=tr, crs=plist["crs"], nodata=nd_out)
            with rasterio.open(tif, "w", **prof) as o:
                for y in range(r0, r1, 1024):
                    h = min(1024, r1 - y)
                    a = d.read(1, window=Window(c0, y, c1 - c0, h)).astype(np.float32)
                    o.write(np.where(keep(a), a, np.float32(nd_out)), 1, window=Window(0, y - r0, c1 - c0, h))
                levels = [f for f in OVERVIEWS if min(r1 - r0, c1 - c0) // f >= 1]
                if levels:
                    o.build_overviews(levels, Resampling.average)
            log("  %s: %d x %d px kept of %d x %d" % (stem, c1 - c0, r1 - r0, d.width, d.height))
            return check_fit(tif, plist)
    finally:
        os.remove(src)


# ------------------------------------------------------------------------------------------------ emodnet

def prep_emodnet(arc, work, plist, stem, log=print):
    """One EMODnet MSL tile -> one GeoTIFF per list box it meets (a list of paths)."""
    from rasterio.transform import from_origin
    with zipfile.ZipFile(arc) as z:
        ascs = [i.filename for i in z.infolist() if i.filename.lower().endswith(".asc") and not i.is_dir()]
    if len(ascs) != 1 or "/" in ascs[0] or "\\" in ascs[0] or ".." in ascs[0]:
        raise ValueError("%s: want one .asc member, have %s" % (arc, ascs))
    src = unzip_member(arc, ascs[0], os.path.join(work, "in_" + stem + ".asc"))
    try:
        a, left, top, cs = read_asc(src)
    finally:
        os.remove(src)
    out = []
    for k, (w, s, e, n) in enumerate(plist["boxes"]):
        c0 = max(0, int(math.floor((w - left) / cs)))
        c1 = min(a.shape[1], int(math.ceil((e - left) / cs)))
        r0 = max(0, int(math.floor((top - n) / cs)))
        r1 = min(a.shape[0], int(math.ceil((top - s) / cs)))
        if c0 >= c1 or r0 >= r1:
            continue
        sub = a[r0:r1, c0:c1]
        if not np.isfinite(sub).any():
            continue
        sub = np.where(np.isfinite(sub), sub, np.float32(plist["nodata"]))
        tif = os.path.join(work, "%s_b%d.tif" % (stem, k))
        write_tif(tif, sub, from_origin(left + c0 * cs, top - r0 * cs, cs, cs), plist["crs"], plist["nodata"])
        log("  %s box %d: %d x %d px" % (stem, k, sub.shape[1], sub.shape[0]))
        out.append(check_fit(tif, plist))
    return out


PREP = {"litto3d": prep_litto3d, "infomar": prep_infomar, "emodnet": prep_emodnet}


# ------------------------------------------------------------------------------------------------ run / merge

def stem_of(f):
    return re.sub(r"[^A-Za-z0-9_.-]", "_", f.get("name") or f["url"].rsplit("/", 1)[-1].split(".")[0])


def run(plist, part, work, store=None, limit=None, log=print, names=None):
    """Prepare (and with a store, upload) every archive of the part (or only those named). Returns the part's rows."""
    files = part_of(plist["files"], part)
    if names:
        files = [f for f in plist["files"] if f.get("name") in names]
        if len(files) != len(set(names)):
            raise ValueError("names not in the list: %s" % sorted(set(names) - {f.get("name") for f in files}))
    if limit:
        files = files[:limit]
    have = set(store.list_keys("%s/%s/" % (M.PREFIX, plist["set"]))) if store else set()
    rows, left_out = [], []
    for k, f in enumerate(files):
        stem = stem_of(f)
        log("%s (%d / %d)" % (stem, k + 1, len(files)))
        arc = os.path.join(work, "arc_" + stem)
        fetch_retry(f["url"], arc, want_bytes=f.get("bytes"), md5=f.get("md5"), sha256=f.get("sha256"), log=log)
        try:
            made = PREP[plist["kind"]](arc, work, plist, stem, log=log)
        finally:
            os.remove(arc)
        made = [] if made is None else (made if isinstance(made, list) else [made])
        if not made:
            left_out.append(f["url"])
        for tif in made:
            zpath = tif[:-4] + ".zip"
            n, sha, member = zip_one(tif, zpath)
            row = dict(url=f["url"], bytes=n, sha256=sha, member=member, bbox=lonlat_bbox(tif))
            for key in ("name", "year", "survey"):
                if f.get(key) is not None:
                    row[key] = f[key]
            os.remove(tif)
            if store is not None:
                key = M.key_of(plist, row)
                if key not in have:
                    with open(zpath, "rb") as fi:
                        store.put(key, fi.read(), "application/zip", M.IMMUTABLE)
                    head = store.client.head_object(Bucket=store.bucket, Key=key)
                    if int(head["ContentLength"]) != n:
                        raise RuntimeError("%s: the bucket holds %s bytes, not %d" % (key, head["ContentLength"], n))
                    have.add(key)
                    log("  -> %s (%.1f MB)" % (key, n / 1e6))
                os.remove(zpath)
            rows.append(row)
    return dict(set=plist["set"], part=part, rows=rows, left_out=left_out)


def merge(plist, parts, order=None):
    """The builder's manifest from the parts' rows, in the prep list's order (INFOMAR: newest survey first)."""
    rows = [r for p in parts for r in p["rows"]]
    pos = {f["url"]: i for i, f in enumerate(plist["files"])}
    rows.sort(key=lambda r: (pos[r["url"]], r["member"]))
    seen = set()
    for r in rows:
        if r["sha256"] in seen:
            raise ValueError("%s: the same prepared archive twice" % r["url"])
        seen.add(r["sha256"])
    want = {f["url"] for f in plist["files"]}
    done = {r["url"] for r in rows} | {u for p in parts for u in p.get("left_out", ())}
    if want - done:
        raise ValueError("%d archives of the list are in no part, e.g. %s" % (len(want - done), sorted(want - done)[0]))
    return dict(set=plist["set"], credit=plist["credit"], licence=plist["licence"], derived=plist["derived"],
                files=rows)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("prep_list")
    r.add_argument("--part", default="1/1")
    r.add_argument("--upload", action="store_true", help="write to the bucket (R2_* environment, as bathy-build)")
    r.add_argument("--limit", type=int)
    r.add_argument("--names", help="only these archives (the list's names, comma-separated): a trial")
    r.add_argument("--work")
    r.add_argument("--out", help="write the part's rows here as JSON too")
    m = sub.add_parser("merge")
    m.add_argument("prep_list")
    m.add_argument("parts", nargs="+", help="part JSON files")
    m.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    plist = load_list(a.prep_list)
    if a.cmd == "merge":
        parts = [json.load(open(p, encoding="utf-8")) for p in a.parts]
        doc = merge(plist, parts)
        with open(a.out, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=1)
        M.load(a.out)
        print("%s: %d prepared archives, %.1f MB" % (a.out, len(doc["files"]), sum(x["bytes"] for x in doc["files"]) / 1e6))
        return 0
    store = None
    if a.upload:
        sys.path.insert(0, os.path.join(os.path.dirname(HERE), "model_frames"))
        import publish as P
        store = P.Store(P.r2_client(os.environ["R2_ACCOUNT_ID"], os.environ["R2_ACCESS_KEY_ID"],
                                    os.environ["R2_SECRET_ACCESS_KEY"]), os.environ["R2_BUCKET"])
    work = a.work or tempfile.mkdtemp(prefix="prep_")
    os.makedirs(work, exist_ok=True)
    if a.names and store is not None:
        raise SystemExit("--names is a trial: no --upload with it")
    doc = run(plist, a.part, work, store, a.limit, names=a.names.split(",") if a.names else None)
    text = json.dumps(doc, indent=1)
    if a.out:
        with open(a.out, "w", encoding="utf-8") as f:
            f.write(text)
    if store is not None:
        i, n = a.part.split("/")
        store.put("%s/%s/parts/%s-of-%s.json" % (M.PREFIX, plist["set"], i, n), text.encode("utf-8"),
                  "application/json", "no-cache")
        rk = "%s/%s/README.txt" % (M.PREFIX, plist["set"])
        if rk not in set(store.list_keys("%s/%s/" % (M.PREFIX, plist["set"]))):
            store.put(rk, M.readme(dict(plist, files=[])).encode("utf-8"), "text/plain; charset=utf-8", M.IMMUTABLE)
    print("part %s: %d prepared, %d left out" % (a.part, len(doc["rows"]), len(doc["left_out"])))
    return 0


if __name__ == "__main__":
    sys.exit(main())
