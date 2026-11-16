"""NOAA BlueTopo (plan section 98): the tile scheme, read to find the delivered tiles of one resolution inside a box.

BlueTopo is NOAA Office of Coast Survey's national bathymetric compilation (CC0), on the open bucket
noaa-ocs-nationalbathymetry-pds. Each delivered tile is a GeoTIFF in its UTM zone (EPSG:269xx), three bands: elevation
(metres, NAVD88 or MLLW-referenced compilation as NOAA states per tile; NaN where none), uncertainty, contributor. The
scheme is a GeoPackage under BlueTopo/_BlueTopo_Tile_Scheme/, one row per tile: the tile name, its lon/lat outline
(EPSG:4326), the GeoTIFF link (empty where NOAA has not delivered the tile yet), the resolution class ("4m", "8m",
"16m", ...) and the UTM zone. A new scheme replaces the old one when NOAA delivers tiles; the newest is used and named
in the build's log.

No GDAL here: the scheme is read with sqlite3 (a GeoPackage is SQLite), each outline's envelope from the GeoPackage
geometry header (no WKB parse needed).
"""
import re
import sqlite3
import struct
import xml.etree.ElementTree as ET

BUCKET = "noaa-ocs-nationalbathymetry-pds"
HOST = "https://%s.s3.amazonaws.com/" % BUCKET
SCHEME_PREFIX = "BlueTopo/_BlueTopo_Tile_Scheme/"
SCHEME_RE = re.compile(r"^BlueTopo/_BlueTopo_Tile_Scheme/BlueTopo_Tile_Scheme_(\d{8}_\d{6})\.gpkg$")
LINK_RE = re.compile(r"^%s(BlueTopo/([A-Z0-9]{6,12})/BlueTopo_\2_\d{8}\.tiff?)$" % re.escape(HOST))
RES_RE = re.compile(r"^(\d{1,3})m$")
MAX_SCHEME_BYTES = 64 * 1024 * 1024
MAX_LISTING_BYTES = 1024 * 1024


def listing_url():
    return HOST + "?list-type=2&prefix=" + SCHEME_PREFIX


def newest_scheme(xml_text):
    """The key of the newest tile scheme in an S3 listing (by the date in its name); ValueError when none."""
    root = ET.fromstring(xml_text)
    keys = [el.text for el in root.iter() if el.tag.rsplit("}", 1)[-1] == "Key" and el.text]
    found = sorted((m.group(1), k) for k in keys for m in [SCHEME_RE.match(k)] if m)
    if not found:
        raise ValueError("no BlueTopo tile scheme in the listing of %s" % SCHEME_PREFIX)
    return found[-1][1]


def fetch_scheme(dest, get_bytes):
    """Download the newest tile scheme to `dest`; returns its key. get_bytes(url) -> bytes."""
    listing = get_bytes(listing_url())
    if len(listing) > MAX_LISTING_BYTES:
        raise ValueError("the BlueTopo scheme listing is %d bytes (more than %d)" % (len(listing), MAX_LISTING_BYTES))
    key = newest_scheme(listing.decode("utf-8"))
    data = get_bytes(HOST + key)
    if not 1024 < len(data) <= MAX_SCHEME_BYTES or not data.startswith(b"SQLite format 3\x00"):
        raise ValueError("%s is not a GeoPackage of a sane size (%d bytes)" % (key, len(data)))
    with open(dest, "wb") as f:
        f.write(data)
    return key


def envelope(blob):
    """(lon0, lat0, lon1, lat1) from a GeoPackage geometry blob's header. Refuses anything but the standard binary
    header with an xy envelope in EPSG:4326 (the scheme's own form)."""
    if not isinstance(blob, (bytes, bytearray)) or len(blob) < 40 or blob[:2] != b"GP" or blob[2] != 0:
        raise ValueError("not a GeoPackage geometry")
    flags = blob[3]
    little = flags & 1
    if (flags >> 1) & 7 != 1:
        raise ValueError("geometry without an xy envelope (flags %d)" % flags)
    order = "<" if little else ">"
    srs = struct.unpack(order + "i", bytes(blob[4:8]))[0]
    if srs != 4326:
        raise ValueError("geometry in srs %d, not 4326" % srs)
    x0, x1, y0, y1 = struct.unpack(order + "4d", bytes(blob[8:40]))
    if not (-180.0 <= x0 < x1 <= 180.0 and -90.0 <= y0 < y1 <= 90.0):
        raise ValueError("envelope out of range: %r" % ((x0, x1, y0, y1),))
    return x0, y0, x1, y1


def gdal_path(link):
    """/vsis3/ path of a delivered tile's link; ValueError for a link not in BlueTopo's form."""
    m = LINK_RE.match(link or "")
    if not m:
        raise ValueError("not a BlueTopo tile link: %r" % (link,))
    return "/vsis3/%s/%s" % (BUCKET, m.group(1))


def read_scheme(path):
    """[(tile, (lon0, lat0, lon1, lat1), resolution m, gdal path)] of every DELIVERED tile (a link given). A row whose
    link names another tile, or a resolution not "<n>m", is refused (a changed scheme must stop the build)."""
    con = sqlite3.connect("file:%s?mode=ro" % path, uri=True)
    try:
        tables = [r[0] for r in con.execute("select table_name from gpkg_contents where data_type = 'features'")]
        if len(tables) != 1:
            raise ValueError("BlueTopo scheme: %d feature tables, expected 1" % len(tables))
        out = []
        q = 'select tile, geom, GeoTIFF_Link, Resolution from "%s"' % tables[0].replace('"', '""')
        for tile, geom, link, res in con.execute(q):
            if not link:
                continue
            p = gdal_path(link)
            if "/%s/" % tile not in p:
                raise ValueError("BlueTopo scheme: tile %s links to %s" % (tile, link))
            m = RES_RE.match(res or "")
            if not m:
                raise ValueError("BlueTopo scheme: tile %s has resolution %r" % (tile, res))
            out.append((tile, envelope(geom), int(m.group(1)), p))
    finally:
        con.close()
    if not out:
        raise ValueError("BlueTopo scheme: no delivered tile")
    return out


def tiles_in(scheme, box, res_m):
    """[(gdal path, lon/lat rectangle)] of the scheme's tiles of resolution res_m (one number or several) meeting the
    lon/lat box, finest resolution first (within a source the first tile with a value wins)."""
    want = {res_m} if isinstance(res_m, int) else set(res_m)
    hit = [(res, p, r) for (_, r, res, p) in scheme
           if res in want and r[0] < box[2] and box[0] < r[2] and r[1] < box[3] and box[1] < r[3]]
    return [(p, r) for (_, p, r) in sorted(hit)]
