"""A pinned STAC collection of cloud-optimised GeoTIFF sheets (plan section 111a: LINZ's New Zealand Coastal LiDAR 1m DEM).

The collection document lists its items (one per map sheet) with each item document's checksum ("file:checksum", a
multihash: "1220" + the SHA-256 in hex). A source pins the collection's id, its item count and a digest of the item list
(the SHA-256 of the sorted "<href> <checksum>" lines), so a sheet added, removed or changed stops the build until the
pin is renewed (the error names the new digest). Each item document is fetched and checked against its listed checksum;
its lon/lat bbox is the sheet's footprint and its "visual" asset (a relative "./<sheet>.tiff") the GeoTIFF read by
window over https (/vsicurl/). No GDAL here.
"""
import hashlib
import json
import re
from concurrent.futures import ThreadPoolExecutor

SHA_RE = re.compile(r"^[0-9a-f]{64}$")
ITEM_RE = re.compile(r"^\./([A-Z0-9]{2,8})\.json$")
ASSET_RE = re.compile(r"^\./([A-Z0-9]{2,8})\.tiff$")
MAX_DOC = 4 * 1024 * 1024


def _json(raw, what):
    if len(raw) > MAX_DOC:
        raise ValueError("%s: %d bytes (more than %d)" % (what, len(raw), MAX_DOC))
    return json.loads(raw.decode("utf-8"))


def item_links(col):
    """[(href, checksum)] of the collection's item links."""
    return [(l.get("href"), l.get("file:checksum")) for l in col.get("links") or () if l.get("rel") == "item"]


def list_digest(links):
    return hashlib.sha256("\n".join(sorted("%s %s" % (h, c) for h, c in links)).encode("utf-8")).hexdigest()


def read_items(spec, get_bytes, workers=8):
    """[(sheet, (lon0, lat0, lon1, lat1), gdal path)] of every item of the pinned collection `spec` (collection URL, id,
    items, digest), sorted by sheet. get_bytes(url) -> bytes. Raises ValueError on any departure from the pin."""
    url = spec["collection"]
    if not url.startswith("https://") or not url.endswith("/collection.json"):
        raise ValueError("stac: %s is not an https collection.json" % url)
    base = url[: -len("collection.json")]
    col = _json(get_bytes(url), url)
    if col.get("type") != "Collection" or col.get("id") != spec["id"]:
        raise ValueError("stac: %s is collection %r, not the pinned %s" % (url, col.get("id"), spec["id"]))
    links = item_links(col)
    got = list_digest(links)
    if len(links) != spec["items"] or got != spec["digest"]:
        raise ValueError("stac: %s lists %d items (digest %s), not the pinned %d (%s): renew the pin after checking "
                         "what changed" % (url, len(links), got, spec["items"], str(spec["digest"])[:16]))
    for href, checksum in links:
        if not ITEM_RE.match(href or "") or not re.match(r"^1220[0-9a-f]{64}$", str(checksum)):
            raise ValueError("stac: item link %r / checksum %r not in the expected form" % (href, checksum))
    with ThreadPoolExecutor(workers) as ex:
        raws = list(ex.map(lambda hc: get_bytes(base + hc[0][2:]), sorted(links)))
    out = []
    for (href, checksum), raw in zip(sorted(links), raws):
        m = ITEM_RE.match(href)
        if "1220" + hashlib.sha256(raw).hexdigest() != checksum:
            raise ValueError("stac: %s does not match its listed checksum" % href)
        it = _json(raw, href)
        a = ((it.get("assets") or {}).get("visual") or {}).get("href")
        am = ASSET_RE.match(a or "")
        bbox = it.get("bbox")
        if it.get("id") != m.group(1) or not am or am.group(1) != m.group(1):
            raise ValueError("stac: %s names sheet %r / asset %r" % (href, it.get("id"), a))
        if not (isinstance(bbox, list) and len(bbox) == 4 and all(isinstance(v, (int, float)) for v in bbox)
                and -180 <= bbox[0] < bbox[2] <= 180 and -90 <= bbox[1] < bbox[3] <= 90):
            raise ValueError("stac: %s has bbox %r" % (href, bbox))
        out.append((it["id"], tuple(float(v) for v in bbox), "/vsicurl/" + base + a[2:]))
    return out
