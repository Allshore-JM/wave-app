"""Country names for the map's wide zooms (plan section 114), built from Natural Earth's admin-0 countries.

Writes static_ui/countries.json, a small UI asset the page serves beside places.js (no bucket prefix):

  {"format": "countries-v1", "source": {...}, "license": ..., "rules": ..., "count": N,
   "countries": [[label, lat, lon, start10, end10, size], ...]}

  label    the name drawn (capitals are the client's): Natural Earth's NAME, or NAME_EN where NAME is abbreviated
           (NAME_FIXES after that), then the owner's short form for long names (SHORT_FORMS); a label still longer than
           WRAP_CHARS characters is cut onto two lines ("\\n") at the space nearest its middle
  lat/lon  Natural Earth's label point (LABEL_Y / LABEL_X), degrees, 3 decimals
  start10  the map zoom (x 10) from which the name shows: by land area (AREA_TIERS), the open-ocean island nations of
           ISLANDS from ISLAND_ZOOM10 (owner, 2026-11-27: "land area + islands")
  end10    the map zoom (x 10) from which it has gone: once the country is about END_PX across on screen (the square
           root of its area at its label latitude), at least MIN_SPAN10 after start10, never after END_CAP10
  size     0-4, the font group (the page's .pc-0 ... .pc-4): the area tier; island nations ISLAND_SIZE

Records are sorted by start10, then area (largest first), then label: the order the client places them in.
Areas are measured on the sphere (R = 6371.0088 km) from the polygons, holes subtracted.

Usage:
  python tools/places/build_countries.py --ne ne_10m_admin_0_countries.geojson --out static_ui/countries.json
  python tools/places/build_countries.py --check static_ui/countries.json [--ne ne_10m_admin_0_countries.geojson]
         (with --ne: the file must equal a rebuild byte for byte)
The source file: https://raw.githubusercontent.com/nvkelso/natural-earth-vector/<NE_COMMIT>/geojson/<NE_FILE>
"""
import argparse
import hashlib
import json
import math
import sys

FORMAT = "countries-v1"
NE_COMMIT = "ca96624a56bd078437bca8184e78163e5039ad19"   # = build_places.NE_COMMIT (a test holds them equal)
NE_FILE = "ne_10m_admin_0_countries.geojson"
NE_SHA256 = "239eec57ac17f100a11e2536cffc56752c318b50ae765b0918ff7aab4ce8f255"
R_KM = 6371.0088
M_PER_PX_Z0 = 156543.03392804097                    # Web Mercator metres per pixel at zoom 0 on the equator (256-px tiles)

# (minimum land area km2, start zoom x 10): the largest countries at world view, smaller ones a little closer
AREA_TIERS = ((1_500_000, 15), (500_000, 25), (100_000, 33), (20_000, 40), (0, 47))
ISLAND_ZOOM10, ISLAND_SIZE = 30, 3
END_PX, MIN_SPAN10, END_CAP10 = 900, 15, 70
WRAP_CHARS = 16

# Left out: leases and bases, the buffer zone, uninhabited banks, reefs and ice, and NE features whose names are wrong
# or that duplicate a larger label (Spratly Is.: NAME_EN says "Wake Island"). Keys are Natural Earth's NAME.
SKIP_TYPES = frozenset(("Lease",))
SKIP = frozenset(("Akrotiri", "Dhekelia", "Cyprus U.N. Buffer Zone", "Bajo Nuevo Bank", "Serranilla Bank",
                  "Scarborough Reef", "Brazilian I.", "Bir Tawil", "Southern Patagonian Ice Field", "Siachen Glacier",
                  "Ashmore and Cartier Is.", "Coral Sea Is.", "Spratly Is.", "Antarctica", "U.S. Minor Outlying Is.",
                  "Indian Ocean Ter.", "Clipperton I."))
NAME_FIXES = {"N. Cyprus": "Northern Cyprus", "eSwatini": "Eswatini"}     # NAME -> label (NAME_EN is wrong or too long)
# The open-ocean island nations shown from ocean views (owner, 2026-11-27). Keys are labels before SHORT_FORMS.
ISLANDS = frozenset(("Fiji", "Samoa", "Tonga", "French Polynesia", "Cook Islands", "Vanuatu", "Solomon Islands",
                     "New Caledonia", "Kiribati", "Marshall Islands", "Micronesia", "Palau", "Tuvalu", "Nauru", "Guam",
                     "Northern Mariana Islands", "American Samoa", "Wallis and Futuna", "Niue", "Pitcairn Islands",
                     "Maldives", "Seychelles", "Mauritius", "Cabo Verde", "Bahamas", "Bermuda", "Jamaica",
                     "Puerto Rico", "Barbados", "Comoros", "São Tomé and Principe", "Faroe Islands"))
# Long names take a common short form (owner, 2026-11-27: "use common abbreviations for long names").
SHORT_FORMS = {
    "United States of America": "USA", "Democratic Republic of the Congo": "DR Congo",
    "Central African Republic": "Central African Rep.", "United Arab Emirates": "UAE",
    "Bosnia and Herzegovina": "Bosnia & Herz.", "Dominican Republic": "Dominican Rep.",
    "Trinidad and Tobago": "Trinidad & Tobago", "Antigua and Barbuda": "Antigua & Barbuda",
    "Saint Kitts and Nevis": "St. Kitts & Nevis", "Saint Vincent and the Grenadines": "St. Vincent & Gren.",
    "São Tomé and Principe": "São Tomé & Príncipe", "Turks and Caicos Islands": "Turks & Caicos",
    "Saint Pierre and Miquelon": "St. Pierre & Miquelon", "Northern Mariana Islands": "N. Mariana Is.",
    "United States Virgin Islands": "US Virgin Is.", "British Virgin Islands": "British Virgin Is.",
    "British Indian Ocean Territory": "Br. Indian Ocean Ter.",
    "South Georgia and the South Sandwich Islands": "S. Georgia & S. Sandwich Is.",
    "French Southern and Antarctic Lands": "Fr. Southern & Antarctic Lands",
    "Heard Island and McDonald Islands": "Heard & McDonald Is.",
}
RULES = ("area tiers %s; islands %d/size %d; end %dpx min %d cap %d; wrap %d; skip types %s"
         % (",".join("%d:%d" % t for t in AREA_TIERS), ISLAND_ZOOM10, ISLAND_SIZE, END_PX, MIN_SPAN10, END_CAP10,
            WRAP_CHARS, ",".join(sorted(SKIP_TYPES))))
COUNT_EXPECT = 239
# (label, start10, end10 range (lo, hi) or None): landmarks a build from the pinned file must reproduce
PROBES = [("USA", 15, (45, 55)), ("Russia", 15, (40, 45)), ("Canada", 15, None), ("China", 15, None),
          ("Spain", 25, (70, 70)), ("France", 25, None), ("Portugal", 40, (70, 70)), ("Fiji", 30, (70, 70)),
          ("Samoa", 30, None), ("French Polynesia", 30, None), ("Micronesia", 30, None), ("DR Congo", 15, None),
          ("United Kingdom", 33, None), ("New Zealand", 33, None), ("Japan", 33, None), ("Luxembourg", 47, None)]


def log(*a):
    print(*a, file=sys.stderr)


def ring_area_km2(ring):
    """A lon/lat ring's area on the sphere (km2), always >= 0."""
    a, n = 0.0, len(ring)
    for i in range(n):
        x1, y1 = ring[i][0], ring[i][1]
        x2, y2 = ring[(i + 1) % n][0], ring[(i + 1) % n][1]
        a += math.radians(x2 - x1) * (2 + math.sin(math.radians(y1)) + math.sin(math.radians(y2)))
    return abs(a * R_KM * R_KM / 2.0)


def geometry_area_km2(geom):
    polys = geom["coordinates"] if geom["type"] == "MultiPolygon" else [geom["coordinates"]]
    return sum(ring_area_km2(p[0]) - sum(ring_area_km2(h) for h in p[1:]) for p in polys)


def base_name(p):
    """NE NAME, or NAME_EN where NAME is abbreviated (has a '.'); NAME_FIXES first."""
    nm = p["NAME"]
    if nm in NAME_FIXES:
        return NAME_FIXES[nm]
    return nm if "." not in nm else (p.get("NAME_EN") or nm)


def wrap(label):
    """A label over WRAP_CHARS characters with a space -> two lines at the space nearest its middle."""
    if len(label) <= WRAP_CHARS or " " not in label:
        return label
    mid = len(label) / 2.0
    cut = min((i for i, ch in enumerate(label) if ch == " "), key=lambda i: (abs(i - mid), i))
    return label[:cut] + "\n" + label[cut + 1:]


def start10_of(area, island):
    for lo, z10 in AREA_TIERS:
        if area >= lo:
            break
    tier = [t[1] for t in AREA_TIERS].index(z10)
    if island and z10 > ISLAND_ZOOM10:
        return ISLAND_ZOOM10, ISLAND_SIZE
    return z10, tier


def end10_of(area, lat, start10):
    """The zoom (x 10, whole) from which the country is about END_PX across: sqrt(area) / (m per px) = END_PX."""
    across_m = math.sqrt(max(area, 1.0)) * 1000.0
    z = math.log2(END_PX * M_PER_PX_Z0 * math.cos(math.radians(lat)) / across_m)
    return int(min(END_CAP10, max(math.floor(z * 10 + 1e-9), start10 + MIN_SPAN10)))


def build(src_bytes):
    d = json.loads(src_bytes.decode("utf-8"))
    rows, used_islands, used_short = [], set(), set()
    for f in d["features"]:
        p = f["properties"]
        if p["TYPE"] in SKIP_TYPES or p["NAME"] in SKIP:
            continue
        name = base_name(p)
        area = geometry_area_km2(f["geometry"])
        island = name in ISLANDS
        if island:
            used_islands.add(name)
        start10, size = start10_of(area, island)
        lat, lon = float(p["LABEL_Y"]), float(p["LABEL_X"])
        label = SHORT_FORMS.get(name, name)
        if name in SHORT_FORMS:
            used_short.add(name)
        rows.append((start10, -area, wrap(label), round(lat, 3), round(lon, 3), end10_of(area, lat, start10), size))
    missing = (ISLANDS - used_islands) | (set(SHORT_FORMS) - used_short)
    if missing:
        raise ValueError("hand-list names that match no country: %s" % ", ".join(sorted(missing)))
    rows.sort(key=lambda r: (r[0], r[1], r[2]))
    out = {"format": FORMAT,
           "source": {"natural_earth": {"commit": NE_COMMIT, "file": NE_FILE, "sha256": hashlib.sha256(src_bytes).hexdigest()}},
           "license": "Natural Earth: public domain", "rules": RULES, "count": len(rows)}
    head = json.dumps(out, ensure_ascii=False, separators=(",", ":"))[:-1]
    body = ",\n".join(json.dumps([r[2], r[3], r[4], r[0], r[5], r[6]], ensure_ascii=False, separators=(",", ":")) for r in rows)
    return (head + ',"countries":[\n' + body + "\n]}\n").encode("utf-8")


def check(data, real=True):
    """Problems of a countries file (a list of strings; empty = good). real: the pinned build's counts and probes."""
    problems = []
    try:
        d = json.loads(data.decode("utf-8"))
    except ValueError as exc:
        return ["not JSON: %s" % exc]
    if d.get("format") != FORMAT:
        problems.append("format %r" % d.get("format"))
    rows = d.get("countries") or []
    if d.get("count") != len(rows):
        problems.append("count %r != %d records" % (d.get("count"), len(rows)))
    prev = None
    for k, r in enumerate(rows):
        if not (isinstance(r, list) and len(r) == 6):
            problems.append("record %d: not 6 values" % k)
            continue
        label, lat, lon, s10, e10, size = r
        if not isinstance(label, str) or not label.strip() or label.count("\n") > 1:
            problems.append("record %d: bad label %r" % (k, label))
        elif any(len(line) > WRAP_CHARS and " " in line for line in label.split("\n")) and "\n" not in label:
            problems.append("record %d (%s): long label not wrapped" % (k, label))
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            problems.append("record %d (%s): position out of range" % (k, label))
        if not (10 <= s10 < e10 <= END_CAP10) or e10 - s10 < MIN_SPAN10:
            problems.append("record %d (%s): zooms %r..%r out of rule" % (k, label, s10, e10))
        if size not in (0, 1, 2, 3, 4):
            problems.append("record %d (%s): size %r" % (k, label, size))
        if prev is not None and s10 < prev:
            problems.append("record %d (%s): not sorted by start" % (k, label))
        prev = s10
    if real:
        src = (d.get("source") or {}).get("natural_earth") or {}
        if src.get("commit") != NE_COMMIT or src.get("sha256") != NE_SHA256:
            problems.append("source is not the pinned Natural Earth file")
        if len(rows) != COUNT_EXPECT:
            problems.append("%d records, expected %d" % (len(rows), COUNT_EXPECT))
        by = {r[0]: r for r in rows if isinstance(r, list) and r}
        for label, s10, erange in PROBES:
            r = by.get(label)
            if r is None:
                problems.append("probe: no %r" % label)
                continue
            if r[3] != s10:
                problems.append("probe: %s starts at %d, expected %d" % (label, r[3], s10))
            if erange and not (erange[0] <= r[4] <= erange[1]):
                problems.append("probe: %s ends at %d, expected %d..%d" % (label, r[4], erange[0], erange[1]))
        for long_name in SHORT_FORMS:
            if long_name in by:
                problems.append("probe: %r kept its long name" % long_name)
    return problems


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--ne", help="ne_10m_admin_0_countries.geojson (the pinned file)")
    ap.add_argument("--out", help="write the countries file here")
    ap.add_argument("--check", help="check this countries file")
    a = ap.parse_args(argv)
    src = None
    if a.ne:
        src = open(a.ne, "rb").read()
        sha = hashlib.sha256(src).hexdigest()
        if sha != NE_SHA256:
            log("refusing: %s has sha256 %s, not the pinned %s" % (a.ne, sha, NE_SHA256))
            return 2
    if a.out:
        if src is None:
            ap.error("--out needs --ne")
        data = build(src)
        problems = check(data)
        if problems:
            log("\n".join(problems))
            return 1
        with open(a.out, "wb") as fh:
            fh.write(data)
        log("wrote %s: %d B, %d countries" % (a.out, len(data), json.loads(data)["count"]))
    if a.check:
        data = open(a.check, "rb").read()
        problems = check(data)
        if src is not None and data != build(src):
            problems.append("%s differs from a rebuild of the pinned file" % a.check)
        log("check %s: %d B; rebuild compared: %s; %d problems" % (a.check, len(data), "yes" if src else "no", len(problems)))
        for p in problems:
            log("  " + p)
        return 1 if problems else 0
    if not a.out and not a.check:
        ap.error("nothing to do: give --out or --check")
    return 0


if __name__ == "__main__":
    sys.exit(main())
