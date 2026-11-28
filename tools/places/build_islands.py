"""Island names for the map (plan section 115), built from Natural Earth's geography regions (Island, Island group).

Writes static_ui/islands.json, a small UI asset the page serves beside countries.json (no bucket prefix):

  {"format": "islands-v1", "source": {...}, "license": ..., "rules": ..., "count": N,
   "islands": [[label, lat, lon, start10, end10, kind], ...]}

  label    the English name (Natural Earth's NAME_EN, else NAME), then NAME_FIXES: the formal spelling with its local
           diacritics where the formal English name has them (Hawaiian ʻokina and kahakō: Oʻahu, Lānaʻi; Honshū;
           Galápagos), Natural Earth's wrong or odd English names corrected; a label longer than WRAP_CHARS is cut onto
           two lines as the country names are
  lat/lon  the label point: the pole of inaccessibility of the feature's largest polygon (so a crescent island's name
           sits on land; a group's polygon is its hull), degrees, 3 decimals
  start10  the map zoom (x 10, absolute) from which the name shows: Natural Earth's MIN_LABEL (owner, 2026-11-28)
  end10    the map zoom (x 10) from which it has gone: once the island (or the group's hull) is about END_PX across on
           screen, at least MIN_SPAN10 after it came, never after END_CAP10
  kind     0 an island group (drawn in letter-spaced capitals), 1 an island (owner: white italic for both)

Left out (owner, 2026-11-28): a name that repeats a country label (static_ui/countries.json, or ALIAS_DUPES), the
ocean-wide regions (SKIP), and features that are not islands (NOT_ISLANDS). Kahoʻolawe, missing from Natural Earth, is
added (ADD). Two records with one label within DUPE_KM of each other are one (the larger is kept).

Records are sorted by start10, then area (largest first), then label.

Usage:
  python tools/places/build_islands.py --ne ne_10m_geography_regions_polys.geojson --out static_ui/islands.json
  python tools/places/build_islands.py --check static_ui/islands.json [--ne ne_10m_geography_regions_polys.geojson]
         (with --ne: the file must equal a rebuild byte for byte)
The source file: https://raw.githubusercontent.com/nvkelso/natural-earth-vector/<NE_COMMIT>/geojson/<NE_FILE>
"""
import argparse
import hashlib
import heapq
import json
import math
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from build_countries import NE_COMMIT, M_PER_PX_Z0, END_PX, MIN_SPAN10, WRAP_CHARS, geometry_area_km2, wrap  # noqa: E402

FORMAT = "islands-v1"
NE_FILE = "ne_10m_geography_regions_polys.geojson"
NE_SHA256 = "b7b26e50ea917d3696aec87f932def2bf5f890f5770e441d59c162c6f4c92a77"
COUNTRIES = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "static_ui", "countries.json")
CLASSES = ("Island", "Island group")
END_CAP10 = 120                                      # never after zoom 12 (a tiny atoll would otherwise stay to 15)
DUPE_KM = 500

# Ocean-wide regions (owner, 2026-11-28) and NE features that are not islands. Keys are Natural Earth's NAME.
SKIP = frozenset(("POLYNESIA", "MELANESIA", "MICRONESIA", "WEST INDIES", "MALAY ARCHIPELAGO", "BRITISH ISLES",
                  "ARCTIC ARCHIPELAGO"))
NOT_ISLANDS = frozenset(("LABRADOR", "Nova Scotia", "Darien Gap", "Pelopónnisos"))
# Island groups that are a country under another spelling (the label rule catches the exact repeats).
ALIAS_DUPES = frozenset(("ILHAS DE CABO VERDE", "Maldive Islands", "BAHAMA ISLANDS", "Mauritius"))
# English label -> the formal name (diacritics, Natural Earth's wrong or odd NAME_EN). Keys are the label before fixes.
NAME_FIXES = {
    # Hawaiʻi (U+02BB ʻokina, kahakō)
    "Hawaii": "Hawaiʻi", "Oahu": "Oʻahu", "Kauai": "Kauaʻi", "Lanai": "Lānaʻi",
    # Samoa
    "Savai'i": "Savaiʻi",
    # Japan
    "Honshu": "Honshū", "Kyushu": "Kyūshū", "Ryukyu Islands": "Ryūkyū Islands",
    # local diacritics in the English name
    "Galapagos Islands": "Galápagos Islands",
    # Natural Earth's NAME_EN wrong or odd
    "Mainland Tasmania": "Tasmania", "Channel Islands of California": "Channel Islands",
    "Hiva-Oa": "Hiva Oa", "Ua Pu": "Ua Pou", "Tuamotus": "Tuamotu Archipelago", "Chuk-to": "Jeju Island",
    "Ryan Bullard": "Utupua", "Billiton": "Belitung", "Kolepom": "Yos Sudarso Island",
    "Aru Islands Regency": "Aru Islands", "Babuyan Island": "Babuyan Islands", "Whitsunday Island": "Whitsunday Islands",
    "Zanzibar Archipelago": "Zanzibar Island", "Disko Island": "Qeqertarsuaq", "Chuuk Lagoon": "Chuuk",
    "Stewart Island / Rakiura": "Stewart Island/Rakiura",            # the New Zealand Gazetteer form
    "Isla Salas y Gómez": "Salas y Gómez Island",
    # G81 A-4: the English or formal form of a transliterated or abbreviated name
    "Zemlya Georga": "George Land", "Arkticheskiy Institut Islands": "Arctic Institute Islands",
    "Iwo Jima": "Iōtō",                                                # the official name since 2007 (as Honshū)
    "St. Lawrence Island": "Saint Lawrence Island", "St. Matthew Island": "Saint Matthew Island",   # GNIS
}
# The one feature whose NAME_EN names another place: Natural Earth's "Bear Is." (Medvezhyi) says "Batu Islands".
NAME_EN_FIXES = {"Bear Is.": "Bear Islands"}
# Missing from Natural Earth (owner, 2026-11-28): (label, lat, lon, start10, area km2)
ADD = (("Kahoʻolawe", 20.55, -156.61, 54, 116.0),)
# A group is a name that says so; the rest are single islands (Natural Earth files Maui, Tahiti, Viti Levu ... as groups).
GROUP_RE = re.compile(r"\b(Islands|Isles|Archipelago|Group|Chain|Antilles|Shoals)\b")
GROUP_NAMES = frozenset(("Svalbard", "Novaya Zemlya", "Severnaya Zemlya", "Franz Josef Land", "Cyclades",
                         "Outer Hebrides", "Tokelau", "Chuuk", "Azores", "Yap", "New Georgia", "Lofoten", "Vesterålen", "Wallis",
                         "Maluku Islands"))
RULES = ("NE MIN_LABEL start; end %dpx min %d cap %d; wrap %d; dupes within %d km" % (END_PX, MIN_SPAN10, END_CAP10,
                                                                                      WRAP_CHARS, DUPE_KM))
# (label, kind, start10, end10 range or None): landmarks a build from the pinned file must reproduce
PROBES = [("Hawaiian Islands", 0, 30, (65, 75)), ("Oʻahu", 1, 53, (110, 120)), ("Kahoʻolawe", 1, 54, None),
          ("Maui", 1, 40, None), ("Hawaiʻi", 1, 30, None), ("Lānaʻi", 1, 54, None), ("Sicily", 1, 40, None),
          ("Galápagos Islands", 0, 40, None), ("Canary Islands", 0, 40, None), ("Azores", 0, 40, None), ("Bali", 1, 54, None),
          ("Honshū", 1, 40, None), ("Tahiti", 1, 40, None)]
ABSENT = ("Polynesia", "Melanesia", "West Indies", "British Isles", "Japan", "Cuba", "Fiji", "Cape Verde", "Labrador",
          "Oahu", "Hawaii", "Chuk-to")
COUNT_RANGE = (400, 430)


def log(*a):
    print(*a, file=sys.stderr)


def country_labels(path=COUNTRIES):
    """The country labels (unwrapped, lower case) the islands must not repeat."""
    d = json.load(open(path, encoding="utf-8"))
    return {r[0].replace("\n", " ").lower() for r in d["countries"]}


def kind_of(label):
    return 0 if (GROUP_RE.search(label) or label in GROUP_NAMES) else 1


def _ring_dist(x, y, rings):
    """Signed distance from (x, y) to the polygon's rings (positive inside), in the projected plane."""
    inside, best = False, float("inf")
    for ring in rings:
        n = len(ring)
        j = n - 1
        for i in range(n):
            ax, ay = ring[i]
            bx, by = ring[j]
            if (ay > y) != (by > y) and x < (bx - ax) * (y - ay) / (by - ay) + ax:
                inside = not inside
            dx, dy = bx - ax, by - ay
            t = 0.0 if dx == 0 and dy == 0 else max(0.0, min(1.0, ((x - ax) * dx + (y - ay) * dy) / (dx * dx + dy * dy)))
            ex, ey = ax + t * dx - x, ay + t * dy - y
            best = min(best, ex * ex + ey * ey)
            j = i
    return (1 if inside else -1) * math.sqrt(best)


def polylabel(polygon, precision_frac=0.002):
    """The pole of inaccessibility (lon, lat) of a GeoJSON polygon (rings of [lon, lat]): the inside point farthest from
    the edges (Mapbox's polylabel), computed with longitude scaled by cos(latitude) so distances are true."""
    lat0 = sum(p[1] for p in polygon[0]) / len(polygon[0])
    k = math.cos(math.radians(lat0)) or 1e-6
    rings = [[(p[0] * k, p[1]) for p in r] for r in polygon]
    xs, ys = [p[0] for p in rings[0]], [p[1] for p in rings[0]]
    x0, y0, x1, y1 = min(xs), min(ys), max(xs), max(ys)
    size = min(x1 - x0, y1 - y0)
    if size <= 0:
        return polygon[0][0][0], polygon[0][0][1]
    precision = max(size * precision_frac, 1e-5)
    h = size / 2.0
    heap, n = [], 0

    def cell(x, y, hh):
        d = _ring_dist(x, y, rings)
        return (-(d + hh * math.sqrt(2)), n, x, y, hh, d)

    x = x0
    while x < x1:
        y = y0
        while y < y1:
            n += 1
            heapq.heappush(heap, cell(x + h, y + h, h))
            y += size
        x += size
    # the centroid of the outer ring as the first best
    a = cx = cy = 0.0
    r0 = rings[0]
    for i in range(len(r0)):
        ax, ay = r0[i]
        bx, by = r0[i - 1]
        f = ax * by - bx * ay
        cx += (ax + bx) * f
        cy += (ay + by) * f
        a += f * 3
    best = (r0[0][0], r0[0][1], _ring_dist(r0[0][0], r0[0][1], rings))
    if a:
        c = (cx / a, cy / a, _ring_dist(cx / a, cy / a, rings))
        if c[2] > best[2]:
            best = c
    while heap:
        neg_max, _, x, y, hh, d = heapq.heappop(heap)
        if d > best[2]:
            best = (x, y, d)
        if -neg_max - best[2] <= precision:
            continue
        hh /= 2.0
        for ddx in (-hh, hh):
            for ddy in (-hh, hh):
                n += 1
                heapq.heappush(heap, cell(x + ddx, y + ddy, hh))
    return best[0] / k, best[1]


def label_point(geom):
    """The label point of the feature's largest polygon (by area)."""
    polys = geom["coordinates"] if geom["type"] == "MultiPolygon" else [geom["coordinates"]]
    big = max(polys, key=lambda p: geometry_area_km2({"type": "Polygon", "coordinates": p}))
    return polylabel(big)


def end10_of(area, lat, start10):
    """The zoom (x 10) from which the island is about END_PX across, at least MIN_SPAN10 after it came, never after
    END_CAP10."""
    across_m = math.sqrt(max(area, 0.01)) * 1000.0
    z = math.log2(END_PX * M_PER_PX_Z0 * math.cos(math.radians(lat)) / across_m)
    return int(min(END_CAP10, max(math.floor(z * 10 + 1e-9), start10 + MIN_SPAN10)))


def english(p):
    nm = p["NAME"]
    if nm in NAME_EN_FIXES:
        return NAME_EN_FIXES[nm]
    return (p.get("NAME_EN") or nm).strip()


def build(src_bytes, countries=None):
    countries = country_labels() if countries is None else countries
    d = json.loads(src_bytes.decode("utf-8"))
    rows, used = [], set()
    seen_skip, seen_not, seen_alias, seen_en = set(), set(), set(), set()
    for f in d["features"]:
        p = f["properties"]
        if p.get("FEATURECLA") not in CLASSES:
            continue
        nm = p["NAME"]
        if nm in SKIP:
            seen_skip.add(nm)
            continue
        if nm in NOT_ISLANDS:
            seen_not.add(nm)
            continue
        if nm in ALIAS_DUPES:
            seen_alias.add(nm)
            continue
        if nm in NAME_EN_FIXES:
            seen_en.add(nm)
        en = english(p)
        if en.lower() in countries or nm.lower() in countries:
            continue
        label = NAME_FIXES.get(en, en)
        if en in NAME_FIXES:
            used.add(en)
        area = geometry_area_km2(f["geometry"])
        lon, lat = label_point(f["geometry"])
        start10 = int(round(float(p["MIN_LABEL"]) * 10))
        rows.append([start10, -area, label, lat, lon])
    for label, lat, lon, start10, area in ADD:
        rows.append([start10, -area, label, lat, lon])
    missing = ((set(NAME_FIXES) - used) | (SKIP - seen_skip) | (NOT_ISLANDS - seen_not) | (ALIAS_DUPES - seen_alias) |
               (set(NAME_EN_FIXES) - seen_en))
    if missing:
        raise ValueError("hand-list names that match no island: %s" % ", ".join(sorted(missing)))
    kept = []
    for r in sorted(rows, key=lambda r: (r[1], r[2])):         # the larger first, so a duplicate keeps the larger
        if any(k[2] == r[2] and _km(k[3], k[4], r[3], r[4]) < DUPE_KM for k in kept):
            continue
        kept.append(r)
    out_rows = []
    for start10, neg_area, label, lat, lon in kept:
        out_rows.append((start10, neg_area, wrap(label), round(lat, 3), round(lon, 3),
                         end10_of(-neg_area, lat, start10), kind_of(label)))
    out_rows.sort(key=lambda r: (r[0], r[1], r[2]))
    out = {"format": FORMAT,
           "source": {"natural_earth": {"commit": NE_COMMIT, "file": NE_FILE, "sha256": hashlib.sha256(src_bytes).hexdigest()}},
           "license": "Natural Earth: public domain", "rules": RULES, "count": len(out_rows)}
    head = json.dumps(out, ensure_ascii=False, separators=(",", ":"))[:-1]
    body = ",\n".join(json.dumps([r[2], r[3], r[4], r[0], r[5], r[6]], ensure_ascii=False, separators=(",", ":")) for r in out_rows)
    return (head + ',"islands":[\n' + body + "\n]}\n").encode("utf-8")


def _km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    c = math.sin(p1) * math.sin(p2) + math.cos(p1) * math.cos(p2) * math.cos(dl)
    return 6371.0088 * math.acos(max(-1.0, min(1.0, c)))


def check(data, real=True, countries=None):
    """Problems of an islands file (a list of strings; empty = good). real: the pinned build's counts and probes."""
    try:
        d = json.loads(data.decode("utf-8"))
    except ValueError as exc:
        return ["not JSON: %s" % exc]
    if not isinstance(d, dict):
        return ["not an islands object"]
    problems = []
    if d.get("format") != FORMAT:
        problems.append("format %r" % d.get("format"))
    rows = d.get("islands") or []
    if not isinstance(rows, list):
        return ["islands is not a list"]
    if d.get("count") != len(rows):
        problems.append("count %r != %d records" % (d.get("count"), len(rows)))
    prev = None
    for k, r in enumerate(rows):
        if not (isinstance(r, list) and len(r) == 6):
            problems.append("record %d: not 6 values" % k)
            continue
        label, lat, lon, s10, e10, kind = r
        if not isinstance(label, str) or not label.strip() or label.count("\n") > 1 or any(not x.strip() for x in label.split("\n")):
            problems.append("record %d: bad label %r" % (k, label))
            continue
        if re.search("[‘’ʼ`]", label) or re.search(r"[aeiouAEIOU]'[aeiou]", label):   # Savai'i, Niuafoʼou; not D'Entrecasteaux (G81 A-5)
            problems.append("record %d (%s): an apostrophe for an ʻokina" % (k, label))
        if "\n" not in label and len(label) > WRAP_CHARS and " " in label:
            problems.append("record %d (%s): long label not wrapped" % (k, label))
        if not (type(lat) in (int, float) and type(lon) in (int, float) and -90 <= lat <= 90 and -180 <= lon <= 180):
            problems.append("record %d (%s): position out of range" % (k, label))
        if not (type(s10) is int and type(e10) is int and 10 <= s10 <= 80 and s10 + MIN_SPAN10 <= e10 <= END_CAP10):
            problems.append("record %d (%s): zooms %r..%r out of rule" % (k, label, s10, e10))
        if type(kind) is not int or kind not in (0, 1):
            problems.append("record %d (%s): kind %r" % (k, label, kind))
        if prev is not None and isinstance(s10, int) and s10 < prev:
            problems.append("record %d (%s): not sorted by start" % (k, label))
        if isinstance(s10, int):
            prev = s10
    if real:
        src = (d.get("source") or {}).get("natural_earth") or {}
        if src.get("commit") != NE_COMMIT or src.get("sha256") != NE_SHA256:
            problems.append("source is not the pinned Natural Earth file")
        if not COUNT_RANGE[0] <= len(rows) <= COUNT_RANGE[1]:
            problems.append("%d records, expected %d..%d" % (len(rows), COUNT_RANGE[0], COUNT_RANGE[1]))
        by = {}
        for r in rows:
            if isinstance(r, list) and r and isinstance(r[0], str):
                by.setdefault(r[0].replace("\n", " "), r)
        for label, kind, s10, erange in PROBES:
            r = by.get(label)
            if r is None:
                problems.append("probe: no %r" % label)
                continue
            if r[5] != kind or r[3] != s10:
                problems.append("probe: %s is kind %r from %r, expected kind %d from %d" % (label, r[5], r[3], kind, s10))
            if erange and not (erange[0] <= r[4] <= erange[1]):
                problems.append("probe: %s ends at %d, expected %d..%d" % (label, r[4], erange[0], erange[1]))
        for label in ABSENT:
            if label in by:
                problems.append("probe: %r should not be named" % label)
        countries = country_labels() if countries is None else countries
        for label in by:
            if label.lower() in countries:
                problems.append("probe: %r repeats a country name" % label)
    return problems


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--ne", help="ne_10m_geography_regions_polys.geojson (the pinned file)")
    ap.add_argument("--out", help="write the islands file here")
    ap.add_argument("--check", help="check this islands file")
    ap.add_argument("--list", help="also write every label to this text file (for review)")
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
        log("wrote %s: %d B, %d islands" % (a.out, len(data), json.loads(data)["count"]))
        if a.list:
            rows = json.loads(data)["islands"]
            with open(a.list, "w", encoding="utf-8") as fh:
                for r in rows:
                    fh.write("%s\t%s\t%d-%d\t%.3f,%.3f\n" % ("G" if r[5] == 0 else "I", r[0].replace("\n", " / "), r[3], r[4], r[1], r[2]))
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
