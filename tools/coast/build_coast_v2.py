"""Coast sets v2+ (plan sections 103-104): the coast-v1 land set with GSHHG replaced, inside bathymetry regions, by the
bathymetry's own MHHW water edge. SETS: v2 = Hawaii (stage S1); v3 = v2 + Puerto Rico / USVI, Guam / CNMI and American
Samoa (stage S2), Hawaii's cells byte-identical to v2's. --set picks one (default v2).

Inputs: a coast-v1 build (static/coast/v1, from build_coast.py) and the published bathymetry tiles of the regions in
REGIONS at zooms zmin-15 (static/bathy/<region>/v<N>/, tools/bathy). Output: a directory in the same coast-v1 format
(the client and the alert builder read it unchanged), its index marked "kind": "land2".

Rule 1, the pixel class at zoom 15, per pixel inside each region box (the 256 x 256 core of a tile; the apron is ignored):
  * the zoom-15 tile when it exists (code > 0 = water, code 0 + source LAND_ID = land, code 0 + source 0 = outside);
  * else the nearest existing ancestor tile (zoom 14 down to the region's zmin), upsampled: all-land tiles are never
    written at 15, and the 1/3" CUDEM water lives at zooms 12-14 only;
  * where no tile exists at any zoom, or the class is "outside", GSHHG v1 tier 1 decides (pixel centre inside its land).
Rule 2, the trace: exact pixel-edge polygons of the land class per block (a zoom-12 tile = 8 x 8 zoom-15 tiles; runs of
land pixels merged into rectangles and unioned with shapely), all blocks unioned in global zoom-15 Web-Mercator pixel
space (integer grid lines join exactly), land rings and holes under SPECK_PX2 dropped (counted), then Douglas-Peucker
at TOL_PX with preserve_topology (holes stay inside their shells), then lon/lat. The raster itself stops at the region
box (by pixel centre), so v2 land reaches at most half a pixel past it; --check counts v2 vertices near a box line.
Rule 3, the seam: each tier-1 cell that a region box overlaps AND whose content changes = union(GSHHG v1 outside the
boxes, v2 inside the boxes), split on the cell's own 5-degree lines, quantised at Q2 = 1e5 (land counter-clockwise, sea
water enclosed by land clockwise: the client's one nonzero fill and build_coast.point_in_pieces both read that right).
Every other tier-1 cell and the tier-0 file are copied from v1 byte for byte (the index proves it: base_v1).

Usage:
  python tools/coast/build_coast_v2.py [--set v3] --v1 build/coast/v1 --tiles build/bathy-tiles --fetch --out build/coast/v3
         (--fetch downloads the pinned bathymetry tiles, and the published v1 set when --v1 holds no index.json)
  python tools/coast/build_coast_v2.py [--set v3] --check build/coast/v3
  python tools/coast/build_coast_v2.py [--set v3] --out build/coast/v3 --upload static/coast/v3   (R2 env as
         build_coast.py; refuses a build that fails --check, is not from the pinned inputs, or differs from the set's pin)

Needs numpy, shapely and Pillow. Data: GSHHG 2.3.7 (LGPL-3.0) as in v1; NOAA NCEI CUDEM and NOAA NGS lidar (US
government, public domain) through the bathymetry tiles. LICENSE.txt beside the data carries both.
"""
import argparse
import hashlib
import http.client
import json
import math
import os
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import shapely
from shapely.geometry import Polygon, box as sbox

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "bathy"))
import build_coast as BC   # noqa: E402
import bathy_tiles as T    # noqa: E402

KIND = "land2"
Z = 15                                   # the class zoom (CUDEM 1/9" = zoom 15)
BLOCK = 8                                # a block = one zoom-12 tile = 8 x 8 zoom-15 tiles
BPX = BLOCK * T.TILE                     # 2048 px
WPX = T.TILE * 2 ** Z                    # the world in zoom-15 px
TOL_PX = 0.7                             # Douglas-Peucker tolerance, zoom-15 px (~3 m in Hawaii)
SPECK_PX2 = 2.0                          # land rings and holes smaller than this (zoom-15 px^2, ~40 m^2) are dropped
# A region may set its own "speck" and "tol" (plan section 104, owner 2026-11-19): Puerto Rico's mangrove and lagoon
# fragments made one zoom-12 tile's land clip 16.7 ms on a desktop (102 ms at 4x CPU throttle, the gate is 8 / 25 ms);
# speck 32 px^2 (~650 m^2 at 18 N: islets and ponds under ~25 m across) with DP 1.0 px gives 5.0 / 14.9 ms, the edge
# still at median 0 m, p90 4.5 m. Hawaii keeps the defaults (its cells stay v2's bytes).
S2_SPECK_PX2, S2_TOL_PX = 32.0, 1.0
# section 104 size ladder rung (a): in a cell under a seam region, a piece of more than SPLIT_VERTS vertices is cut on
# the SPLIT_DEG lines (the client walks every vertex of a piece whose bbox meets a tile: one 290k-vertex shell cost
# 10-20 ms a tile at 4x CPU throttle). The overlay only fills the pieces (one nonzero path; its coast line under wind
# comes from the filled mask), so the cuts never show; the alert builder skips edges along them (index.json "split").
SPLIT_DEG, SPLIT_VERTS = 0.25, 20000
# section 104 size gates per changed cell (the client's per-tile cost and first-load bytes)
MAX_CELL_BYTES, MAX_CELL_VERTS, MAX_PIECE_VERTS = 2 * 1024 * 1024, 600000, 150000
SEAM_MARGIN_PX = 2                       # section 104: a seam region's raster reaches this far past its box
# Puget Sound, Hood Canal, the San Juan Islands and the US shore of the Strait of Georgia (east of 123.2 W, so Port
# Angeles and the outer Strait stay on the bathymetry): GSHHG's (owner 2026-11-20: the outer coast only)
PUGET_SOUND = (-123.2, 46.9, -121.8, 49.1)
Q2 = 100000                              # the changed cells' quantum (1e-5 degree, ~1.1 m)
OUT, LAND, WATER = 0, 1, 2
PUBLIC = "https://models.allshoresurf.com"
UA = "allshore-coast-build/2 (+https://allshoresurf.com)"   # the edge refuses Python's default User-Agent

# The bathymetry regions replacing GSHHG (tools/bathy/bathy_sources.REGIONS: the same boxes; the tests compare them),
# pinned by the published index's digest (bathy index.json "digest") and by the sha256 of the zoom-zmin..15 tiles read
# (tiles_digest), so a real build always reads the same bytes. A coast set (SETS) uses some of them.
ALL_REGIONS = {
    "hawaii": dict(v=2, box=(-160.5, 18.5, -154.5, 22.5), zmin=8,
                   digest="d5bdaf1b73a66d886654d93facc2e719067e0fd2484212ec0187cfaa9f5cadd1",
                   tiles_sha256="4d30de1c827b8afd23e386e22b31080c0f29fc119fe0221244f5f7e9a38c07e2"),
    "nwhi-lidar": dict(v=1, box=(-168.5, 22.5, -160.5, 24.5), zmin=8,
                       digest="0ae9561cfccb63f6724bd481fb65e8bf12ac5ddb3615ca72bebe9750fc32985e",
                       tiles_sha256="2c040fd2e5ca9172701ed3e578ba29c67f91aa854b43e3094d7f64d46b1e09b8"),
    "prvi": dict(v=2, box=(-68.0, 17.25, -64.25, 19.0), zmin=8,
                 digest="ec70e799bd9df888ff55f80900ff76091b9ea19651efc024c095c98466e90452",
                 tiles_sha256="c733f6cedc74b6ae52cd2f0c729c8a2f3a8af31a1fb8bcea5c8cf7606d58e031", speck=S2_SPECK_PX2, tol=S2_TOL_PX),
    "marianas": dict(v=2, box=(144.5, 13.0, 146.0, 15.5), zmin=8,
                     digest="bc283d8a2a75c5d261e22ece3c48c1bcf7d829477adbb7e3c059d79307398d79",
                     tiles_sha256="15f2985069d6f391c7bec467d370a14f3c8c74d00a3ec0486a335da875e5325e", speck=S2_SPECK_PX2,
                     tol=S2_TOL_PX),
    "samoa": dict(v=2, box=(-171.0, -14.5, -169.25, -14.0), zmin=8,
                  digest="8815034a8a28a5f09ffe72433fdd78114fc6b2194a21cb2e90f2966009261ba1",
                  tiles_sha256="a65ba559e75e1e92c74272f6e3b50aaf67a278b68b7edc3c1fc41da75adf2a42", speck=S2_SPECK_PX2,
                  tol=S2_TOL_PX),
    # the US West Coast (section 104 release 2, owner 2026-11-20 "West Coast first"): the boxes cut the continent, so
    # each is a seam region (margin_px: its raster reaches past the box and is cut exactly on the box line, where
    # GSHHG's land meets it); Puget Sound stays GSHHG (an exclusion box); the S2 speck / DP
    "us-ca-south": dict(v=1, box=(-121.0, 32.4, -116.9, 34.6), zmin=8,
                        digest="b503ffd01244d2ed4fddf553e3d3121c71b4894e523be1c397997cd2253d41df",
                        tiles_sha256="e3024fd61d591afb9162d821ebf3e51c808768fd2f1b45c1f2d2996c8b433431",
                        speck=S2_SPECK_PX2, tol=S2_TOL_PX, margin_px=SEAM_MARGIN_PX),
    "us-ca-north": dict(v=1, box=(-124.6, 34.6, -119.5, 42.0), zmin=8,
                        digest="116e8ee1a854027f0be11fed7e81f598239c8f43eb52d199ea1f13b05824732f",
                        tiles_sha256="4fd26f3e37cf6bf30fb0c0ed05b13d37fea0fa11cbf90daafe9154fb51df881e",
                        speck=S2_SPECK_PX2, tol=S2_TOL_PX, margin_px=SEAM_MARGIN_PX),
    "us-or-wa": dict(v=1, box=(-125.2, 42.0, -121.8, 49.1), zmin=8,
                     digest="49cba467bd4faf6713ba24033599787fd4c4627da2c0bf5068eacf042b606379",
                        tiles_sha256="79cc15320127ba55b8da0bcad4015985f14e62b0c4fd2109d876805fdb468a04",
                     speck=S2_SPECK_PX2, tol=S2_TOL_PX, margin_px=SEAM_MARGIN_PX, exclude=[PUGET_SOUND]),
}
# The published v1 set this build starts from (static/coast/v1 index.json tier0.sha256 / tier1.sha256).
V1_PIN_TIER0_SHA256 = "aabbff516107953c42915a11c70941682030ee3aecc29b0d0456d33b3ef9cebc"
V1_PIN_TIER1_SHA256 = "a4b97403aac09c0a4ca88f50d77d8e339eb061342204a0dd227294e71cf001c6"
AREA_TOL = 0.03
SEAM_STEP_MAX_M = 600                    # section 104: an edge along a seam box line (traced shore vs GSHHG's) at most this
# Hawaii probes (lon, lat, land?): rule 1 at zoom 15 with a margin; picked from the data in step 1.
HAWAII_PROBES = [
    (-158.11281, 21.58959, False),      # Haleiwa: the 250-300 m band GSHHG drew as land, 62 m off the MHHW edge
    (-158.04895, 21.66794, False),      # Sunset Beach: the same band
    (-157.805, 21.315, True),           # Honolulu inland (Manoa), class from a zoom-12 ancestor, 2.2 km inland
    (-157.958, 21.372, False),          # Pearl Harbor, East Loch (GSHHG: land)
    (-157.757, 21.385, True),           # Kawainui Marsh: below MHHW but not joined to the sea
    (-155.468, 19.821, True),           # Mauna Kea: the class from a zoom-9 ancestor
    (-155.00417, 19.33659, False),      # Puna: an 8-px pit (to 51 m deep, so "sea" by the water rule) = a CW hole
    (-166.20, 23.78, False),            # French Frigate Shoals lagoon (GSHHG: water too)
    (-161.922, 23.061, True),           # Nihoa (GSHHG's Nihoa is offset: water here)
]
# Puerto Rico / USVI, Guam / CNMI and American Samoa probes (plan section 104 step 1: the deepest bathymetry-water
# pixel under GSHHG land, or the most inland bathymetry-land pixel, within 3 zoom-15 tiles of each spot).
S2_PROBES = [
    (-66.1154, 18.45863, False),        # San Juan, Condado / bay mouth: GSHHG land, ~1 km of water
    (-66.08279, 18.42752, True),        # San Juan inland
    (-67.25832, 18.36493, True),        # Rincon inland
    (-64.94004, 18.3651, False),        # St Thomas, Magens Bay (GSHHG: land)
    (144.6788, 13.46457, False),        # Guam, Apra Harbor inner (GSHHG: land)
    (144.82176, 13.53922, True),        # Guam inland above Tumon
    (145.73362, 15.21001, True),        # Saipan inland
    (-170.7167, -14.25359, False),      # Tutuila, Pago Pago Harbor (GSHHG: land)
    (-169.45328, -14.26295, False),     # Ta'u, off the south shore (GSHHG's Ta'u is offset: land here)
    (-169.43113, -14.22181, True),      # Ta'u inland (GSHHG: water)
]
# the West Coast (section 104 release 2): water where GSHHG has land (one has GSHHG's water too), land inland of
# each spot, and Puget Sound (an exclusion box: GSHHG's coast stays)
WEST_PROBES = [
    (-117.98233, 33.64151, False),      # Huntington Beach, the surf line (GSHHG: land)
    (-117.97121, 33.65119, True),       # Huntington Beach inland
    (-117.25723, 32.86126, False),      # La Jolla Shores (GSHHG: land)
    (-117.24612, 32.85192, True),       # La Jolla inland
    (-122.5087, 37.7274, False),        # Ocean Beach SF (GSHHG: land)
    (-122.49758, 37.74464, True),       # the Sunset, inland
    (-121.99204, 36.95845, False),      # Santa Cruz, the harbour mouth side (GSHHG: land)
    (-122.03611, 36.96743, True),       # Santa Cruz inland
    (-123.96988, 45.88629, False),      # Cannon Beach (GSHHG: land)
    (-123.95876, 45.91293, True),       # Cannon Beach inland
    (-124.12369, 46.90661, False),      # Westport, the Grays Harbor entrance
    (-124.11257, 46.89775, True),       # Westport inland
    (-122.36, 47.60, False),            # Elliott Bay, Puget Sound (GSHHG's, as in v1)
    (-122.53, 47.63, True),             # Bainbridge Island (GSHHG's, as in v1)
    # bays and estuaries (G72 #4: the area gate is mostly inland GSHHG fill, too coarse to see a bay flip)
    (-122.30, 37.70, False),            # SF Bay, central
    (-122.38, 37.80, False),            # SF Bay off Treasure Island
    (-122.43, 37.862, True),            # Angel Island
    (-122.35, 38.05, False),            # San Pablo Bay
    (-122.05, 38.06, False),            # Suisun Bay (GSHHG: land)
    (-121.60, 38.04, False),            # Franks Tract, the flooded Delta island (GSHHG: land)
    (-121.72, 38.07, True),             # Sherman Island, a diked Delta island below sea level
    (-124.22, 40.72, False),            # Humboldt Bay, south (GSHHG: land)
    (-124.00, 46.24, False),            # the Columbia River mouth
    (-123.95, 46.55, False),            # Willapa Bay
    (-124.00, 46.95, False),            # Grays Harbor
]
NOAA_CREDIT_V2 = """
Inside the bathymetry regions (Hawaii: 160.5-154.5 W, 18.5-22.5 N; the NW Hawaiian Islands: 168.5-160.5 W,
22.5-24.5 N) the coastline is the mean-higher-high-water edge of the allshoresurf.com bathymetry tiles, built from
NOAA NCEI Continuously Updated Digital Elevation Models (CUDEM, 1/9 and 1/3 arc-second, Hawaii) and the NOAA NGS 2021
Northwestern Hawaiian Islands topobathy lidar (NOAA Digital Coast). These US government data are in the public domain.
Not for navigation.
"""
NOAA_CREDIT_V3 = """
Inside the bathymetry regions (Hawaii: 160.5-154.5 W, 18.5-22.5 N; the NW Hawaiian Islands: 168.5-160.5 W,
22.5-24.5 N; Puerto Rico and the US Virgin Islands: 68-64.25 W, 17.25-19 N; Guam and the CNMI: 144.5-146 E,
13-15.5 N; American Samoa: 171-169.25 W, 14.5-14 S) the coastline is the mean-higher-high-water edge of the
allshoresurf.com bathymetry tiles, built from NOAA NCEI Continuously Updated Digital Elevation Models (CUDEM, 1/9 and
1/3 arc-second) and the NOAA NGS 2021 Northwestern Hawaiian Islands topobathy lidar (NOAA Digital Coast). These US
government data are in the public domain. Not for navigation.
"""
# Hawaii's five cells as published in static/coast/v2 (sha256 of each file): every later set keeps them byte for byte.
V2_HAWAII_CELLS = {
    "15_-155": "e9ffdbebf924eaf651a702b685f2c59e1eaeb7df79b2e5938d9db6e7dfd9e989",
    "15_-160": "77b11d852493e8c1c7f614a88bc6a2812e5f13466f81192ef47d278bd2354a04",
    "20_-160": "6c2f124300ba97e65d04a7efdf2fff4c1ad54de03052d56a360253804409f00d",
    "20_-165": "d9031b442d293362ea5a7adb5a05f781d94be30ccbb72db81807943f430d2028",
    "20_-170": "be644a005e4dea22c575df61c4cd2883837ddbc9535f4f5bb3207b152ae96908",
}
# v3's twelve changed cells as published in static/coast/v3 (sha256): v4 keeps them byte for byte.
V3_CELLS = dict(V2_HAWAII_CELLS, **{
    "-15_-170": "437f67815b24ba1b66e6c7964a6f1fe5a812f476baf3af5ba5816bd47c6fecd2",
    "-15_-175": "7467e55a953d8a884e631b1997077cd76c195878d44bd5ecb95d35edfd82331e",
    "10_140": "207c4278f7078500e91e0cc1196a68885dfec7a28db0effa4bda598314cf6a12",
    "10_145": "043508288f6d13807fa6aa7c0008b0eb6a3ff75dc44ca75c14b1a21f2fe9678a",
    "15_-65": "151de8af30316acd043fd728291d6a5c04fe6d1ee024a541dd5cdce2ed616461",
    "15_-70": "52c055bd833c46e8ad8688c46636f04321dd6ac6fe3de079bc9d19536e44f2e1",
    "15_145": "b63df81ae6b159a51413cc9d5cc7fc921d9d0b3718ae1501e29bdeac69511cd4",
})
NOAA_CREDIT_V4 = """
Inside the bathymetry regions (Hawaii: 160.5-154.5 W, 18.5-22.5 N; the NW Hawaiian Islands: 168.5-160.5 W,
22.5-24.5 N; Puerto Rico and the US Virgin Islands: 68-64.25 W, 17.25-19 N; Guam and the CNMI: 144.5-146 E,
13-15.5 N; American Samoa: 171-169.25 W, 14.5-14 S; the US West Coast: California, Oregon and the outer coast of
Washington, 125.2-116.9 W, 32.4-49.1 N, Puget Sound excepted) the coastline is the mean-higher-high-water edge of the
allshoresurf.com bathymetry tiles, built from NOAA NCEI Continuously Updated Digital Elevation Models (CUDEM, 1/9 and
1/3 arc-second), the NOAA NGS 2021 Northwestern Hawaiian Islands topobathy lidar, USGS CoNED topobathymetric models,
NOAA OCS BlueTopo and USACE topobathy data (NOAA Digital Coast). These US government data are in the public domain.
Elsewhere the coastline is GSHHG's. Not for navigation.
"""
# The coast sets (plan sections 103-104). Per set:
#   regions           the bathymetry regions traced;
#   expected_changed  the tier-1 cells a real build changes (every other cell is v1's bytes);
#   pin               the exact tier-1 bytes of a real build (deterministic from the pinned inputs, this code and the
#                     pinned shapely; laptop and runner must agree): a deliberate change updates it. Tier 0 is v1's;
#   v1_sqdeg          the land area of expected_changed in v1 (sq deg, build_coast.signed_area of the q=1e4 rings,
#                     unweighted as in build_coast.check): the new total stays within AREA_TOL of it, judged on the
#                     total only (per cell, Niihau loses ~6 %, French Frigate Shoals ~85 %, Ta'u ~5 %: GSHHG offsets);
#   probes            (lon, lat, land?) checked in the changed cells, beside build_coast.PROBES;
#   same_cells        {cell: sha256} that must come out byte-identical (Hawaii stays as published in v2).
SETS = {
    "v2": dict(regions=["hawaii", "nwhi-lidar"],
               expected_changed=["15_-155", "15_-160", "20_-160", "20_-165", "20_-170"],
               pin="9a0ca75f08d7b2fb6bc4d6df0a43cb1653ddabbcb1fd4b1b5cf77cc6d21923f7",  # laptop and runner agree (coast-build dry run #711, ubuntu-24.04; shapely 2.2.0)
               v1_sqdeg=1.456635,                                                        # a real v2 build: 1.43924 (-1.19 %)
               probes=HAWAII_PROBES, same_cells={}, credit=NOAA_CREDIT_V2),
    "v3": dict(regions=["hawaii", "nwhi-lidar", "prvi", "marianas", "samoa"],
               expected_changed=["-15_-170", "-15_-175", "10_140", "10_145", "15_-155", "15_-160", "15_-65", "15_-70",
                                 "15_145", "20_-160", "20_-165", "20_-170"],
               pin="0881b635c8d073476aa1aee576cfb754e9a2d024329d02e09bb65f61ffe82a56",  # laptop and runner agree (coast-build dry run #731, ubuntu-24.04; shapely 2.2.0)
               v1_sqdeg=4.074942,                                                        # a real v3 build: 4.04433 (-0.75 %)
               probes=HAWAII_PROBES + S2_PROBES, same_cells=V2_HAWAII_CELLS, credit=NOAA_CREDIT_V3),
    "v4": dict(regions=["hawaii", "nwhi-lidar", "prvi", "marianas", "samoa", "us-ca-south", "us-ca-north", "us-or-wa"],
               # v3's twelve + the West Coast's eight (35_-120 / 40_-120: the inland box strip east of -120, GSHHG's
               # land re-joined at q = 1e5; 45_-130: the box's -125.2..-125 strip, which reaches Vancouver Island's shore)
               expected_changed=["-15_-170", "-15_-175", "10_140", "10_145", "15_-155", "15_-160", "15_-65", "15_-70",
                                 "15_145", "20_-160", "20_-165", "20_-170", "30_-120", "30_-125", "35_-120", "35_-125",
                                 "40_-120", "40_-125", "45_-125", "45_-130"],
               # the runner's build (dry run #745): the laptop's (6e5740bf) is the same coast, 208 rings of the GEOS-joined
               # West Coast cells starting at another vertex
               pin="58d55125de71c316f5d974c1edd7ebac0bf691d52e76cfd12f6f5ea251ac5004",
               v1_sqdeg=121.761359,                                                      # a real v4 build: 121.56886 (-0.16 %)
               probes=HAWAII_PROBES + S2_PROBES + WEST_PROBES, same_cells=V3_CELLS, credit=NOAA_CREDIT_V4),
}
LICENSE = BC.LICENSE + "; NOAA NCEI CUDEM and NOAA NGS lidar (public domain) inside the bathymetry regions"


def format_key(regions):
    return ("coast-v1-land2|base=coast-v1|z=%d|block=%d|tol=%g|speck=%g|q=%d|rule=tile>ancestor>gshhg|boxes=%s|"
            "shapely=%s" % (Z, BLOCK, TOL_PX, SPECK_PX2, Q2,
                            ";".join("%s:v%d:%s%s" % (n, r["v"], ",".join("%g" % v for v in r["box"]),
                                                      "".join(":%s=%g" % (k, r[k]) for k in ("speck", "tol", "margin_px") if k in r) +
                                                      "".join(":exclude=%s" % ",".join("%g" % v for v in e) for e in r.get("exclude", ())))
                                     for n, r in sorted(regions.items())),
                            ".".join(shapely.__version__.split(".")[:2]))) + (
        "|split=%g>%d" % (SPLIT_DEG, SPLIT_VERTS) if any(r.get("margin_px") for r in regions.values()) else "")


def split_big(geom):
    """-> (the Polygon parts of geom, each of more than SPLIT_VERTS vertices cut into its pieces inside the SPLIT_DEG
    squares its bounds meet, the number of parts cut). Section 104 rung a; the squares are exact multiples of SPLIT_DEG."""
    out, cut = [], 0
    for p in polygons(geom):
        if shapely.get_num_coordinates(p) <= SPLIT_VERTS:
            out.append(p)
            continue
        x0, y0, x1, y1 = p.bounds
        i0, i1 = math.floor(x0 / SPLIT_DEG), math.ceil(x1 / SPLIT_DEG)
        j0, j1 = math.floor(y0 / SPLIT_DEG), math.ceil(y1 / SPLIT_DEG)
        cut += 1
        sq = [sbox(i * SPLIT_DEG, j * SPLIT_DEG, (i + 1) * SPLIT_DEG, (j + 1) * SPLIT_DEG)
              for i in range(i0, i1) for j in range(j0, j1)]
        for part in shapely.intersection(p, sq):
            out.extend(polygons(part))
    return (shapely.MultiPolygon(out) if out else shapely.MultiPolygon()), cut


def use_set(name):
    """Make coast set `name` the active one: the module-level REGIONS, EXPECTED_CHANGED, PIN_TIER1_SHA256,
    V1_CHANGED_SQDEG, SET_PROBES, SAME_CELLS, FORMAT_KEY, SOURCE and NOAA_CREDIT the build and check read."""
    global SET, REGIONS, EXPECTED_CHANGED, PIN_TIER1_SHA256, V1_CHANGED_SQDEG, SET_PROBES, SAME_CELLS
    global FORMAT_KEY, SOURCE, NOAA_CREDIT
    st = SETS[name]
    SET = name
    REGIONS = {n: ALL_REGIONS[n] for n in st["regions"]}
    EXPECTED_CHANGED = list(st["expected_changed"])
    PIN_TIER1_SHA256 = st["pin"]
    V1_CHANGED_SQDEG = st["v1_sqdeg"]
    SET_PROBES = list(st["probes"])
    SAME_CELLS = dict(st["same_cells"])
    FORMAT_KEY = format_key(REGIONS)
    SOURCE = source(REGIONS)
    NOAA_CREDIT = st["credit"]


def source(regions):
    return "GSHHG 2.3.7 + NOAA bathymetry MHHW water edge (allshoresurf bathy regions %s)" % ", ".join(sorted(regions))


use_set("v2")


def log(msg):
    print(msg, flush=True)


# ---------------------------------------------------------------- inputs

def _ssl_context():
    """certifi's list, not the system's (plan section 57: a compromised chain stayed trusted in the runner's list)."""
    import certifi
    return ssl.create_default_context(cafile=certifi.where())


_conn = threading.local()


def _get(url, timeout=60, tries=4):
    """GET with a keep-alive HTTPS connection per thread (a new TLS connection per tile cost ~0.5 s); 403/404 raise
    urllib.error.HTTPError at once, other failures retry on a fresh connection."""
    parts = urllib.parse.urlsplit(url)
    path = parts.path + ("?" + parts.query if parts.query else "")
    for i in range(tries):
        conns = getattr(_conn, "c", None)
        if conns is None:
            conns = _conn.c = {}
        c = conns.get(parts.netloc)
        if c is None:
            c = conns[parts.netloc] = http.client.HTTPSConnection(parts.netloc, context=_ssl_context(), timeout=timeout)
        try:
            c.request("GET", path, headers={"User-Agent": UA})
            r = c.getresponse()
            data = r.read()
            if r.status == 200:
                return data
            exc = urllib.error.HTTPError(url, r.status, r.reason, r.headers, None)
            if r.status in (403, 404) or 300 <= r.status < 400 or i == tries - 1:   # a redirect is not followed (G71 P3-3)
                raise exc
        except urllib.error.HTTPError:
            raise
        except (http.client.HTTPException, OSError):
            c.close()
            conns.pop(parts.netloc, None)
            if i == tries - 1:
                raise
        time.sleep(1 + 2 * i)


def _idx_offsets(zmax):
    """build_bathy.idx_offsets: each zoom's bitset inside an idx file (zooms 11..zmax)."""
    out, off = {}, 0
    for z in range(11, zmax + 1):
        n = ((2 ** (z - 10)) ** 2 + 7) // 8
        out[z] = (off, n)
        off += n
    return out


def listed_tiles(index, idx_bytes):
    """[(z, x, y)] of the zooms zmin..Z a bathy index lists: zooms <= 10 from index["levels"], finer ones from the idx
    files (idx_bytes: {"x_y": bytes})."""
    import base64
    out = []
    for z, L in index.get("levels", {}).items():
        if int(z) > Z:
            continue
        m = T.unpack_bits(base64.b64decode(L["bits"]), (L["h"], L["w"]))
        for r, c in zip(*np.nonzero(m)):
            out.append((int(z), L["x0"] + int(c), L["y0"] + int(r)))
    offs = _idx_offsets(int(index["zmax"])) if index.get("idx") else {}
    for cell in index.get("t10") or ():
        cx, cy = (int(v) for v in cell.split("_"))
        data = idx_bytes[cell]
        for z in range(11, min(Z, int(index["zmax"])) + 1):
            k = 2 ** (z - 10)
            o, n = offs[z]
            m = T.unpack_bits(data[o:o + n], (k, k))
            for r, c in zip(*np.nonzero(m)):
                out.append((z, cx * k + int(c), cy * k + int(r)))
    return sorted(out)


def fetch_tiles(tiles_dir, regions=None, workers=16):
    """Download the pinned regions' index, idx files and tiles of zooms zmin..15 into tiles_dir/<region>/v<N>/."""
    regions = regions or REGIONS
    for name, reg in sorted(regions.items()):
        base = "%s/static/bathy/%s/v%d" % (PUBLIC, name, reg["v"])
        d = os.path.join(tiles_dir, name, "v%d" % reg["v"])
        os.makedirs(os.path.join(d, "idx"), exist_ok=True)
        raw = _get(base + "/index.json")
        index = json.loads(raw.decode("utf-8"))
        if index.get("digest") != reg["digest"]:
            raise ValueError("%s v%d: published digest %s is not the pinned %s" % (
                name, reg["v"], str(index.get("digest"))[:12], reg["digest"][:12]))
        open(os.path.join(d, "index.json"), "wb").write(raw)
        idx = {}
        for cell in index.get("t10") or ():
            p = os.path.join(d, "idx", cell + ".bin")
            if not os.path.exists(p):
                open(p, "wb").write(_get(base + "/idx/%s.bin" % cell))
            idx[cell] = open(p, "rb").read()
        want = listed_tiles(index, idx)

        def one(t):
            z, x, y = t
            p = os.path.join(d, "t", str(z), str(x), "%d.png" % y)
            if os.path.exists(p) and os.path.getsize(p) > 0:
                return 0
            os.makedirs(os.path.dirname(p), exist_ok=True)
            data = _get(base + "/t/%d/%d/%d.png" % t)
            with open(p + ".part", "wb") as f:
                f.write(data)
            os.replace(p + ".part", p)
            return 1
        t0 = time.time()
        with ThreadPoolExecutor(workers) as ex:
            n = sum(ex.map(one, want))
        log("fetch %s v%d: %d tiles listed at zooms %d-%d, %d downloaded (%.0f s)" % (
            name, reg["v"], len(want), reg["zmin"], Z, n, time.time() - t0))


def fetch_v1(v1_dir):
    """Download the published static/coast/v1 set (index, tier 0, every tier-1 cell) into v1_dir."""
    base = PUBLIC + "/static/coast/v1"
    raw = _get(base + "/index.json")
    index = json.loads(raw.decode("utf-8"))
    os.makedirs(os.path.join(v1_dir, index["tier1"]["dir"]), exist_ok=True)
    rels = [index["tier0"]["file"]] + ["%s/%s.bin" % (index["tier1"]["dir"], n) for n in sorted(index["tier1"]["cells"])]

    def one(rel):
        p = os.path.join(v1_dir, rel)
        if not os.path.exists(p):
            open(p, "wb").write(_get(base + "/" + rel))
    with ThreadPoolExecutor(16) as ex:
        list(ex.map(one, rels))
    open(os.path.join(v1_dir, "index.json"), "wb").write(raw)
    log("fetch v1: %d files" % len(rels))


class Tiles:
    """One region's tiles on disk (tiles_dir/<region>/v<N>/t/z/x/y.png + index.json): presence, classes, ancestors."""

    def __init__(self, tiles_dir, name, reg):
        self.name, self.reg = name, reg
        self.dir = os.path.join(tiles_dir, name, "v%d" % reg["v"])
        self.index = json.load(open(os.path.join(self.dir, "index.json")))
        if int(self.index.get("version", -1)) != reg["v"] or self.index.get("region", name) != name:
            raise ValueError("%s: index is version %s, expected %d" % (name, self.index.get("version"), reg["v"]))
        self.present = {}
        for z in range(reg["zmin"], Z + 1):
            s = set()
            zd = os.path.join(self.dir, "t", str(z))
            if os.path.isdir(zd):
                for x in os.listdir(zd):
                    for f in os.listdir(os.path.join(zd, x)):
                        if f.endswith(".png"):
                            s.add((int(x), int(f[:-4])))
            self.present[z] = s
            want = int((self.index.get("tiles") or {}).get(str(z), 0))
            if len(s) != want:
                raise ValueError("%s: zoom %d holds %d tiles, the index lists %d (incomplete download?)"
                                 % (name, z, len(s), want))
        self._anc = {}
        self.anomalies = 0          # code 0 with a source that is neither LAND_ID nor 0, or water with LAND_ID

    def digest(self):
        """sha256 over the tiles read (zooms zmin..15): 'z/x/y.png' + each file's sha256, sorted."""
        h = hashlib.sha256()
        n = 0
        for z in sorted(self.present):
            for (x, y) in sorted(self.present[z]):
                rel = "%d/%d/%d.png" % (z, x, y)
                h.update(rel.encode() + b"\0" + hashlib.sha256(open(os.path.join(self.dir, "t", rel), "rb").read()).digest())
                n += 1
        return h.hexdigest(), n

    def cls(self, z, x, y):
        with open(os.path.join(self.dir, "t", str(z), str(x), "%d.png" % y), "rb") as f:
            code, sf = T.unpack_tile(f.read())
        code, src = code[1:1 + T.TILE, 1:1 + T.TILE], sf[1:1 + T.TILE, 1:1 + T.TILE] & T.SRC_BITS
        c = np.full(code.shape, OUT, np.uint8)
        c[code > 0] = WATER
        c[(code == 0) & (src == T.LAND_ID)] = LAND
        self.anomalies += int(((code == 0) & (src != T.LAND_ID) & (src != 0)).sum())
        self.anomalies += int(((code > 0) & (src == T.LAND_ID)).sum())
        return c

    def ancestor(self, x, y):
        for za in range(Z - 1, self.reg["zmin"] - 1, -1):
            sh = Z - za
            if (x >> sh, y >> sh) in self.present.get(za, ()):
                return za, x >> sh, y >> sh
        return None

    def anc_slice(self, x, y, a):
        """The k x k slice (k = 256 >> (15 - za)) of ancestor a covering zoom-15 tile x/y."""
        za, xa, ya = a
        if a not in self._anc:
            if len(self._anc) > 3000:
                self._anc.clear()
            self._anc[a] = self.cls(za, xa, ya)
        sh = Z - za
        k = T.TILE >> sh
        ox, oy = (x - (xa << sh)) * k, (y - (ya << sh)) * k
        return self._anc[a][oy:oy + k, ox:ox + k]


# ---------------------------------------------------------------- geometry helpers

def px_lon(X):
    return np.asarray(X, np.float64) / WPX * 360.0 - 180.0


def px_lat(Y):
    return np.degrees(np.arctan(np.sinh(np.pi * (1.0 - 2.0 * np.asarray(Y, np.float64) / WPX))))


def lonlat_px(lon, lat):
    s = np.sin(np.radians(lat))
    return (np.asarray(lon, np.float64) + 180.0) / 360.0 * WPX, (0.5 - np.log((1 + s) / (1 - s)) / (4 * np.pi)) * WPX


def raster_box(reg):
    """The lon/lat box the region's raster covers: its box, or for a seam region (section 104: a box line that cuts
    through land) its box grown by margin_px zoom-15 px (in longitude degrees, which over-covers latitude), so the
    traced land reaches past the box and the cell stage cuts it exactly on the box line, where GSHHG's land meets it."""
    b, m = reg["box"], reg.get("margin_px", 0)
    if not m:
        return b
    pad = (m + 0.5) * 360.0 / WPX
    return (b[0] - pad, b[1] - pad, b[2] + pad, b[3] + pad)


def to_lonlat(geom):
    return shapely.transform(geom, lambda a: np.column_stack([px_lon(a[:, 0]), px_lat(a[:, 1])]))


def seam_step_lengths(seam_iv, q=None):
    """{(cell, "x"|"y", line value): [(lo, hi, upward?), ...]} edges lying along seam box lines -> [(cell, metres)] of
    the stretches covered one way only: a split cut runs both ways (land on both sides) and cancels; a step where the
    traced shore and GSHHG's disagree does not. ("x": the line is x = value, so lengths are in latitude.)"""
    q = q or Q2
    out = []
    for (name, kind, line), ivs in sorted(seam_iv.items()):
        ways = [shapely.union_all([shapely.LineString([(lo, 0.0), (hi, 0.0)]) for lo, hi, up in ivs if up == w])
                for w in (True, False)]
        for part in line_parts(shapely.symmetric_difference(*ways)):
            if part.length * q < 0.5:                    # quantum noise where the two ways' vertices differ by a unit
                continue
            m = part.length * 111320.0 * (1.0 if kind == "x" else np.cos(np.radians(line)))
            out.append((name, round(float(m), 1)))
    return out


def line_parts(geom):
    """The LineString parts of any geometry (seam-step intervals, section 104)."""
    if geom is None or geom.is_empty:
        return []
    if geom.geom_type == "LineString":
        return [geom]
    return [p for g in getattr(geom, "geoms", ()) for p in line_parts(g)]


def polygons(geom):
    """The Polygon parts of any geometry (intersections may also yield lines and points)."""
    if geom is None or geom.is_empty:
        return []
    if geom.geom_type == "Polygon":
        return [geom]
    out = []
    for g in getattr(geom, "geoms", ()):
        out.extend(polygons(g))
    return out


def mask_polygons(mask, X0=0, Y0=0):
    """Exact pixel-edge polygons of a boolean mask (row 0 at Y0, column 0 at X0, y down): runs of True pixels per row,
    equal runs of consecutive rows merged into rectangles, all unioned. Pixels touching only at a corner stay apart
    (or meet at a single point)."""
    h, w = mask.shape
    if not mask.any():
        return []
    m = np.zeros((h, w + 2), np.int8)
    m[:, 1:-1] = mask
    d = np.diff(m, axis=1)
    rs, c0 = np.nonzero(d == 1)
    _, c1 = np.nonzero(d == -1)
    order = np.lexsort((rs, c1, c0))
    r, a, b = rs[order], c0[order], c1[order]
    new = np.ones(len(r), bool)
    new[1:] = (a[1:] != a[:-1]) | (b[1:] != b[:-1]) | (r[1:] != r[:-1] + 1)
    starts = np.nonzero(new)[0]
    ends = np.append(starts[1:], len(r)) - 1
    boxes = shapely.box(X0 + a[starts], Y0 + r[starts], X0 + b[starts], Y0 + r[ends] + 1)
    return polygons(shapely.union_all(boxes))


def drop_specks(geom, min_area):
    """-> (polygons, land specks dropped, hole specks filled)."""
    kept, n_land, n_hole = [], 0, 0
    for p in polygons(geom):
        if p.area < min_area:
            n_land += 1
            continue
        holes = []
        for r in p.interiors:
            if Polygon(r).area < min_area:
                n_hole += 1
            else:
                holes.append(r)
        kept.append(Polygon(p.exterior, holes) if len(holes) != len(p.interiors) else p)
    return kept, n_land, n_hole


# ---------------------------------------------------------------- v1

def load_v1(v1_dir):
    index = json.load(open(os.path.join(v1_dir, "index.json")))
    if index.get("format") != "coast-v1" or index.get("kind") is not None or index.get("q") != BC.Q:
        raise ValueError("--v1 %s is not a coast-v1 land set" % v1_dir)
    return index


def v1_cell_geom(v1_dir, index, name, cache={}):   # noqa: B006 (a per-process cache)
    """(raw bytes or None, shapely union of the cell's land in lon/lat or None, rings as lon/lat arrays)."""
    key = (v1_dir, name)
    if key not in cache:
        if name not in index["tier1"]["cells"]:
            cache[key] = (None, None, [])
        else:
            buf = open(os.path.join(v1_dir, index["tier1"]["dir"], name + ".bin"), "rb").read()
            d = BC.decode_file(buf)
            rings, polys = [], []
            for pc in d["pieces"]:
                ps = [np.column_stack([ix / d["q"], iy / d["q"]]) for ix, iy in pc["rings"]]
                rings.extend(ps)
                p = Polygon(ps[0], ps[1:])
                polys.append(p if p.is_valid else shapely.make_valid(p))
            cache[key] = (buf, shapely.union_all(polys) if polys else None, rings)
    return cache[key]


def cells_overlapping(box_, cell=5):
    lon0, lat0, lon1, lat1 = box_
    out = []
    for la in range(int(math.floor(lat0 / cell)) * cell, int(math.ceil(lat1 / cell)) * cell, cell):
        for lo in range(int(math.floor(lon0 / cell)) * cell, int(math.ceil(lon1 / cell)) * cell, cell):
            out.append(BC.cell_name(la, lo))
    return out


def gshhg_near(v1_dir, index, lonlat_box, cache={}):   # noqa: B006 (a per-process cache)
    """The v1 land overlapping a lon/lat box (prepared), or None. (The union is cached by the cells it is made of:
    section 104's mainland blocks ask for the same two or four cells thousands of times.)"""
    names = []
    for name in cells_overlapping(lonlat_box):
        g = v1_cell_geom(v1_dir, index, name)[1]
        if g is not None and g.intersects(sbox(*lonlat_box)):
            names.append(name)
    if not names:
        return None
    key = (v1_dir, tuple(names))
    if key not in cache:
        if len(cache) > 64:
            cache.clear()
        g = shapely.union_all([v1_cell_geom(v1_dir, index, n)[1] for n in names])
        shapely.prepare(g)
        cache[key] = g
    return cache[key]


FILL_LEAF = 32          # gshhg_fill: squares this small or smaller are tested pixel by pixel


def gshhg_fill(gs, lons, lats, want):
    """-> a bool raster: True where `want` and the pixel centre (lons[col], lats[row]) is inside gs (shapely.contains_xy,
    so a centre on gs's boundary is not inside). The same answer as testing every wanted pixel, found by a quadtree:
    a square whose centres' bounding box is properly inside gs is all land, one disjoint from gs is all sea (section
    104: a mainland block is millions of pixels, and most of them are far from any GSHHG shoreline)."""
    out = np.zeros(want.shape, bool)
    stack = [(0, want.shape[0], 0, want.shape[1])]
    while stack:
        r0, r1, c0, c1 = stack.pop()
        w = want[r0:r1, c0:c1]
        if not w.any():
            continue
        lo0, lo1 = float(min(lons[c0], lons[c1 - 1])), float(max(lons[c0], lons[c1 - 1]))
        la0, la1 = float(min(lats[r0], lats[r1 - 1])), float(max(lats[r0], lats[r1 - 1]))
        rect = sbox(lo0, la0, lo1, la1) if lo1 > lo0 and la1 > la0 else None
        if rect is not None and shapely.disjoint(gs, rect):
            continue
        if rect is not None and shapely.contains_properly(gs, rect):
            out[r0:r1, c0:c1] = w
            continue
        if r1 - r0 <= FILL_LEAF and c1 - c0 <= FILL_LEAF:
            rr, cc = np.nonzero(w)
            hit = shapely.contains_xy(gs, lons[c0 + cc], lats[r0 + rr])
            out[r0 + rr[hit], c0 + cc[hit]] = True
            continue
        rm, cm = (r0 + r1) // 2, (c0 + c1) // 2
        for q in ((r0, rm, c0, cm), (r0, rm, cm, c1), (rm, r1, c0, cm), (rm, r1, cm, c1)):
            if q[1] > q[0] and q[3] > q[2]:
                stack.append(q)
    return out


# ---------------------------------------------------------------- trace

def block_class(tiles, bx, by, v1_dir, v1_index, stats):
    """The zoom-15 class raster (BPX x BPX) of block bx/by by rule 1, or None when it can hold no land."""
    reg = tiles.reg
    X0, Y0 = bx * BPX, by * BPX
    lon_w, lon_e = float(px_lon(X0)), float(px_lon(X0 + BPX))
    lat_n, lat_s = float(px_lat(Y0)), float(px_lat(Y0 + BPX))
    b = raster_box(reg)
    if lon_e <= b[0] or lon_w >= b[2] or lat_n <= b[1] or lat_s >= b[3]:
        return None
    ex_here = [e for e in reg.get("exclude", ()) if not (lon_e <= e[0] or lon_w >= e[2] or lat_n <= e[1] or lat_s >= e[3])]
    parts, any_land, any_out = [], False, False
    for j in range(BLOCK):
        for i in range(BLOCK):
            x, y = bx * BLOCK + i, by * BLOCK + j
            if (x, y) in tiles.present[Z]:
                c = tiles.cls(Z, x, y)
                kind = "z15"
            else:
                a = tiles.ancestor(x, y)
                c = None if a is None else tiles.anc_slice(x, y, a)
                kind = "none" if a is None else "ancestor"
            stats[kind] += 1
            if c is None:
                any_out = True
            else:
                any_land = any_land or bool((c == LAND).any())
                any_out = any_out or bool((c == OUT).any())
            parts.append((i, j, c))
    gs = None
    if any_out or ex_here:
        gs = gshhg_near(v1_dir, v1_index, (max(lon_w, b[0]), max(lat_s, b[1]), min(lon_e, b[2]), min(lat_n, b[3])))
    if not any_land and gs is None:
        return None
    cls = np.full((BPX, BPX), OUT, np.uint8)
    for i, j, c in parts:
        if c is None:
            continue
        rep = T.TILE // c.shape[0]
        if rep > 1:
            c = np.repeat(np.repeat(c, rep, 0), rep, 1)
        cls[j * T.TILE:(j + 1) * T.TILE, i * T.TILE:(i + 1) * T.TILE] = c
    lons = px_lon(X0 + np.arange(BPX) + 0.5)
    lats = px_lat(Y0 + np.arange(BPX) + 0.5)
    inx = (lons >= b[0]) & (lons <= b[2])
    iny = (lats >= b[1]) & (lats <= b[3])
    inbox = iny[:, None] & inx[None, :]
    cls[~inbox] = OUT                          # outside the box: GSHHG's, added at the cell stage
    for e in ex_here:                          # an exclusion box (section 104: Puget Sound): GSHHG's, filled below
        cls[((lats >= e[1]) & (lats <= e[3]))[:, None] & ((lons >= e[0]) & (lons <= e[2]))[None, :]] = OUT
    if gs is not None:
        land = gshhg_fill(gs, lons, lats, (cls == OUT) & inbox)
        cls[land] = LAND
        stats["gshhg_px"] += int(land.sum())
    cls[~inbox] = OUT
    return cls


def trace_region(tiles, v1_dir, v1_index, log=log):
    """-> (the region's land as [(shell, [holes])] rings of lon/lat arrays (shell CCW, holes CW), stats).

    Every GEOS operation runs on integer zoom-15 pixel coordinates (the union of pixel rectangles, the speck filter, the
    simplifier, which keeps a subset of its input vertices), the union's result normalised before the simplifier. The
    lon/lat conversion is then point by point. The output is pinned from the runner's build (PIN_TIER1_SHA256): a
    build on another platform may differ, and only the runner's bytes are uploaded. (A first version clipped and unioned in lon/lat: the runner's build differed from the laptop's by a few
    dozen vertices, so its hash could not be pinned. Land never reaches a box edge in S1: the raster already stops at the
    box, by pixel centre.)"""
    t0 = time.time()
    reg = tiles.reg
    stats = dict(z15=0, ancestor=0, none=0, blocks=0, blocks_traced=0, gshhg_px=0)
    x0, y0, x1, y1 = T.tiles_in_box(raster_box(reg), Z - 3)
    polys = []
    for bx in range(x0, x1 + 1):
        for by in range(y0, y1 + 1):
            stats["blocks"] += 1
            cls = block_class(tiles, bx, by, v1_dir, v1_index, stats)
            if cls is None:
                continue
            land = cls == LAND
            if land.any():
                stats["blocks_traced"] += 1
                polys.extend(mask_polygons(land, bx * BPX, by * BPX))
    stats["t_blocks_s"] = round(time.time() - t0, 1)
    u = shapely.union_all(polys) if polys else shapely.MultiPolygon()
    speck, tol = reg.get("speck", SPECK_PX2), reg.get("tol", TOL_PX)       # a region's own params (section 104)
    kept, stats["speck_land"], stats["speck_holes"] = drop_specks(u, speck)
    # normalised first (G70 P2-1): the polygons, their rings and each ring's start vertex in a canonical order, so the
    # simplifier (anchored at the ring start) sees the same input whatever order GEOS's union produced on this platform
    simp = shapely.normalize(shapely.MultiPolygon(kept)).simplify(tol, preserve_topology=True) if kept else shapely.MultiPolygon()
    out = []
    for p in polygons(simp):
        rings = [np.asarray(p.exterior.coords, np.float64)[:-1]] + [np.asarray(r.coords, np.float64)[:-1] for r in p.interiors]
        ll = [(px_lon(r[:, 0]), px_lat(r[:, 1])) for r in rings]
        sh = ll[0] if BC.signed_area(*ll[0]) > 0 else (ll[0][0][::-1].copy(), ll[0][1][::-1].copy())
        holes = [h if BC.signed_area(*h) < 0 else (h[0][::-1].copy(), h[1][::-1].copy()) for h in ll[1:]]
        out.append((sh, holes))
    stats["anomaly_px"] = tiles.anomalies
    stats["t_total_s"] = round(time.time() - t0, 1)
    stats["polygons"] = len(out)
    stats["holes"] = sum(len(h) for _, h in out)
    log("trace %s: %s" % (tiles.name, stats))
    return out, stats


# ---------------------------------------------------------------- cells

def ring_q(coords, ccw):
    a = np.asarray(coords, np.float64)[:-1]
    r = BC.quantize_ring(a[:, 0], a[:, 1], Q2)
    if r is None:
        return None
    if (BC.signed_area(r[0].astype(np.float64), r[1].astype(np.float64)) > 0) != ccw:
        r = (r[0][::-1].copy(), r[1][::-1].copy())
    return r


def clip_to_cell(x, y, lo, la, deg):
    """One ring clipped to a cell with build_coast's exact Sutherland-Hodgman (the v1 cells' own clip), or None."""
    for axis, c, keep_le in ((0, lo, False), (0, lo + deg, True), (1, la, False), (1, la + deg, True)):
        a = x if axis == 0 else y
        if (a >= c).all() if not keep_le else (a <= c).all():
            continue
        r = BC.clip_half(x, y, axis, c, keep_le)
        if r is None:
            return None
        x, y = r
    return x, y


def cell_pieces_exact(parts, lo, la, deg):
    """Quantised pieces of a cell from [(shell, holes)] lon/lat rings: rings wholly inside the cell as they are, the rest
    clipped ring by ring (a hole goes with its shell). Arithmetic only, no GEOS: the same bytes on every platform."""
    pieces = []
    for sh, holes in parts:
        if sh[0].max() <= lo or sh[0].min() >= lo + deg or sh[1].max() <= la or sh[1].min() >= la + deg:
            continue
        c = clip_to_cell(sh[0], sh[1], lo, la, deg)
        if c is None:
            continue
        shq = BC.quantize_ring(c[0], c[1], Q2)
        if shq is None or BC.signed_area(shq[0].astype(np.float64), shq[1].astype(np.float64)) <= 0:
            continue
        rings = [shq]
        for h in holes:
            ch = clip_to_cell(h[0], h[1], lo, la, deg)
            hq = BC.quantize_ring(ch[0], ch[1], Q2) if ch is not None else None
            if hq is not None and BC.signed_area(hq[0].astype(np.float64), hq[1].astype(np.float64)) < 0:
                rings.append(hq)
        pieces.append(rings)
    return pieces


def sort_pieces(pieces):
    pieces.sort(key=lambda rs: (int(rs[0][0].min()), int(rs[0][1].min()), len(rs[0][0]), int(rs[0][0][0]), int(rs[0][1][0])))
    return pieces


def cell_pieces(geom):
    """Quantised pieces (shell CCW, holes CW) of a lon/lat geometry, sorted for determinism."""
    pieces = []
    for p in polygons(geom):
        sh = ring_q(p.exterior.coords, True)
        if sh is None:
            continue
        rings = [sh]
        for h in p.interiors:
            r = ring_q(h.coords, False)
            if r is not None:
                rings.append(r)
        pieces.append(rings)
    pieces.sort(key=lambda rs: (int(rs[0][0].min()), int(rs[0][1].min()), len(rs[0][0]), int(rs[0][0][0]), int(rs[0][1][0])))
    return pieces


def build(v1_dir, tiles_dir, out_dir, regions=None, log=log):
    regions = regions or REGIONS
    t0 = time.time()
    v1 = load_v1(v1_dir)
    os.makedirs(os.path.join(out_dir, v1["tier1"]["dir"]), exist_ok=True)
    region_geom, bathy, rstats = {}, {}, {}
    for name, reg in sorted(regions.items()):
        tiles = Tiles(tiles_dir, name, reg)
        sha, n = tiles.digest()
        region_geom[name], rstats[name] = trace_region(tiles, v1_dir, v1, log)
        bathy[name] = {"v": reg["v"], "z": Z, "zmin": reg["zmin"], "box": list(reg["box"]),
                       "digest": tiles.index.get("digest"), "tiles": n, "sha256": sha}
    boxes = shapely.union_all([sbox(*r["box"]) for r in regions.values()])
    boxes_line = boxes.boundary
    # where the traced land is used: the boxes less their exclusion boxes (G72 #3: Puget Sound keeps v1's own rings, cut
    # exactly on the exclusion box lines like a seam; the raster there is GSHHG's pixels only so the trace crosses them)
    excl = [sbox(*e) for r in regions.values() for e in r.get("exclude", ())]
    traced_area = boxes.difference(shapely.union_all(excl)) if excl else boxes
    v2_parts = [pt for name in sorted(region_geom) for pt in region_geom[name]]   # the boxes touch at a corner only
    candidates = sorted({c for r in regions.values() for c in cells_overlapping(r["box"])})
    listing, digests, changed, base_changed, crossing = {}, {}, [], {}, 0
    tot_bytes = tot_verts = 0
    cell_stats, split_cells = {}, []
    deg = v1["tier1"]["cell"]
    for name in sorted(set(v1["tier1"]["cells"]) | set(candidates)):
        src = os.path.join(v1_dir, v1["tier1"]["dir"], name + ".bin")
        buf = None
        if name in candidates:
            la, lo = (int(v) for v in name.split("_"))
            cbox = sbox(lo, la, lo + deg, la + deg)
            raw, g1, rings = v1_cell_geom(v1_dir, v1, name)
            n_cross = sum(1 for r in rings if shapely.LinearRing(r).intersects(boxes_line))
            crossing += n_cross
            v2_here = cell_pieces_exact(v2_parts, lo, la, deg)
            d1 = BC.decode_file(raw) if raw is not None else {"pieces": [], "q": BC.Q}
            v1_hit = [bool(shapely.intersects(Polygon(np.column_stack([pc["rings"][0][0], pc["rings"][0][1]]) / d1["q"]), boxes))
                      for pc in d1["pieces"]]
            seam_here = any(r.get("margin_px") and shapely.intersects(cbox, sbox(*r["box"])) for r in regions.values())
            if v2_here or any(v1_hit):
                if n_cross == 0 and not seam_here:
                    # the exact path: v2's pieces + the v1 pieces outside every box, v1's integers scaled to the new quantum
                    k = Q2 // d1["q"]
                    pieces = sort_pieces(v2_here + [[(ix * k, iy * k) for ix, iy in pc["rings"]]
                                                    for pc, hit in zip(d1["pieces"], v1_hit) if not hit])
                else:
                    # a GSHHG ring crosses a box edge (none in S1): GSHHG outside the boxes and v2 inside joined by GEOS
                    log("cell %s: %d v1 rings cross a box edge: joined with GEOS (not bit-exact across platforms)" % (name, n_cross))
                    v2g = [Polygon(np.column_stack(sh), [np.column_stack(h) for h in hs]) for sh, hs in v2_parts
                           if sh[0].max() >= lo and sh[0].min() <= lo + deg and sh[1].max() >= la and sh[1].min() <= la + deg]
                    geom = shapely.union_all([shapely.union_all(v2g).intersection(traced_area)] +
                                             ([g1.difference(traced_area)] if g1 is not None else []))
                    geom = geom.intersection(cbox)
                    if seam_here:
                        geom, n_cut = split_big(geom)
                        if n_cut:
                            split_cells.append(name)
                    pieces = cell_pieces(geom)
                changed.append(name)
                base_changed[name] = hashlib.sha256(raw).hexdigest() if raw is not None else None
                nh = sum(len(pc) - 1 for pc in pieces)
                cell_stats[name] = {"pieces": len(pieces), "holes": nh, "v1_rings_crossing_box": n_cross}
                if pieces:
                    buf = BC.encode_file(pieces, deg, q=Q2)
                    log("cell %s: %d pieces, %d holes, %d vertices, %d bytes (v1 %s bytes)" % (
                        name, len(pieces), nh, sum(len(r[0]) for pc in pieces for r in pc), len(buf),
                        len(raw) if raw is not None else "-"))
        if buf is None and name not in changed and name in v1["tier1"]["cells"]:
            buf = open(src, "rb").read()
        if buf is None:
            continue
        open(os.path.join(out_dir, v1["tier1"]["dir"], name + ".bin"), "wb").write(buf)
        d = BC.decode_file(buf)
        nv = sum(len(r[0]) for pc in d["pieces"] for r in pc["rings"])
        listing[name] = [len(buf), nv]
        digests[name] = hashlib.sha256(buf).hexdigest()
        tot_bytes += len(buf)
        tot_verts += nv
    t0f = v1["tier0"]["file"]
    tier0 = open(os.path.join(v1_dir, t0f), "rb").read()
    open(os.path.join(out_dir, t0f), "wb").write(tier0)
    index = {k: v for k, v in v1.items() if k not in ("tier1",)}
    index.update({
        "kind": KIND, "set": SET, "format": "coast-v1", "format_key": format_key(regions), "q": Q2,
        "q_note": "q of the changed cells; tier 0 and every other cell keep v1's 10000: each file carries its own q",
        "source": source(regions), "license": LICENSE,
        "built_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "base_v1": {"tier0_sha": v1["tier0"]["sha256"], "tier1_sha": v1["tier1"]["sha256"], "changed_sha": base_changed},
        "bathy": bathy, "changed": changed, "v1_rings_crossing_box": crossing,
        **({"split": {"deg": SPLIT_DEG, "min_vertices": SPLIT_VERTS, "cells": split_cells,
                      "note": "pieces cut on these lines in these cells: edges along them are not coast"}}
           if any(r.get("margin_px") for r in regions.values()) else {}),
        "stats": {"regions": rstats, "cells": cell_stats},
        "versions": {"shapely": shapely.__version__, "numpy": np.__version__},
        "tier1": {"dir": v1["tier1"]["dir"], "res": v1["tier1"]["res"], "cell": deg, "min_zoom": v1["tier1"]["min_zoom"],
                  "cells": listing, "bytes": tot_bytes, "vertices": tot_verts, "sha256": BC.cells_digest(digests)},
    })
    with open(os.path.join(out_dir, "index.json"), "w") as fh:
        json.dump(index, fh, separators=(",", ":"), sort_keys=True)
    with open(os.path.join(out_dir, "LICENSE.txt"), "w", encoding="utf-8", newline="\n") as fh:
        fh.write(open(BC.LICENSE_FILE, encoding="utf-8").read().rstrip("\n") + "\n" + NOAA_CREDIT)
    log("%s: %d cells changed %s, %d v1 rings cross a box edge; tier 1 %d cells, %d vertices, %d bytes, sha %s (%.0f s)"
        % (SET, len(changed), changed, crossing, len(listing), tot_verts, tot_bytes, index["tier1"]["sha256"][:12],
           time.time() - t0))
    return index


# ---------------------------------------------------------------- check

def _pinned(value):
    return value is not None


def is_real(index, regions=None):
    """Built from the pinned inputs (never judged from the result)."""
    regions = regions or REGIONS
    b = index.get("bathy") or {}
    base = index.get("base_v1") or {}
    if set(b) != set(regions) or base.get("tier0_sha") != V1_PIN_TIER0_SHA256 or base.get("tier1_sha") != V1_PIN_TIER1_SHA256:
        return False
    for name, reg in regions.items():
        e = b[name]
        if e.get("v") != reg["v"] or e.get("digest") != reg["digest"]:
            return False
        if _pinned(reg["tiles_sha256"]) and e.get("sha256") != reg["tiles_sha256"]:
            return False
    return True


def check(out_dir, log=log, regions=None):
    regions = regions or REGIONS
    index = json.load(open(os.path.join(out_dir, "index.json")))
    problems = []
    if index.get("kind") != KIND:
        return ["index kind %r is not %r" % (index.get("kind"), KIND)]
    if index.get("format") != "coast-v1" or index.get("q") != Q2:
        problems.append("index format/q")
    if index.get("format_key") != format_key(regions):
        problems.append("index format_key %r != this builder's %r" % (index.get("format_key"), format_key(regions)))
    real = is_real(index, regions)
    base = index.get("base_v1") or {}
    t0 = index["tier0"]
    buf = open(os.path.join(out_dir, t0["file"]), "rb").read()
    sha0 = hashlib.sha256(buf).hexdigest()
    if len(buf) != t0["bytes"] or sha0 != t0["sha256"]:
        problems.append("tier 0 size/hash mismatch")
    if sha0 != base.get("tier0_sha"):
        problems.append("tier 0 is not v1's bytes")
    d0 = BC.decode_file(buf)
    for lon, lat, land in BC.PROBES if real else []:
        if BC.point_in_pieces(lon, lat, d0["pieces"], d0["q"]) != land:
            problems.append("tier 0: probe %s,%s should be %s" % (lon, lat, "land" if land else "water"))
    t1 = index["tier1"]
    changed = set(index.get("changed") or ())
    present = set(n[:-4] for n in os.listdir(os.path.join(out_dir, t1["dir"])) if n.endswith(".bin"))
    if present != set(t1["cells"]):
        problems.append("tier 1: files %d vs index %d" % (len(present), len(t1["cells"])))
    if real and sorted(changed) != sorted(EXPECTED_CHANGED):
        problems.append("changed cells %s != the expected %s" % (sorted(changed), EXPECTED_CHANGED))
    digests, v1_digests, area = {}, dict(base.get("changed_sha") or {}), 0.0
    n_holes = 0
    for name, (nbytes, nverts) in sorted(t1["cells"].items()):
        buf = open(os.path.join(out_dir, t1["dir"], name + ".bin"), "rb").read()
        digests[name] = hashlib.sha256(buf).hexdigest()
        if name not in changed:
            v1_digests[name] = digests[name]
        if len(buf) != nbytes:
            problems.append("tier 1 %s: size mismatch" % name)
        lat0, lon0 = (int(v) for v in name.split("_"))
        problems += ["tier 1 %s: %s" % (name, p) for p in BC.check_file(buf, t1["cell"], (lat0, lon0, t1["cell"]),
                                                                         ccw_only=False)]
        try:
            d = BC.decode_file(buf)
        except Exception:                             # noqa: BLE001  (already reported by check_file)
            continue
        if sum(len(r[0]) for pc in d["pieces"] for r in pc["rings"]) != nverts:
            problems.append("tier 1 %s: vertex count != index" % name)
        want_q = Q2 if name in changed else BC.Q
        if d["q"] != want_q:
            problems.append("tier 1 %s: q %d, expected %d" % (name, d["q"], want_q))
        if name in changed:
            q = d["q"]
            big = max((sum(len(r[0]) for r in pc["rings"]) for pc in d["pieces"]), default=0)
            if len(buf) > MAX_CELL_BYTES or nverts > MAX_CELL_VERTS or big > MAX_PIECE_VERTS:
                problems.append("tier 1 %s: %d bytes / %d vertices / biggest piece %d over the size gates (%d / %d / %d)"
                                % (name, len(buf), nverts, big, MAX_CELL_BYTES, MAX_CELL_VERTS, MAX_PIECE_VERTS))
            for k, pc in enumerate(d["pieces"]):
                rings = pc["rings"]
                if BC.signed_area(rings[0][0].astype(np.float64), rings[0][1].astype(np.float64)) <= 0:
                    problems.append("tier 1 %s: piece %d shell not counter-clockwise" % (name, k))
                shell = None
                for ix, iy in rings[1:]:
                    shell = shell or Polygon(np.column_stack([rings[0][0], rings[0][1]])).buffer(0.5)
                    n_holes += 1
                    if BC.signed_area(ix.astype(np.float64), iy.astype(np.float64)) >= 0:
                        problems.append("tier 1 %s: piece %d hole not clockwise" % (name, k))
                    if not shell.covers(Polygon(np.column_stack([ix, iy]))):
                        problems.append("tier 1 %s: piece %d hole outside its shell" % (name, k))
                area += sum(BC.signed_area(ix / q, iy / q) for ix, iy in rings)
                if len(problems) > 60:
                    break
        for lon, lat, land in (BC.PROBES + SET_PROBES) if real else []:
            if lat0 <= lat < lat0 + t1["cell"] and lon0 <= lon < lon0 + t1["cell"] and \
                    BC.point_in_pieces(lon, lat, d["pieces"], d["q"]) != land:
                problems.append("tier 1 %s: probe %s,%s should be %s" % (name, lon, lat, "land" if land else "water"))
    for lon, lat, land in (BC.PROBES + SET_PROBES) if real else []:
        name = BC.cell_name(int(np.floor(lat / t1["cell"])) * t1["cell"], int(np.floor(lon / t1["cell"])) * t1["cell"])
        if land and name not in t1["cells"]:
            problems.append("tier 1: probe %s,%s should be land but its cell %s is absent" % (lon, lat, name))
    # unchanged cells are v1's bytes: v1's own tier-1 digest rebuilt from them + the replaced cells' v1 hashes
    v1_digests = {k: v for k, v in v1_digests.items() if v is not None}
    if BC.cells_digest(v1_digests) != base.get("tier1_sha"):
        problems.append("tier 1: the unchanged cells + base_v1.changed_sha do not rebuild v1's tier-1 hash")
    if present == set(t1["cells"]) and t1.get("sha256") != BC.cells_digest(digests):
        problems.append("tier 1: content hash != index (tier1.sha256)")
    if real:
        for name, sha in sorted(SAME_CELLS.items()):          # Hawaii stays as published in v2 (plan section 104)
            if digests.get(name) != sha:
                problems.append("tier 1 %s: not byte-identical to the published v2 cell" % name)
        for name, reg in regions.items():
            if not _pinned(reg["tiles_sha256"]):
                log("  note: %s tiles_sha256 not pinned yet (%s)" % (name, index["bathy"][name]["sha256"]))
        near = 0                                          # v2 vertices within ~1 zoom-15 px (5e-5 deg) of a box line (G70 P3-4)
        tight = [r for r in regions.values() if not r.get("margin_px")]   # a seam region's box lines are measured below instead
        # a seam region's box lines and its exclusion boxes' lines (where its traced shore meets GSHHG's inside the raster)
        seam_boxes = [b for r in regions.values() if r.get("margin_px") for b in [r["box"]] + list(r.get("exclude", ()))]
        seam_steps = []                                   # (cell, length m) of output edges lying along a seam box line
        # the edges along each seam box line, by cell, as (lo, hi, direction) intervals on the line: a split cut (rung
        # a) along a box line runs both ways (land on both sides) and cancels; a step between the two shores does not
        seam_iv = {}
        for name in changed:
            d = BC.decode_file(open(os.path.join(out_dir, t1["dir"], name + ".bin"), "rb").read())
            for pc in d["pieces"]:
                for ix, iy in pc["rings"]:
                    x, y = ix / d["q"], iy / d["q"]
                    for r in tight:
                        b0, b1, b2, b3 = r["box"]
                        inside = (x >= b0 - 5e-5) & (x <= b2 + 5e-5) & (y >= b1 - 5e-5) & (y <= b3 + 5e-5)
                        edge = (np.abs(x - b0) < 5e-5) | (np.abs(x - b2) < 5e-5) | (np.abs(y - b1) < 5e-5) | (np.abs(y - b3) < 5e-5)
                        near += int((inside & edge).sum())
                    xn, yn = np.roll(x, -1), np.roll(y, -1)
                    for b0, b1, b2, b3 in seam_boxes:
                        for along, line, lo_, hi_ in ((x, b0, b1, b3), (x, b2, b1, b3), (y, b1, b0, b2), (y, b3, b0, b2)):
                            here = y if along is x else x          # the coordinate running along the line
                            other = yn if along is x else xn
                            nxt = xn if along is x else yn
                            mid = (here + other) / 2
                            tol = 0.6 / d["q"]                 # the line as the cell's quantum rounds it
                            on = (np.abs(along - line) < tol) & (np.abs(nxt - line) < tol) & (mid >= lo_) & (mid <= hi_)
                            key = (name, "x" if along is x else "y", line)
                            for k in np.nonzero(on)[0]:
                                a, b = float(here[k]), float(other[k])
                                if a != b:
                                    seam_iv.setdefault(key, []).append((min(a, b), max(a, b), b > a))
        seam_steps = seam_step_lengths(seam_iv)
        if seam_boxes:
            longest = max((m for _, m in seam_steps), default=0.0)
            log("  seam box lines: %d edges along them, longest %.0f m" % (len(seam_steps), longest))
            if longest > SEAM_STEP_MAX_M:
                problems.append("a %.0f m edge lies along a seam box line (more than %d m: the two shores disagree there)"
                                % (longest, SEAM_STEP_MAX_M))
        if near:
            problems.append("%d v2 vertices lie within 5e-5 deg of a region box line (none expected in S1)" % near)
        if index.get("v1_rings_crossing_box") != 0 and not seam_boxes:
            problems.append("%s v1 rings cross a box edge (expected 0 for S1)" % index.get("v1_rings_crossing_box"))
        if _pinned(V1_CHANGED_SQDEG):
            if abs(area - V1_CHANGED_SQDEG) > AREA_TOL * V1_CHANGED_SQDEG:
                problems.append("changed cells: land area %.5f sq deg, v1 %.5f (more than %d %% apart)"
                                % (area, V1_CHANGED_SQDEG, AREA_TOL * 100))
        else:
            log("  note: V1_CHANGED_SQDEG not pinned yet")
        if _pinned(PIN_TIER1_SHA256):
            if t1.get("sha256") != PIN_TIER1_SHA256:
                problems.append("pin: tier 1 %s != the pinned %s (a deliberate change updates PIN_TIER1_SHA256)"
                                % (str(t1.get("sha256"))[:12], PIN_TIER1_SHA256[:12]))
        else:
            log("  note: PIN_TIER1_SHA256 not pinned yet (%s)" % t1.get("sha256"))
    log("check (land2): tier 0 = v1 %s; tier 1 %d cells (%d changed, %d holes) / %d vertices / %d B; changed area "
        "%.5f sq deg; real-build gates %s; %d problems" % (
            sha0[:12], len(t1["cells"]), len(changed), n_holes, t1["vertices"], t1["bytes"], area,
            "on" if real else "OFF", len(problems)))
    for p in problems[:50]:
        log("  " + p)
    return problems


# ---------------------------------------------------------------- main

def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--v1", help="a coast-v1 land set directory (build_coast.py output, or fetched with --fetch)")
    ap.add_argument("--tiles", help="bathymetry tiles directory: <region>/v<N>/{index.json,t/z/x/y.png}")
    ap.add_argument("--fetch", action="store_true", help="download the pinned tiles (and v1 when --v1 is empty) first")
    ap.add_argument("--out", help="output directory (build) or directory to upload")
    ap.add_argument("--check", metavar="DIR", help="validate a built v2 directory")
    ap.add_argument("--upload", metavar="PREFIX", help="upload --out to the R2 bucket under PREFIX")
    ap.add_argument("--set", choices=sorted(SETS), default="v2", help="the coast set to build, check or upload")
    ap.add_argument("--replace", action="store_true", help="allow overwriting a different build under PREFIX")
    a = ap.parse_args(argv)
    if a.set != SET:
        use_set(a.set)
    if a.check:
        return 1 if check(a.check) else 0
    if a.upload:
        if not a.out:
            ap.error("--upload needs --out")
        if a.upload.strip("/") != "static/coast/" + SET:          # the prefix is the set's own (G71 P3-4)
            print("refusing to upload set %s under %r (its prefix is static/coast/%s)" % (SET, a.upload, SET))
            return 1
        if check(a.out):
            print("refusing to upload a directory that fails --check")
            return 1
        index = json.load(open(os.path.join(a.out, "index.json")))
        if not is_real(index) or not all(_pinned(r["tiles_sha256"]) for r in REGIONS.values()):
            print("refusing to upload a build that is not from the pinned bathymetry and v1 inputs")
            return 1
        if not _pinned(PIN_TIER1_SHA256) or index["tier1"]["sha256"] != PIN_TIER1_SHA256:
            print("refusing to upload a build whose tier-1 hash is not PIN_TIER1_SHA256")
            return 1
        return 0 if BC.upload(a.out, a.upload.strip("/"), replace=a.replace,
                              license_file=os.path.join(a.out, "LICENSE.txt")) else 1
    if not (a.v1 and a.tiles and a.out):
        ap.error("--v1, --tiles and --out are required to build")
    if a.fetch:
        if not os.path.exists(os.path.join(a.v1, "index.json")):
            fetch_v1(a.v1)
        fetch_tiles(a.tiles)
    index = build(a.v1, a.tiles, a.out)
    probs = check(a.out)
    if not is_real(index):                            # G70 P3-1: a default build must be from the pinned inputs
        print("the build is not from the pinned v1 / bathymetry inputs: the real-build gates were off")
        return 1
    return 1 if probs else 0


if __name__ == "__main__":
    sys.exit(main())
