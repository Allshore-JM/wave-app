"""Bathymetry sources and regions (plan section 93b). Data only.

A SOURCE is one gridded elevation set read through GDAL (a VRT over NOAA's open S3 bucket, or a local file in
tests); a REGION is a box of the map built from its sources, best first, as one versioned tile set
(static/bathy/<region>/vN/). The CUDEM regions' boxes do not overlap; GEBCO lies under all of them (zooms 0-7). The
1-m lidar regions (plan section 96: "-lidar", prio 20, zooms 16-17; the NW Hawaiian Islands 8-17) lie on top of them:
the client stacks regions by prio, and a pixel the higher region has no picture for falls through to the next.

A region's `fill` sources are read and merged under its own (the lidar's holes and the water just beyond it take their
values) but never decide which blocks are read and never earn a zoom beyond their own `zmax`; inside the lidar's edge the
depth blends from the lidar to the fill (flagged "blended"): over FEATHER_PX pixels at the survey's edge and round
big holes, narrower round small holes (build_bathy.feather). Where two of a region's own surveys meet, the older one
fades in under the newer one by the same rule (build_bathy.read_field).

Elevations are metres above the source's vertical datum, positive up. Every stage-1 source sits on a local
mean-sea-level datum (checked 2026-11-11: Hawaii LMSL, Guam GUVD04, CNMI NMVD03, American Samoa ASVD02,
Puerto Rico PRVD02, USVI VIVD09; the Hawaii, Guam and Puerto Rico 1-m lidar agreed with CUDEM to 0.03-0.34 m),
so `offset` (added to bring a source to ~MSL) is 0 for all of them.

Two 2021 NOAA NGS lidar sets (Niihau, the NW Hawaiian Islands) are NAD83(PA11) ELLIPSOID heights, ~14-16 m above sea
level. `geoid` "egm2008" subtracts the EGM2008 geoid height N (PROJ's us_nga_egm08_25.tif, read bilinear onto the block's
grid) and `offset` -0.3 brings them to local mean sea level: at Niihau (ell - N) minus USACE's 2013 LMSL survey read
+0.30..+0.37 m (plan section 96 step 1; owner decision: convert both). The NW Islands have no sea-level survey to check
against, so their depths are approximate.

`mhhw` is the region's mean higher high water above mean sea level, rounded up a little: a pixel is water when its
elevation is at or below it AND it is connected to the open sea (tools/bathy/bathy_water.py). From NOAA's tide datums:
Honolulu +0.27 m, Hilo +0.36, Apra Harbor (Guam) ~+0.35, Pago Pago +0.43, San Juan ~+0.17, Charlotte Amalie ~+0.14.

Source ids are 1-29 (30 marks land on the source plane: bathy_tiles.LAND_ID; 31 = GEBCO); a lower id wins where two
sources overlap, so the finer source of a region has the lower id.

`zmax` is the finest zoom a source's pixels earn: a tile is written at a zoom only where a water pixel of it comes from a
source that earns that zoom (the client draws the next coarser tile, enlarged, elsewhere). `deep` = (metres, zoom): the
source's pixels deeper than that earn only that zoom (deep open water needs no 10-m tiles).

Digital Earth Pacific's Sentinel-2 satellite-derived depths (plan section 97; kind "dep": the builder writes a GDAL VRT over
DEP's 96-km COG tiles, tools/bathy/bathy_dep.py) form ONE region across the Pacific, drawn between GEBCO and the US
surveys (prio 5). Every pixel DEP gives is water (`water_all`: DEP masked land itself; the estimate stops at ~26 m, so
the deep-water connectivity rule would find no open sea; a value at or above 0 m is never drawn); water deeper than
`max_depth` 12 m is left out so GEBCO shows there (owner, 2026-11-15: past ~12 m the estimate saturates). The cap applies
to the value after the warp averages DEP's 10-m pixels onto the tile grid (G64a F2), so a few pixels a little deeper than
12 m can show and a shallow pixel beside deep water can drop out. The four turbid Torres Strait / Gulf of Papua tiles are
left out (`skip_tiles`), and nothing is drawn inside the US regions' boxes (`exclude`, owner 2026-11-11: their own
surveys). No bias correction (owner, 2026-11-15): DEP's numbers as published.

The Great Lakes (plan section 98) have no CUDEM and GSHHG counts them as land (no GEBCO there). Each lake is two regions
over the same box: the lake's NOAA NCEI bathymetry grid (kind "lakegrid": ~90 m, downloaded once per build by
tools/bathy/bathy_lakes.py, zooms 5-11) and, over it (prio 12, zooms 12-15), the lake's 1-m USACE / NOAA lidar DEMs read to
zoom 15, the grid filling their holes. Depths are below the lake's Low Water Datum (IGLD 1985: Superior 183.2 m,
Michigan-Huron 176.0, St Clair 174.4, Erie 173.5, Ontario 74.2), the lake's chart datum: the grids are already relative to
it, the IGLD85 lidar subtracts it (`offset`), and the two NAVD88 sets (Whitefish Bay 2023, the upper Lake Michigan islands
2016) subtract it plus 0.01 m. Lake St Clair has no grid of its own: Lake Erie's covers it, 0.9 m lower (St Clair's datum is
0.9 m above Erie's). `mhhw` is the lake's mean level above its datum (NOAA's monthly means, 1991-2020, rounded up: the
water's edge), and the open water is seeded from the published Great Lakes shorelines (seed "lake"), so the shallow west
basin of Erie and Lake St Clair need no water deeper than 20 m. Boxes cut at the straits so they do not overlap (the Huron
box leaves out the Ontario one; the Erie box the St Clair one).
"""

import os

BUCKET = "noaa-nos-coastal-lidar-pds"
CUDEM_DOI = "doi:10.25921/ds9v-ky35"

CUDEM = dict(kind="vrt", zmax=15, res_m=3, licence="public domain (NOAA)", offset=0.0,
             name="NOAA NCEI CUDEM 1/9 arc-second topobathy",
             credit="NOAA NCEI Continuously Updated DEM (CUDEM), " + CUDEM_DOI)

# NOAA's 1/3 arc-second (~10 m) CUDEM: the same compilation as the 1/9 arc-second grid and its complement (checked
# 2026-11-12: over their overlap the two differ by a median 0.01 m; at the Diamond Head hole of the 1/9 grid the 1/3 set
# holds 93 %). It fills the 1/9 grid's holes and reaches further offshore, mostly deep water (owner decision 2026-11-12).
# Read bilinear (it is coarser than the zoom-15 grid). Year = NOAA's publication of most of the set's files.
CUDEM3 = dict(CUDEM, zmax=14, res_m=10, deep=(200.0, 12), name="NOAA NCEI CUDEM 1/3 arc-second topobathy")

# NOAA's 1-m topobathy lidar DEMs (plan section 96), on the same bucket. Names end in "lidar" and credits start with the
# agency (the client's short credit is the credit's first word). Read average when finer than the grid (always at z17).
# survey_fade: where two of a region's lidar surveys meet, the older fades in under the newer (owner, 2026-11-14);
# the CUDEM grids of the stage-1 regions are not marked, so their merge is unchanged.
LIDAR = dict(kind="vrt", zmax=17, res_m=1, licence="public domain (US government)", offset=0.0, survey_fade=True)
USACE13 = dict(LIDAR, year="2013", name="USACE lidar", datum="LMSL (Hawaii local mean sea level)",
               nodata=-3.4028234663852886e38,
               credit="USACE National Coastal Mapping Program 2013 Hawaii topobathy lidar (NOAA Digital Coast)")
EGM2008 = dict(geoid="egm2008", offset=-0.3,
               datum="NAD83(PA11) ellipsoid heights to mean sea level: EGM2008 geoid, -0.3 m (approximate)")

# The Lower 48 (plan section 98). Every mainland source is in NAVD88; its heights become heights above mean sea level by
# subtracting the committed grid of mean sea level above NAVD88 (GEOIDS "msl_navd88", from NOAA CO-OPS station datums).
# The national CUDEM sets cover every coast in one VRT each (two files per resolution, split only by how NOAA tagged
# their CRS); `clip` = True keeps the part inside the region's box, a (west, south, east, north) box keeps the part inside
# both (a fill that only matters where CUDEM has a gap). Within a region the lowest id wins at a pixel: CUDEM 1/9, then
# the 1-m surveys read to zoom 15 in CUDEM's West Coast gaps, then BlueTopo 4 m, CUDEM 1/3, BlueTopo 8 m and 16 m (owner
# decision 2026-11-15, rule A: CUDEM 1/9 everywhere to zoom 15, BlueTopo to zoom 15 only to 100 m, CUDEM 1/3 to zoom 14
# down to 200 m).
NAVD88 = dict(geoid="msl_navd88", offset=0.0,
              datum="NAVD88 to mean sea level: grid from NOAA CO-OPS tide-station datums (about 0.1 m)")
CUDEM_US = dict(CUDEM, **NAVD88, clip=True, year="2014-2024")
CUDEM3_US = dict(CUDEM3, **NAVD88, clip=True, year="2014-2024")
BLUETOPO = dict(NAVD88, kind="bluetopo", clip=True, nodata=float("nan"), year="2020-2026", licence="CC0 (NOAA)")
# The 1-m surveys used as baseline fill where CUDEM 1/9 has nothing on the West Coast (Southern California, Northern
# California north of 38.75 N, Oregon south of 43.5 N), read down to zoom 15 like the 3-m grid.
SURVEY15 = dict(NAVD88, kind="vrt", zmax=15, res_m=1, licence="public domain (US government)")

# The Great Lakes (plan section 98). LWD = each lake's Low Water Datum, IGLD 1985 metres.
LAKE_LWD = {"superior": 183.2, "michigan": 176.0, "huron": 176.0, "stclair": 174.4, "erie": 173.5, "ontario": 74.2}
LAKEGRID = dict(kind="lakegrid", id=20, zmax=11, res_m=90, nodata=-9999.0, clip=True, offset=0.0,
                datum="Low Water Datum (IGLD 1985)", name="NOAA NCEI Great Lakes bathymetry",
                licence="public domain (NOAA)")
NAVD88_IGLD85_M = 0.01      # NAVD88 heights read 0.01 m above IGLD 1985 at the lake surveys checked (plan section 98)
USACE_NCMP = "USACE National Coastal Mapping Program %s topobathy lidar (NOAA Digital Coast)"


def lake_dem(lwd, navd88=False):
    """A lake's 1-m lidar DEM read to zoom 15 (heights in IGLD 1985, or NAVD88 when `navd88`), brought to depth below
    the lake's Low Water Datum `lwd` by `offset`."""
    if navd88:
        return dict(SURVEY15_LAKE, lake_lwd=lwd, offset=-round(lwd + NAVD88_IGLD85_M, 3), nodata=-3.4028234663852886e38,
                    datum="NAVD88 to the lake's Low Water Datum (IGLD 1985 %.1f m, less %.2f m)" % (
                        lwd, NAVD88_IGLD85_M))
    return dict(SURVEY15_LAKE, lake_lwd=lwd, offset=-lwd, nodata=-3.4028234663852886e38,
                datum="IGLD 1985 to the lake's Low Water Datum (%.1f m)" % lwd)


SURVEY15_LAKE = dict(LIDAR, zmax=15, clip=True)

# The Lower 48's 1-m lidar (plan section 98, step 6): NAVD88 like the base map, cut to each lidar region's box, and
# (except Florida) kept to 0-30 m: deeper than 30 m a pixel is drawn only to zoom 15, which the base region draws.
LIDAR98_ALL = dict(LIDAR, **NAVD88, clip=True)
LIDAR98 = dict(LIDAR98_ALL, deep=(30.0, 15))
NOAA_CA11 = "NOAA Office for Coastal Management 2011 California topobathy merge (NOAA Digital Coast)"

SOURCES = {
    "cudem-hawaii": dict(CUDEM, id=10, year="2021",
                         path="dem/NCEI_ninth_Topobathy_Hawaii_9428/NCEI_ninth_Topobathy_Hawaii_EPSG-4326.vrt",
                         datum="LMSL (Hawaii local mean sea level)"),
    "cudem-guam": dict(CUDEM, id=10, year="2022",
                       path="dem/NCEI_ninth_Topobathy_Guam_9462/NCEI_ninth_Topobathy_Guam_m9462_EPSG-6325.vrt",
                       datum="GUVD04 (Guam local mean sea level)"),
    "cudem-cnmi": dict(CUDEM, id=11, year="2022",
                       path="dem/NCEI_ninth_Topobathy_CNMI_9560/NCEI_ninth_Topobathy_CNMI_EPSG-4269.vrt",
                       datum="NMVD03 (Northern Marianas local mean sea level)"),
    "cudem-samoa": dict(CUDEM, id=10, year="2021",
                        path="dem/NCEI_ninth_Topobathy_AmSam_9460/NCEI_ninth_Topobathy_AmSam_EPSG-4269.vrt",
                        datum="ASVD02 (American Samoa local mean sea level)"),
    "cudem-pr": dict(CUDEM, id=10, year="2022",
                     path="dem/NCEI_ninth_Topobathy_PuertoRico_9525/NCEI_ninth_Topobathy_PuertoRico_EPSG-4269.vrt",
                     datum="PRVD02 (Puerto Rico local mean sea level)"),
    "cudem-usvi": dict(CUDEM, id=11, year="2022",
                       path="dem/NCEI_ninth_Topobathy_USVI_9529/NCEI_ninth_Topobathy_USVI_EPSG-4269.vrt",
                       datum="VIVD09 (Virgin Islands local mean sea level)"),
    "cudem3-hawaii": dict(CUDEM3, id=20, year="2021",
                          path="dem/NCEI_third_Topobathy_Hawaii_9429/NCEI_third_Topobathy_Hawaii_EPSG-4326.vrt",
                          datum="LMSL (Hawaii local mean sea level)"),
    "cudem3-guam": dict(CUDEM3, id=20, year="2022",
                        path="dem/NCEI_third_Topobathy_Guam_9463/NCEI_third_Topobathy_Guam_m9463_EPSG-6325.vrt",
                        datum="GUVD04 (Guam local mean sea level)"),
    "cudem3-cnmi": dict(CUDEM3, id=21, year="2022",
                        path="dem/NCEI_third_Topobathy_CNMI_9561/NCEI_third_Topobathy_CNMI_m9561_EPSG-6325.vrt",
                        datum="NMVD03 (Northern Marianas local mean sea level)"),
    "cudem3-samoa": dict(CUDEM3, id=20, year="2021",
                         path="dem/NCEI_third_Topobathy_AmSam_9461/NCEI_third_Topobathy_AmSam_EPSG-4269.vrt",
                         datum="ASVD02 (American Samoa local mean sea level)"),
    "cudem3-pr": dict(CUDEM3, id=20, year="2022",
                      path="dem/NCEI_third_Topobathy_PuertoRico_9524/NCEI_third_Topobathy_PuertoRico_EPSG-4269.vrt",
                      datum="PRVD02 (Puerto Rico local mean sea level)"),
    "cudem3-usvi": dict(CUDEM3, id=21, year="2022",
                        path="dem/NCEI_third_Topobathy_USVI_9528/NCEI_third_Topobathy_USVI_EPSG-4269.vrt",
                        datum="VIVD09 (Virgin Islands local mean sea level)"),
    # Hawaii lidar: the newest survey wins (owner decision): NGS Niihau 2021 first, USACE 2013 fills its holes and
    # fades in at its edge (the gap-size rule, owner 2026-11-14).
    "ngs-niihau21": dict(LIDAR, **EGM2008, id=1, year="2021", name="NOAA NGS lidar", nodata=-999999.0,
                         path="dem/NGS_Niihau_HI_Topobathy_2021_10193/NGS_Niihau_HI_Topobathy_2021_m10193_EPSG-6634.vrt",
                         credit="NOAA NGS 2021 Niihau topobathy lidar (NOAA Digital Coast)"),
    "usace-oahu13": dict(USACE13, id=2,
                         path="dem/USACE_Oahu_HI_LMSL_DEM_2013_9365/USACE_Oahu_HI_LMSL_DEM_2013_m9365_EPSG-6322.vrt"),
    "usace-kauai13": dict(USACE13, id=3,
                          path="dem/USACE_Kauai_HI_LMSL_DEM_2013_9335/USACE_Kauai_HI_LMSL_DEM_2013_m9335_EPSG-6322.vrt"),
    "usace-maui13": dict(USACE13, id=4,
                         path="dem/USACE_Maui_HI_LMSL_DEM_2013_9337/USACE_Maui_HI_LMSL_DEM_2013_m9337_EPSG-6322.vrt"),
    "usace-molokai13": dict(USACE13, id=5, path="dem/USACE_Molokai_HI_LMSL_DEM_2013_9344/"
                                                "USACE_Molokai_HI_LMSL_DEM_2013_m9344_EPSG-6322.vrt"),
    "usace-lanai13": dict(USACE13, id=6,
                          path="dem/USACE_Lanai_HI_LMSL_DEM_2013_9336/USACE_Lanai_HI_LMSL_DEM_2013_m9336_EPSG-6322.vrt"),
    "usace-bigisland13": dict(USACE13, id=7, path="dem/USACE_Big_Island_HI_LMSL_DEM_2013_9326/"
                                                  "USACE_Big_Island_HI_LMSL_DEM_2013_m9326_EPSG-6318.vrt"),
    "usace-niihau13": dict(USACE13, id=8, path="dem/USACE_HI_Niihau_LMSL_DEM_2013_9002/"
                                               "USACE_HI_Niihau_LMSL_DEM_2013_m9002_EPSG-6322.vrt"),
    "ngs-nwhi21": dict(LIDAR, **EGM2008, id=1, year="2021", name="NOAA NGS lidar", nodata=-999999.0,
                       path="dem/NGS_NWHawaiianIslands_Topobathy_2021_10191/"
                            "NGS_NWHawaiianIslands_Topobathy_2021_m10191_EPSG-6322.vrt",
                       credit="NOAA NGS 2021 Northwestern Hawaiian Islands topobathy lidar (NOAA Digital Coast)"),
    "ngs-guam20": dict(LIDAR, id=1, year="2020", name="NOAA NGS lidar", nodata=-999999.0,
                       datum="GUVD04 (Guam local mean sea level)",
                       path="dem/NGS_Guam_topobathy_DEM_2020_9323/NGS_Guam_topobathy_DEM_2020_m9323_EPSG-8693.vrt",
                       credit="NOAA NGS 2020 Guam topobathy lidar (NOAA Digital Coast)"),
    # Puerto Rico: NGS 2019 (orthometric; minus CUDEM -0.03..-0.25 m) wins, USACE 2018 fills its holes and fades in
    # at its edge (the gap-size rule, owner 2026-11-14).
    "ngs-pr19": dict(LIDAR, id=1, year="2019", name="NOAA NGS lidar", nodata=-3.4028234663852886e38,
                     datum="PRVD02 (Puerto Rico local mean sea level)",
                     path="dem/NGS_PR_Topobathy_DEM_2019_9392/NGS_PR_Topobathy_DEM_2019_m9392_EPSG-26919.vrt",
                     credit="NOAA NGS 2019 Puerto Rico topobathy lidar (NOAA Digital Coast)"),
    "usace-pr18": dict(LIDAR, id=2, year="2018", name="USACE lidar", nodata=-3.4028234663852886e38,
                       datum="PRVD02 (Puerto Rico local mean sea level)",
                       path="dem/USACE_PR_Topobathy_DEM_2018_8571/USACE_PR_Topobathy_DEM_2018_m8571_EPSG-6318.vrt",
                       credit="USACE National Coastal Mapping Program 2018 Puerto Rico topobathy lidar "
                              "(NOAA Digital Coast)"),
    # Digital Earth Pacific (plan section 97). Measured against NOAA's CUDEM in Hawaii and Guam (2026-11-15): where it
    # says 0-12 m, 56-97 % of pixels lie within 2 m of the survey; on reef flats it reads ~2 m too deep, past ~12 m it
    # saturates (hence max_depth, applied after the warp's averaging). The name carries its year and the warning (the
    # client shows the name as the label).
    "dep-sdb": dict(kind="dep", id=12, zmax=14, res_m=10, year="2024", offset=0.0, max_depth=12.0, water_all=True,
                    skip_tiles=((20, 30), (21, 30), (22, 30), (24, 31)), nodata=float("nan"),
                    name="DEP satellite estimate 2024, often off by 2 m",
                    datum="about mean sea level (FES2022 tide-corrected; satellite estimate)",
                    licence="CC BY 4.0",
                    credit="DEP / SPC Digital Earth Pacific satellite-derived bathymetry (Sentinel-2, 10 m), CC BY 4.0"),
    # The Lower 48 (plan section 98): national sets, clipped to each region's box.
    "cudem-us": dict(CUDEM_US, id=10, nodata=-99999.0,
                     path="dem/NCEI_ninth_Topobathy_2014_8483/NCEI_ninth_Topobathy_2014_m8483_EPSG-4269.vrt"),
    "cudem-us-b": dict(CUDEM_US, id=11, nodata=-99999.0,
                       path="dem/NCEI_ninth_Topobathy_2014_8483/NCEI_ninth_Topobathy_2014_m8483_EPSG-4269_1.vrt"),
    "coned-socal16": dict(SURVEY15, id=12, year="2016", name="USGS CoNED lidar", nodata=-32767.0,
                          clip=(-121.0, 32.0, -116.0, 36.5),
                          path="dem/CA_Southern_CoNED_DEM_2016_8658/CA_Southern_CoNED_DEM_2016_m8658_EPSG-26911.vrt",
                          credit="USGS Coastal National Elevation Database (CoNED) 2016 Southern California "
                                 "topobathymetric model (NOAA Digital Coast)"),
    "coned-central17": dict(SURVEY15, id=13, year="2017", name="USGS CoNED lidar", nodata=-32767.0,
                            clip=(-123.0, 34.0, -120.0, 36.5),
                            path="dem/CA_Central_CoNED_DEM_2017_8657/CA_Central_CoNED_DEM_2017_m8657_EPSG-3717.vrt",
                            credit="USGS Coastal National Elevation Database (CoNED) 2017 Central California "
                                   "topobathymetric model (NOAA Digital Coast)"),
    "coned-norcal20": dict(SURVEY15, id=14, year="2020", name="USGS CoNED lidar", nodata=-3.4028230607370965e38,
                           clip=(-125.0, 38.75, -119.0, 42.1),
                           path="dem/CA_north_coned_DEM_2020_9181/CA_north_coned_DEM_2020_m9181_EPSG-3717.vrt",
                           credit="USGS Coastal National Elevation Database (CoNED) 2020 Northern California "
                                  "topobathymetric model (NOAA Digital Coast)"),
    "usace-or14": dict(SURVEY15, id=15, year="2014", name="USACE lidar", nodata=-3.4028234663852886e38,
                       clip=(-125.5, 41.9, -121.0, 43.5),
                       path="dem/USACE_OR_Topobathy_DEM_2014_6282/USACE_OR_Topobathy_DEM_2014_m6282_EPSG-6318.vrt",
                       credit="USACE National Coastal Mapping Program 2014 Oregon topobathy lidar (NOAA Digital Coast)"),
    "bluetopo4": dict(BLUETOPO, id=16, bt_res=(2, 4), zmax=15, res_m=4, deep=(100.0, 13), name="NOAA BlueTopo 4 m",
                      credit="NOAA Office of Coast Survey BlueTopo national bathymetric compilation"),
    "cudem3-us": dict(CUDEM3_US, id=20, nodata=-99999.0,
                      path="dem/NCEI_third_Topobathy_2014_8580/NCEI_third_Topobathy_2014_m8580_EPSG-4269.vrt"),
    "cudem3-us-b": dict(CUDEM3_US, id=21, nodata=-9999.0,
                        path="dem/NCEI_third_Topobathy_2014_8580/NCEI_third_Topobathy_2014_m8580_EPSG-4269_1.vrt"),
    "bluetopo8": dict(BLUETOPO, id=22, bt_res=(8,), zmax=14, res_m=8, deep=(100.0, 13), name="NOAA BlueTopo 8 m",
                      credit="NOAA Office of Coast Survey BlueTopo national bathymetric compilation"),
    "bluetopo16": dict(BLUETOPO, id=23, bt_res=(16,), zmax=13, res_m=16, name="NOAA BlueTopo 16 m",
                       credit="NOAA Office of Coast Survey BlueTopo national bathymetric compilation"),
    # The Great Lakes (plan section 98). The NCEI grids: one per lake, below the lake's Low Water Datum, land positive.
    "lakegrid-superior": dict(LAKEGRID, lake="superior", year="draft", credit="NOAA NCEI (NGDC) Bathymetry of Lake "
                              "Superior, draft compilation (no DOI)"),
    "lakegrid-michigan": dict(LAKEGRID, lake="michigan", year="1996", credit="NOAA NCEI (NGDC) 1996 Bathymetry of "
                              "Lake Michigan, doi:10.7289/V5B85627"),
    "lakegrid-huron": dict(LAKEGRID, lake="huron", year="1999", credit="NOAA NCEI (NGDC) 1999 Bathymetry of Lake "
                           "Huron, doi:10.7289/V5G15XS5"),
    "lakegrid-erie": dict(LAKEGRID, lake="erie", year="1999", credit="NOAA NCEI (NGDC) 1999 Bathymetry of Lake Erie "
                          "and Lake St. Clair, doi:10.7289/V5KS6PHK"),
    "lakegrid-stclair": dict(LAKEGRID, lake="erie", year="1999", offset=-0.9,
                             datum="Low Water Datum of Lake St Clair (IGLD 1985, 174.4 m): Lake Erie's grid less 0.9 m",
                             credit="NOAA NCEI (NGDC) 1999 Bathymetry of Lake Erie and Lake St. Clair, "
                                    "doi:10.7289/V5KS6PHK"),
    "lakegrid-ontario": dict(LAKEGRID, lake="ontario", year="1999", credit="NOAA NCEI (NGDC) 1999 Bathymetry of Lake "
                             "Ontario, doi:10.7289/V56H4FBH"),
    # The lakes' 1-m lidar DEMs, read to zoom 15 (newest survey first: the lowest id wins).
    "noaa-whitefish23": dict(lake_dem(183.2, navd88=True), id=1, year="2023", name="NOAA lidar",
                             nodata=-3.40282306073709653e38,
                             path="dem/NOAA_LakeSup_Whitefish_DEM_2023_10250/"
                                  "NOAA_LakeSup_Whitefish_DEM_2023_m10250_EPSG-26916.vrt",
                             credit="NOAA 2023 Lake Superior Whitefish Bay topobathy lidar (NOAA Digital Coast)"),
    "usace-stampsands22": dict(lake_dem(183.2), id=2, year="2022", name="USACE lidar",
                               path="dem/USACE_StampSands_MI_DEM_2022_10028/"
                                    "USACE_StampSands_MI_DEM_2022_m10028_EPSG-6345.vrt",
                               credit="USACE 2022 Keweenaw stamp sands topobathy lidar (NOAA Digital Coast)"),
    "usace-superior19a": dict(lake_dem(183.2), id=3, year="2019", name="USACE lidar",
                              path="dem/USACE_Superior_DEM_2019_9185/USACE_Superior_DEM_2019_m9185_EPSG-6344.vrt",
                              credit=USACE_NCMP % "2019 Lake Superior"),
    "usace-superior19b": dict(lake_dem(183.2), id=4, year="2019", name="USACE lidar",
                              path="dem/USACE_Superior_DEM_2019_9185/USACE_Superior_DEM_2019_m9185_EPSG-6345.vrt",
                              credit=USACE_NCMP % "2019 Lake Superior"),
    "usace-michigan20": dict(lake_dem(176.0), id=1, year="2020", name="USACE lidar",
                             path="dem/USACE_Lake_Mich_IL_IN_MI_WI_DEM_2020_9970/"
                                  "USACE_Lake_Mich_IL_IN_MI_WI_DEM_2020_m9970_EPSG-6345.vrt",
                             credit=USACE_NCMP % "2020 Lake Michigan"),
    "mi-islands16": dict(lake_dem(176.0, navd88=True), id=2, year="2016", name="Michigan lidar",
                         path="dem/MI_Upper_Lake_MI_Islands_Topobathy_DEM_2016_6195/"
                              "MI_Upper_Lake_MI_Islands_Topobathy_DEM_2016_m6195_EPSG-26916.vrt",
                         credit="Michigan 2016 upper Lake Michigan islands topobathy lidar (NOAA Digital Coast)"),
    "usace-huron19a": dict(lake_dem(176.0), id=1, year="2019", name="USACE lidar",
                           path="dem/USACE_Lake_Huron_MI_DEM_2019_9944/USACE_Lake_Huron_MI_DEM_2019_m9944_EPSG-6345.vrt",
                           credit=USACE_NCMP % "2019 Lake Huron (Michigan)"),
    "usace-huron19b": dict(lake_dem(176.0), id=2, year="2019", name="USACE lidar",
                           path="dem/USACE_Lake_Huron_MI_DEM_2019_9944/USACE_Lake_Huron_MI_DEM_2019_m9944_EPSG-6346.vrt",
                           credit=USACE_NCMP % "2019 Lake Huron (Michigan)"),
    "usace-huron18": dict(lake_dem(176.0), id=3, year="2018", name="USACE lidar",
                          path="dem/USACE_Huron_DEM_2018_9657/USACE_Huron_DEM_2018_m9657_EPSG-6346.vrt",
                          credit=USACE_NCMP % "2018 Lake Huron"),
    "usace-stclair20": dict(lake_dem(174.4), id=1, year="2020", name="USACE lidar",
                            path="dem/USACE_Lake_StClair_MI_DEM_2020_9972/"
                                 "USACE_Lake_StClair_MI_DEM_2020_m9972_EPSG-6346.vrt",
                            credit=USACE_NCMP % "2020 Lake St. Clair"),
    "usace-erie20": dict(lake_dem(173.5), id=1, year="2020", name="USACE lidar",
                         path="dem/USACE_Lake_Erie_MI_OH_DEM_2020_9971/USACE_Lake_Erie_MI_OH_DEM_2020_m9971_EPSG-6346.vrt",
                         credit=USACE_NCMP % "2020 Lake Erie (Michigan, Ohio)"),
    "usace-erie18": dict(lake_dem(173.5), id=2, year="2018", name="USACE lidar",
                         path="dem/USACE_Lake_Erie_NY_OH_PA_DEM_2018_9945/"
                              "USACE_Lake_Erie_NY_OH_PA_DEM_2018_m9945_EPSG-6346.vrt",
                         credit=USACE_NCMP % "2018 Lake Erie (New York, Ohio, Pennsylvania)"),
    "usace-ontario18a": dict(lake_dem(74.2), id=1, year="2018", name="USACE lidar",
                             path="dem/USACE_NY_Ontario_DEM_2018_8981/USACE_NY_Ontario_DEM_2018_m8981_EPSG-26918.vrt",
                             credit=USACE_NCMP % "2018 Lake Ontario (New York)"),
    "usace-ontario18b": dict(lake_dem(74.2), id=2, year="2018", name="USACE lidar",
                             path="dem/USACE_NY_Ontario_DEM_2018_8981/USACE_NY_Ontario_DEM_2018_m8981_EPSG-6346.vrt",
                             credit=USACE_NCMP % "2018 Lake Ontario (New York)"),
    "usace-ontario18c": dict(lake_dem(74.2), id=3, year="2018", name="USACE lidar",
                             path="dem/USACE_NY_Ontario_DEM_2018_8981/USACE_NY_Ontario_DEM_2018_m8981_EPSG-6347.vrt",
                             credit=USACE_NCMP % "2018 Lake Ontario (New York)"),
    # The Lower 48's 1-m lidar (plan section 98, owner 2026-11-15/17): read at zooms 16-17 over the US base regions in
    # the key surf areas, all of California and Florida's Space Coast. Ids count within a region (the newest survey
    # lowest, so it wins); a survey used by several regions keeps one id that fits each. `deep`: California and the key
    # areas are kept to 0-30 m; the Space Coast keeps every depth (owner: Florida all depths).
    "coned-norcal20-l": dict(LIDAR98, id=1, year="2020", name="USGS CoNED lidar", nodata=-3.4028230607370965e38,
                             path="dem/CA_north_coned_DEM_2020_9181/CA_north_coned_DEM_2020_m9181_EPSG-3717.vrt",
                             credit="USGS Coastal National Elevation Database (CoNED) 2020 Northern California "
                                    "topobathymetric model (NOAA Digital Coast)"),
    "coned-central17-l": dict(LIDAR98, id=2, year="2017", name="USGS CoNED lidar", nodata=-32767.0,
                              path="dem/CA_Central_CoNED_DEM_2017_8657/CA_Central_CoNED_DEM_2017_m8657_EPSG-3717.vrt",
                              credit="USGS Coastal National Elevation Database (CoNED) 2017 Central California "
                                     "topobathymetric model (NOAA Digital Coast)"),
    "coned-socal16-l": dict(LIDAR98, id=3, year="2016", name="USGS CoNED lidar", nodata=-32767.0,
                            path="dem/CA_Southern_CoNED_DEM_2016_8658/CA_Southern_CoNED_DEM_2016_m8658_EPSG-26911.vrt",
                            credit="USGS Coastal National Elevation Database (CoNED) 2016 Southern California "
                                   "topobathymetric model (NOAA Digital Coast)"),
    # NOAA's 2011 California topobathy merge, in two VRTs split by UTM zone (10 west of 120 W, 11 east of it)
    "noaa-ca11a": dict(LIDAR98, id=4, year="2011", name="NOAA lidar", nodata=-3.4028234663852886e38,
                       path="dem/California_Topobathy_DEM_2011_2616/California_Topobathy_DEM_2011_m2616_EPSG-26910.vrt",
                       credit=NOAA_CA11),
    "noaa-ca11b": dict(LIDAR98, id=5, year="2011", name="NOAA lidar", nodata=-3.4028234663852886e38,
                       path="dem/California_Topobathy_DEM_2011_2616/California_Topobathy_DEM_2011_m2616_EPSG-26911.vrt",
                       credit=NOAA_CA11),
    "usace-wa20": dict(LIDAR98, id=1, year="2020", name="USACE lidar", nodata=-3.4028234663852886e38,
                       path="dem/USACE_WA_DEM_2020_10116/USACE_WA_DEM_2020_m10116_EPSG-6339.vrt",
                       credit=USACE_NCMP % "2020 Washington"),
    # the same VRT as usace-or14 (the base map's zoom-15 fill south of 43.5 N), here at zooms 16-17
    "usace-or14-l": dict(LIDAR98, id=2, year="2014", name="USACE lidar", nodata=-3.4028234663852886e38,
                         path="dem/USACE_OR_Topobathy_DEM_2014_6282/USACE_OR_Topobathy_DEM_2014_m6282_EPSG-6318.vrt",
                         credit=USACE_NCMP % "2014 Oregon"),
    "usace-orwa10": dict(LIDAR98, id=3, year="2010", name="USACE lidar", nodata=-9999.0,
                         path="dem/USACE_OR_WA_DEM_2010_9443/USACE_OR_WA_DEM_2010_m9443_EPSG-4269.vrt",
                         credit=USACE_NCMP % "2010 Oregon and Washington"),
    "ngs-vasc20": dict(LIDAR98, id=1, year="2020", name="NOAA NGS lidar", nodata=-999999.0,
                       path="dem/NGS_VAtoSC_Topobathy_DEM_2020_9468/NGS_VAtoSC_Topobathy_DEM_2020_m9468_EPSG-6347.vrt",
                       credit="NOAA NGS 2020 Virginia to South Carolina topobathy lidar (NOAA Digital Coast)"),
    "usace-nc19": dict(LIDAR98, id=2, year="2019", name="USACE lidar", nodata=-3.4028234663852886e38,
                       path="dem/USACE_Topobathy_NC_DEM_2019_9001/USACE_Topobathy_NC_DEM_2019_m9001_EPSG-6318.vrt",
                       credit=USACE_NCMP % "2019 North Carolina"),
    # the 2018 East Coast set's two UTM VRTs hold nothing inside the key areas: only the geographic one is read
    "usace-east18": dict(LIDAR98, id=3, year="2018", name="USACE lidar", nodata=-3.4028234663852886e38,
                         path="dem/USACE_EastCoast_DEM_2018_8691/USACE_EastCoast_DEM_2018_m8691_EPSG-6318.vrt",
                         credit=USACE_NCMP % "2018 East Coast"),
    "usace-east17": dict(LIDAR98, id=4, year="2017", name="USACE lidar", nodata=-3.4028234663852886e38,
                         path="dem/USACE_East_Coast_DEM_2017_8517/USACE_East_Coast_DEM_2017_m8517_EPSG-6318.vrt",
                         credit=USACE_NCMP % "2017 East Coast"),
    # the zone-19 VRT of the 2022 New Jersey / New York set holds nothing inside the New Jersey box
    "usace-njny22": dict(LIDAR98, id=1, year="2022", name="USACE lidar", nodata=-3.4028234663852886e38,
                         path="dem/USACE_NJ_NY_DEM_2022_9851/USACE_NJ_NY_DEM_2022_m9851_EPSG-6347.vrt",
                         credit=USACE_NCMP % "2022 New Jersey and New York"),
    # the set's other VRT (no "m") lists the same 50 tiles inside the New Jersey box
    "coned-njde15": dict(LIDAR98, id=5, year="2015", name="USGS CoNED lidar", nodata=-3.4028234663852886e38,
                         path="dem/NewJersey_Delaware_Coned_Topobathy_DEM_2015_5040/"
                              "NewJersey_Delaware_Coned_Topobathy_DEM_2015_m5040_EPSG-26918.vrt",
                         credit="USGS Coastal National Elevation Database (CoNED) 2015 New Jersey and Delaware "
                                "topobathymetric model (NOAA Digital Coast)"),
    # the set's 1-tile m6194_EPSG-26918 VRT is left out
    "coned-ne16": dict(LIDAR98, id=6, year="2016", name="USGS CoNED lidar", nodata=-3.4028230607370965e38,
                       path="dem/New_England_Coned_Topobathy_DEM_2016_6194/"
                            "New_England_Coned_Topobathy_DEM_2016_m6194_EPSG-26918_1.vrt",
                       credit="USGS Coastal National Elevation Database (CoNED) 2016 New England topobathymetric "
                              "model (NOAA Digital Coast)"),
    "ngs-lis23": dict(LIDAR98, id=1, year="2023", name="NOAA NGS lidar", nodata=-999999.0,
                      path="dem/NGS_LongIslandSound_NY_Topobathy_2023_10274/"
                           "NGS_LongIslandSound_NY_Topobathy_2023_m10274_EPSG-6347.vrt",
                      credit="NOAA NGS 2023 Long Island Sound topobathy lidar (NOAA Digital Coast)"),
    "ngs-maine22": dict(LIDAR98, id=1, year="2022", name="NOAA NGS lidar", nodata=-999999.0,
                        path="dem/NGS_CoastalMaine_Topobathy_2022_10422/"
                             "NGS_CoastalMaine_Topobathy_2022_m10422_EPSG-6348.vrt",
                        credit="NOAA NGS 2022 coastal Maine topobathy lidar (NOAA Digital Coast)"),
    # Florida's Space Coast, every depth. The post-Nicole 2022 set (10011) holds nothing inside the box: left out.
    "usace-milton24": dict(LIDAR98_ALL, id=1, year="2024", name="USACE lidar", nodata=-3.4028234663852886e38,
                           path="dem/USACE_Post_Milton_DEM_2024_10208/USACE_Post_Milton_DEM_2024_m10208_EPSG-6346.vrt",
                           credit=USACE_NCMP % "2024 post-Milton Florida"),
    "usace-irma17": dict(LIDAR98_ALL, id=2, year="2017", name="USACE lidar", nodata=-3.4028234663852886e38,
                         path="dem/USACE_FL_Post_Irma_Topobathy_DEM_2017_8388/"
                              "USACE_FL_Post_Irma_Topobathy_DEM_2017_m8388_EPSG-6318.vrt",
                         credit=USACE_NCMP % "2017 post-Irma Florida"),
    "gebco": dict(kind="gebco", id=31, zmax=7, res_m=460, year="2026", offset=0.0,
                  name="GEBCO_2026 Grid", datum="mean sea level (approximate)",
                  licence="GEBCO licence: free to use with credit",
                  credit="GEBCO Compilation Group (2026) GEBCO 2026 Grid"),
}

REGIONS = {
    # Boxes reach as far as the 1/3 arc-second sets do (Hawaii -160.5..-154.5 x 18.5..22.5; Puerto Rico to 19 N, the
    # Virgin Islands to 17.25 N), so the regional grids end where their own data ends, not on a straight box line.
    "hawaii": dict(box=(-160.5, 18.5, -154.5, 22.5), sources=["cudem-hawaii", "cudem3-hawaii"], mhhw=0.4, zmin=8,
                   prio=10),
    "marianas": dict(box=(144.5, 13.0, 146.0, 15.5), sources=["cudem-guam", "cudem-cnmi", "cudem3-guam", "cudem3-cnmi"],
                     mhhw=0.4, zmin=8, prio=10),
    "samoa": dict(box=(-171.0, -14.5, -169.25, -14.0), sources=["cudem-samoa", "cudem3-samoa"], mhhw=0.45, zmin=8,
                  prio=10),
    "prvi": dict(box=(-68.0, 17.25, -64.25, 19.0), sources=["cudem-pr", "cudem-usvi", "cudem3-pr", "cudem3-usvi"],
                 mhhw=0.25, zmin=8, prio=10),
    "gebco": dict(box=(-180.0, -85.0511, 180.0, 85.0511), sources=["gebco"], mhhw=0.0, zmin=0, prio=0),
    # The 1-m lidar regions (plan section 96), drawn over the CUDEM regions (prio 20). zmin 16: zoom 15 and coarser stay
    # the CUDEM regions' tiles. `ring`: blocks read around the lidar's own (fill only, never written) so the water rule
    # reaches deep water beyond the lidar. `margin`: pixels read around a block at its finest zoom (the feather needs
    # 3 x FEATHER_PX, so the blend (which looks 3 x FEATHER_PX around a pixel) agrees across block edges).
    "hawaii-lidar": dict(box=(-160.5, 18.5, -154.5, 22.5), mhhw=0.4, zmin=16, prio=20, ring=1, margin=72,
                         sources=["ngs-niihau21", "usace-oahu13", "usace-kauai13", "usace-maui13", "usace-molokai13",
                                  "usace-lanai13", "usace-bigisland13", "usace-niihau13"],
                         fill=["cudem-hawaii", "cudem3-hawaii"]),
    # Guam: the box reaches 144.4 E (the lidar's offshore banks lie to 144.43 E, west of the CUDEM region's 144.5).
    "guam-lidar": dict(box=(144.4, 13.0, 145.1, 13.8), mhhw=0.4, zmin=16, prio=20, ring=1, margin=72,
                       sources=["ngs-guam20"], fill=["cudem-guam", "cudem3-guam"]),
    "pr-lidar": dict(box=(-68.0, 17.25, -65.0, 19.0), mhhw=0.25, zmin=16, prio=20, ring=1, margin=72,
                     sources=["ngs-pr19", "usace-pr18"],
                     # the lidar reaches past 65.25 W (Culebra, Vieques), where only the Virgin Islands grids hold data
                     fill=["cudem-pr", "cudem-usvi", "cudem3-pr", "cudem3-usvi"]),
    # The NW Hawaiian Islands (French Frigate Shoals, Necker, Nihoa): nothing finer below them, so zooms 8-17 from the
    # lidar itself (GEBCO under it); the lidar reaches 30-50 m, so the usual deep-water rule finds the open sea.
    "nwhi-lidar": dict(box=(-168.5, 22.5, -160.5, 24.5), mhhw=0.4, zmin=8, prio=20, margin=32,
                       sources=["ngs-nwhi21"]),
    # Digital Earth Pacific's satellite depths (plan section 97): one region across the date line (wrap: tile aprons
    # and coarser zooms join x = 0 and x = 2^z - 1), under the US regions (prio 5), never inside their boxes.
    "pacific-sdb": dict(box=(-180.0, -28.5, 180.0, 24.5), sources=["dep-sdb"], mhhw=0.0, zmin=8, prio=5, wrap=True,
                        exclude=["hawaii", "marianas", "samoa", "prvi", "hawaii-lidar", "guam-lidar", "pr-lidar",
                                 "nwhi-lidar"]),
}

# The Lower 48's ocean coasts (plan section 98): twelve boxes, each built on its own (cut: nothing drawn outside the box,
# so the neighbour draws there), MHHW from the committed grid. (west, south, east, north). Each reads REACH_DEG past its
# box (reach) for the water rule only: a river or bay whose mouth lies in the neighbour's box still joins the sea
# (the St Johns at Jacksonville, the Raritan); what lies outside the box is cut after the water rule.
REACH_DEG = 0.3
US_EAST = ["cudem-us", "cudem-us-b", "bluetopo4", "cudem3-us", "cudem3-us-b", "bluetopo8", "bluetopo16"]
US_BOXES = {
    "us-me-nh": ((-71.2, 42.8, -66.8, 47.5), US_EAST),
    # to 74.5 W: the Raritan and Arthur Kill; to 42.8 N: the Hudson's tidal reach to Troy (42.74 N)
    "us-sne-ny": ((-74.5, 40.4, -69.8, 42.8), US_EAST),
    "us-nj-ches": ((-77.7, 36.9, -73.5, 40.4), US_EAST),
    "us-va-nc": ((-79.5, 33.7, -75.0, 36.9), US_EAST),
    "us-sc-ga": ((-82.0, 30.7, -77.5, 33.7), US_EAST),
    "us-fl-east": ((-81.6, 26.0, -79.7, 30.7), US_EAST),
    "us-fl-south": ((-83.2, 24.3, -79.7, 26.0), US_EAST),
    "us-gulf-east": ((-88.45, 26.0, -81.6, 31.2), US_EAST),
    "us-gulf-west": ((-97.7, 25.7, -88.45, 30.6), US_EAST),
    "us-ca-south": ((-121.0, 32.4, -116.9, 34.6),
                    ["cudem-us", "cudem-us-b", "coned-socal16", "coned-central17", "bluetopo4", "bluetopo8",
                     "bluetopo16"]),
    "us-ca-north": ((-124.6, 34.6, -119.5, 42.0),
                    ["cudem-us", "cudem-us-b", "coned-central17", "coned-norcal20", "bluetopo4", "bluetopo8",
                     "bluetopo16"]),
    "us-or-wa": ((-125.2, 42.0, -121.8, 49.1), ["cudem-us", "cudem-us-b", "usace-or14", "bluetopo4", "bluetopo8",
                                                 "bluetopo16"]),
}
for _name, (_box, _srcs) in US_BOXES.items():
    REGIONS[_name] = dict(box=_box, sources=list(_srcs), mhhw="grid", zmin=8, prio=10, cut=True, seed="sea",
                          reach=REACH_DEG)
# The eastern Gulf's box reaches north to 31.2 N (the St Marys and the Okefenokee's rivers meet the sea at Fernandina);
# its corner east of 82.0 W and north of 30.7 N is the Georgia box's: left out, but only after the water rule
# (exclude_late), which needs the corner's water to join the St Marys to the sea.
REGIONS["us-gulf-east"].update(exclude=["us-sc-ga"], exclude_late=True)

# The Great Lakes (plan section 98): per lake (box, the lake's mean level above its datum, its grid, its lidar DEMs,
# the lake regions its box must leave out).
LAKE_BOXES = {
    "lake-superior": ((-92.4, 46.38, -84.36, 49.1), 0.19, "lakegrid-superior",
                      ["noaa-whitefish23", "usace-stampsands22", "usace-superior19a", "usace-superior19b"], []),
    "lake-michigan": ((-88.1, 41.6, -84.5, 46.2), 0.39, "lakegrid-michigan", ["usace-michigan20", "mi-islands16"], []),
    # to 46.6 N: the lower St Marys River (at Lake Huron's level below the Soo locks); its corner west of 84.36 W is
    # Superior's (left out: exclude). Superior's box never reaches east of 84.36 W: its grid there reads Huron-level
    # water and low land below Superior's datum as water.
    "lake-huron": ((-84.5, 43.0, -79.6, 46.6), 0.33, "lakegrid-huron",
                   ["usace-huron19a", "usace-huron19b", "usace-huron18"], ["lake-ontario", "lake-superior"]),
    "lake-stclair": ((-83.05, 42.3, -82.35, 43.0), 0.69, "lakegrid-stclair", ["usace-stclair20"], []),
    # to 43.1 N: the Niagara River's lidar past Grand Island (Erie's grid ends at 43.0 N); the strip west of 79.6 W
    # is Huron's (exclude)
    "lake-erie": ((-83.6, 41.3, -78.85, 43.1), 0.77, "lakegrid-erie", ["usace-erie20", "usace-erie18"],
                  ["lake-stclair", "lake-huron"]),
    "lake-ontario": ((-79.95, 43.1, -76.0, 44.3), 0.49, "lakegrid-ontario",
                     ["usace-ontario18a", "usace-ontario18b", "usace-ontario18c"], []),
}
for _name, (_box, _level, _grid, _dems, _ex) in LAKE_BOXES.items():
    REGIONS[_name] = dict(box=_box, sources=[_grid], mhhw=_level, zmin=5, prio=10, cut=True, seed="lake",
                          exclude=list(_ex))
    REGIONS[_name + "-dem"] = dict(box=_box, sources=list(_dems), fill=[_grid], mhhw=_level, zmin=12, prio=12, ring=1,
                                   margin=72, cut=True, seed="lake", exclude=[e + "-dem" for e in _ex] + list(_ex))

# The Lower 48's 1-m lidar regions (plan section 98, step 6): over the US base regions (prio 20 against their 10), each
# cut to its box, reading only where its base region has published water (`presence`). `fill` = the base region's
# sources, less any that read the same file as one of the region's surveys or lie outside its box.
WEST_FILL = ["cudem-us", "cudem-us-b", "bluetopo4", "bluetopo8", "bluetopo16"]
LIDAR98_BOXES = {
    # California: five boxes tile us-ca-south (split at 119.8 W and 119.0 W) and the coast of us-ca-north. The
    # Point Conception to Ventura box was split at 119.8 W (half its blocks each side): as one it took 2 h 10 min on the
    # runner (section 98 dry run, 2026-11-17).
    "ca-s1": ((-119.0, 32.4, -116.9, 34.6), ["coned-socal16-l", "noaa-ca11b"], WEST_FILL, "us-ca-south"),
    "ca-s2w": ((-121.0, 32.4, -119.8, 34.6), ["coned-central17-l", "coned-socal16-l", "noaa-ca11a", "noaa-ca11b"],
               WEST_FILL, "us-ca-south"),
    # (the 2017 Central California model and the 2011 merge's zone-10 VRT hold nothing east of 119.8 W)
    "ca-s2e": ((-119.8, 32.4, -119.0, 34.6), ["coned-socal16-l", "noaa-ca11b"], WEST_FILL, "us-ca-south"),
    "ca-c": ((-123.2, 34.6, -120.0, 38.3), ["coned-norcal20-l", "coned-central17-l", "noaa-ca11a"], WEST_FILL,
             "us-ca-north"),
    "ca-n": ((-124.6, 38.3, -122.0, 42.0), ["coned-norcal20-l", "noaa-ca11a"], WEST_FILL, "us-ca-north"),
    # the key surf areas
    "key-or": ((-124.10, 44.55, -123.90, 46.05), ["usace-or14-l", "usace-orwa10"], WEST_FILL, "us-or-wa"),
    "key-wa": ((-124.20, 46.80, -124.05, 46.95), ["usace-wa20", "usace-orwa10"], WEST_FILL, "us-or-wa"),
    "key-nc": ((-75.80, 35.20, -75.40, 36.20), ["ngs-vasc20", "usace-nc19", "usace-east18", "usace-east17"], US_EAST,
               "us-va-nc"),
    "key-nj": ((-74.35, 39.45, -73.95, 40.15), ["usace-njny22", "usace-east17", "coned-njde15"], US_EAST, "us-nj-ches"),
    "key-ny-rock": ((-73.95, 40.55, -73.55, 40.62), ["usace-east17", "coned-ne16"], US_EAST, "us-sne-ny"),
    "key-ny-mtk": ((-72.05, 40.98, -71.85, 41.10), ["ngs-lis23", "coned-ne16"], US_EAST, "us-sne-ny"),
    "key-ri": ((-71.70, 41.35, -71.30, 41.50), ["usace-east18", "coned-ne16"], US_EAST, "us-sne-ny"),
    "key-nh": ((-70.85, 42.85, -70.70, 43.05), ["ngs-maine22"], US_EAST, "us-me-nh"),
    # Cape Canaveral, Cocoa Beach, Sebastian Inlet (inside us-fl-east), every depth
    "key-fl-space": ((-80.70, 27.80, -80.30, 28.65), ["usace-milton24", "usace-irma17"], US_EAST, "us-fl-east"),
}
for _name, (_box, _srcs, _fill, _base) in LIDAR98_BOXES.items():
    REGIONS[_name] = dict(box=_box, mhhw="grid", zmin=16, prio=20, ring=1, margin=72, cut=True, seed="sea",
                          sources=list(_srcs), fill=list(_fill), presence=[_base])

# International coasts (plan section 111a: Australia and New Zealand). Each region is built on its own, cut to its box
# (nothing finer lies beneath them: GEBCO only), its open water seeded from the GSHHG land file (seed "sea": most surveys
# stop 10-49 m deep, but several NZ sheets only 1-3 m, never reaching the 20-m deep-water seed).
#
# New Zealand: LINZ's national "New Zealand Coastal LiDAR 1m DEM" (kind "stac": the collection's 65 1-m COG sheets on the
# open bucket nz-coastal, NZGD2000 / NZTM EPSG:2193, NZVD2016 heights, nodata -9999), read by window over https. The
# collection is LINZ's "ongoing" one: the build checks it holds exactly the pinned item list (count + digest of each
# item's link checksum), so a new or changed sheet stops the build until the pin is renewed. Heights become heights above
# mean sea level by subtracting the committed grid of MSL in NZVD2016 (from 10 LINZ standard ports, -0.03 .. -0.21 m),
# and the water's edge is mean higher high water from the committed EOT20 tide grid corrected at 12 tide gauges
# (tools/bathy/build_tide_grids.py; 0.28-1.49 m over the sheets, lowest by Cook Strait). Deeper than 30 m a pixel earns
# zoom 15 only.
#
# New South Wales: the 2018 Marine LiDAR topo-bathy DEM, 5 m (kind "tilezip": ten ELVIS order archives mirrored to the
# bucket by tools/bathy/bathy_mirror.py, mirror_nsw.json; each holds one GeoTIFF in GDA2020 / MGA zone 56, EPSG:7856,
# Australian Height Datum). AHD is mean sea level within 0.05 m at most NSW ocean gauges (MHL 2012, 1990-2010: -0.10 ..
# +0.05 m, median +0.02), so offset 0; mean higher high water 0.68 m above it at Sydney and Port Kembla (UHSLC
# 2003-2021; EOT20 0.66-0.67), rounded up to 0.7.
LINZ_COLLECTION = "https://nz-coastal.s3.ap-southeast-2.amazonaws.com/new-zealand/new-zealand/dem_1m/2193/collection.json"
NZ_MSL = dict(geoid="nz_msl_nzvd2016", offset=0.0,
              datum="NZVD2016 to mean sea level: grid from 10 LINZ standard ports (-0.03 to -0.21 m)")
INTL_REACH = 0.3
SOURCES.update({
    "linz-nz-coastal": dict(NZ_MSL, kind="stac", id=1, zmax=17, res_m=1, deep=(30.0, 15), clip=True, nodata=-9999.0,
                            survey_fade=True,
                            year="2024-2026", name="LINZ coastal lidar", short="LINZ", licence="CC BY 4.0",
                            stac=dict(collection=LINZ_COLLECTION, id="01JXTPWYDWTP9TRXV5CTW6MTCX", items=65,
                                      digest="03e9b4447fd02fda404aa25983bee34ddc36b10fce0cfe640ba215af95f3b9bf"),
                            credit="LINZ (Toitu Te Whenua Land Information New Zealand) New Zealand Coastal LiDAR 1m "
                                   "DEM 2024-2026, CC BY 4.0"),
    "nsw-marine18": dict(kind="tilezip", id=1, zmax=15, res_m=5, offset=0.0, clip=True,
                         nodata=-3.402820018375656e38, year="2018", name="NSW marine lidar", short="NSW", licence="CC BY 4.0",
                         mirror="mirror_nsw.json", crs="EPSG:7856",
                         datum="AHD (Australian Height Datum), mean sea level within 0.05 m at most NSW ocean gauges",
                         credit="NSW Department of Planning, Industry and Environment, NSW Marine LiDAR Topo-Bathy 2018 "
                                "(5 m), CC BY 4.0"),
})
INTL_BOXES = {
    # New South Wales, split at 32.85 S (the order strips' own seam) so each half stays well inside a runner's 5 hours
    "au-nsw-s": ((149.5, -37.7, 152.6, -32.85), ["nsw-marine18"], 0.7),
    "au-nsw-n": ((151.2, -32.85, 153.8, -28.0), ["nsw-marine18"], 0.7),
    # New Zealand's 65 sheets in three groups: the North Island to Taranaki and Gisborne, Kapiti / Wellington / Nelson,
    # and the south (Haast, Otago, Southland, Stewart Island)
    "nz-north": ((172.9, -39.5, 178.9, -34.5), ["linz-nz-coastal"], "grid:nz"),
    "nz-central": ((172.4, -41.7, 175.4, -39.5), ["linz-nz-coastal"], "grid:nz"),
    "nz-south": ((167.3, -47.1, 171.2, -43.5), ["linz-nz-coastal"], "grid:nz"),
}
for _name, (_box, _srcs, _mhhw) in INTL_BOXES.items():
    REGIONS[_name] = dict(box=_box, sources=list(_srcs), mhhw=_mhhw, zmin=8, prio=20, cut=True, seed="sea",
                          reach=INTL_REACH)

FEATHER_PX = 24        # at a region's finest zoom: the widest band inside the lidar's edge that blends to the fill
FEATHER_MIN_PX = 2     # the narrowest (round a gap of a pixel or two)

# Datum grids of the Lower 48 (plan section 98), committed beside this file and pinned by hash (DATUM_GRIDS):
# us_msl_navd88.tif = mean sea level's height above NAVD88, us_mhhw_msl.tif = mean higher high water's height above mean
# sea level, both from NOAA CO-OPS tide-station datums (tools/bathy/build_datum_grids.py).
DATUM_DIR = os.path.dirname(os.path.abspath(__file__))
DATUM_GRIDS = {      # file name -> sha256 (built 2026-11-16 from tools/bathy/us_datum_stations.json; box widened)
    "us_msl_navd88.tif": "ba1cb69e124cc7ae7755ef1a05a79704e6b8b5ee694825082c35e86fc838dff5",
    "us_mhhw_msl.tif": "cc574bbe57b92d89464a20dbd59b64e317cae7d060a748ce0a65a135d86e89fc",
    # New Zealand (plan section 111a, built 2026-11-26 by tools/bathy/build_tide_grids.py from nz_datum_stations.json
    # and EOT20): MSL in NZVD2016, MHHW above MSL
    "nz_msl_nzvd2016.tif": "e14f883213f1aae4bdffdd16685ded8a7c221a8019591c53329c2bd025b347ac",
    "nz_mhhw_msl.tif": "2d345eb9ca36f8b2a2f9fbf033010449a52615b2a08172899dcc331643eaac9e",
}
MHHW_GRID = os.path.join(DATUM_DIR, "us_mhhw_msl.tif")
# A region's mhhw "grid" (the Lower 48) or "grid:<name>" -> the committed grid of MHHW above mean sea level it reads.
# nz_mhhw_msl.tif: EOT20 (DGFI-TUM, CC BY 4.0) predicted 19 years at each 1/8-degree cell, corrected at 12 UHSLC tide
# gauges (tools/bathy/build_tide_grids.py, plan section 111a).
MHHW_GRIDS = {"grid": MHHW_GRID, "grid:nz": os.path.join(DATUM_DIR, "nz_mhhw_msl.tif")}

# Grids a source subtracts from its elevations (a source's `geoid`), GDAL paths, metres. "egm2008": the geoid height N
# above the ellipsoid (ellipsoid height - N = height above the geoid). "msl_navd88": mean sea level above NAVD88 (NAVD88
# height - it = height above mean sea level). "nz_msl_nzvd2016": mean sea level in NZVD2016 (plan section 111a).
GEOIDS = {"egm2008": "/vsicurl/https://cdn.proj.org/us_nga_egm08_25.tif",
          "msl_navd88": os.path.join(DATUM_DIR, "us_msl_navd88.tif"),
          "nz_msl_nzvd2016": os.path.join(DATUM_DIR, "nz_msl_nzvd2016.tif")}
GEOID_SANE_M = 200.0  # |geoid height| beyond this is a fill value of the geoid grid (real N: about -107 to +86 m)

GEBCO_URL = "https://dap.ceda.ac.uk/bodc/gebco/global/gebco_2026/ice_surface_elevation/geotiff/gebco_2026_geotiff.zip"

# GEBCO's quadrant GeoTIFFs: (west, south) corners; the build refuses a zip missing any of them (a 90 x 90 degree hole).
GEBCO_QUADRANTS = {(w, s) for w in (-180, -90, 0, 90) for s in (-90, 0)}

# GEBCO's elevations are relative to sea level everywhere, so land and lake beds below sea level (Qattara, the Dead Sea,
# the Caspian, Baikal's bed) would read as sea depth (G61a P2-2). Its water is therefore also outside GSHHG's land
# (level 1, lakes inside it count as land), from the site's own published coast-v1 overview (~1 km), pinned by hash.
LAND = dict(url="https://models.allshoresurf.com/static/coast/v1/world-i.bin",
            sha256="aabbff516107953c42915a11c70941682030ee3aecc29b0d0456d33b3ef9cebc",
            name="GSHHG 2.3.7 shorelines (land mask for GEBCO)",
            credit="Wessel, P., and W. H. F. Smith (1996), GSHHG", licence="LGPL 3")

DEEP_M = 20.0          # water deeper than this seeds "the open sea" for the connectivity rule
# A region with `seed` "sea" (or "lake") finds its open water from a shoreline file instead of water deeper than DEEP_M
# alone: low pixels at least SEED_M from the shore on the file's water side, or deeper than DEEP_M and on its water side.
# The Lower 48's coasts need it: lidar that stops 10-19 m deep (south Oregon) never reaches 20 m, and a deep inland
# quarry is no sea. The shoreline files are GSHHG intermediate (about 1 km off at worst), so SEED_M stays above that.
SEED_M = 1000.0
LAKES = dict(url="https://models.allshoresurf.com/static/coast/lakes-v1/lakes-i.bin",
             sha256="00fa10638529042205de0373a677d474a4791c334f01976795f8972fbd71b8ea",
             name="GSHHG 2.3.7 Great Lakes shorelines (open-water seed)",
             credit="Wessel, P., and W. H. F. Smith (1996), GSHHG", licence="LGPL 3")
SEED_FILES = {"sea": ("land-world-i.bin", LAND), "lake": ("lakes-i.bin", LAKES)}
SANE_M = 12000.0       # |elevation| beyond this is a fill value


def gdal_path(src):
    """The GDAL path of a source: its own `local` path when set (tests), else /vsis3/ on NOAA's bucket; None for GEBCO
    (its files come from the downloaded archive, build_bathy --gebco) and for DEP (build_bathy writes its VRT)."""
    if src.get("local"):
        return src["local"]
    if not src.get("path"):
        return None
    return "/vsis3/%s/%s" % (BUCKET, src["path"])
