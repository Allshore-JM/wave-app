"""Bathymetry sources and regions (plan section 93b). Data only.

A SOURCE is one gridded elevation set read through GDAL (a VRT over NOAA's open S3 bucket, or a local file in
tests); a REGION is a box of the map built from its sources, best first, as one versioned tile set
(static/bathy/<region>/vN/). The CUDEM regions' boxes do not overlap; GEBCO lies under all of them (zooms 0-7). The
1-m lidar regions (plan section 96: "-lidar", prio 20, zooms 16-17; the NW Hawaiian Islands 8-17) lie on top of them:
the client stacks regions by prio, and a pixel the higher region has no picture for falls through to the next.

A region's `fill` sources are read and merged under its own (the lidar's holes and the water just beyond it take their
values) but never decide which blocks are read and never earn a zoom beyond their own `zmax`; inside the lidar's edge the
depth blends from the lidar to the fill over FEATHER_PX pixels (flagged "blended").

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
"""

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
LIDAR = dict(kind="vrt", zmax=17, res_m=1, licence="public domain (US government)", offset=0.0)
USACE13 = dict(LIDAR, year="2013", name="USACE lidar", datum="LMSL (Hawaii local mean sea level)",
               nodata=-3.4028234663852886e38,
               credit="USACE National Coastal Mapping Program 2013 Hawaii topobathy lidar (NOAA Digital Coast)")
EGM2008 = dict(geoid="egm2008", offset=-0.3,
               datum="NAD83(PA11) ellipsoid heights to mean sea level: EGM2008 geoid, -0.3 m (approximate)")

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
    # Hawaii lidar: the newest survey wins (owner decision): NGS Niihau 2021 first, USACE 2013 fills its holes.
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
    # Puerto Rico: NGS 2019 (orthometric; minus CUDEM -0.03..-0.25 m) wins, USACE 2018 fills its holes.
    "ngs-pr19": dict(LIDAR, id=1, year="2019", name="NOAA NGS lidar", nodata=-3.4028234663852886e38,
                     datum="PRVD02 (Puerto Rico local mean sea level)",
                     path="dem/NGS_PR_Topobathy_DEM_2019_9392/NGS_PR_Topobathy_DEM_2019_m9392_EPSG-26919.vrt",
                     credit="NOAA NGS 2019 Puerto Rico topobathy lidar (NOAA Digital Coast)"),
    "usace-pr18": dict(LIDAR, id=2, year="2018", name="USACE lidar", nodata=-3.4028234663852886e38,
                       datum="PRVD02 (Puerto Rico local mean sea level)",
                       path="dem/USACE_PR_Topobathy_DEM_2018_8571/USACE_PR_Topobathy_DEM_2018_m8571_EPSG-6318.vrt",
                       credit="USACE National Coastal Mapping Program 2018 Puerto Rico topobathy lidar "
                              "(NOAA Digital Coast)"),
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
    # FEATHER_PX beyond the coarser zooms' apron).
    "hawaii-lidar": dict(box=(-160.5, 18.5, -154.5, 22.5), mhhw=0.4, zmin=16, prio=20, ring=1, margin=32,
                         sources=["ngs-niihau21", "usace-oahu13", "usace-kauai13", "usace-maui13", "usace-molokai13",
                                  "usace-lanai13", "usace-bigisland13", "usace-niihau13"],
                         fill=["cudem-hawaii", "cudem3-hawaii"]),
    "guam-lidar": dict(box=(144.5, 13.0, 145.1, 13.8), mhhw=0.4, zmin=16, prio=20, ring=1, margin=32,
                       sources=["ngs-guam20"], fill=["cudem-guam", "cudem3-guam"]),
    "pr-lidar": dict(box=(-68.0, 17.25, -65.0, 19.0), mhhw=0.25, zmin=16, prio=20, ring=1, margin=32,
                     sources=["ngs-pr19", "usace-pr18"], fill=["cudem-pr", "cudem3-pr"]),
    # The NW Hawaiian Islands (French Frigate Shoals, Necker, Nihoa): nothing finer below them, so zooms 8-17 from the
    # lidar itself (GEBCO under it); the lidar reaches 30-50 m, so the usual deep-water rule finds the open sea.
    "nwhi-lidar": dict(box=(-168.5, 22.5, -160.5, 24.5), mhhw=0.4, zmin=8, prio=20, margin=32,
                       sources=["ngs-nwhi21"]),
}

FEATHER_PX = 24        # at a region's finest zoom: the band inside the lidar's edge that blends towards the fill

# Geoid grids (a source's `geoid`): GDAL paths of the geoid height N above the ellipsoid, in metres.
GEOIDS = {"egm2008": "/vsicurl/https://cdn.proj.org/us_nga_egm08_25.tif"}

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
SANE_M = 12000.0       # |elevation| beyond this is a fill value


def gdal_path(src):
    """The GDAL path of a source: its own `local` path when set (tests), else /vsis3/ on NOAA's bucket; None for GEBCO
    (its files come from the downloaded archive, build_bathy --gebco)."""
    if src.get("local"):
        return src["local"]
    if not src.get("path"):
        return None
    return "/vsis3/%s/%s" % (BUCKET, src["path"])
