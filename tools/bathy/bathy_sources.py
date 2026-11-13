"""Bathymetry sources and regions (plan section 93b). Data only.

A SOURCE is one gridded elevation set read through GDAL (a VRT over NOAA's open S3 bucket, or a local file in
tests); a REGION is a box of the map built from its sources, best first, as one versioned tile set
(static/bathy/<region>/vN/). Region boxes do not overlap; GEBCO lies under all of them (zooms 0-7).

Elevations are metres above the source's vertical datum, positive up. Every stage-1 source sits on a local
mean-sea-level datum (checked 2026-11-11: Hawaii LMSL, Guam GUVD04, CNMI NMVD03, American Samoa ASVD02,
Puerto Rico PRVD02, USVI VIVD09; the Hawaii, Guam and Puerto Rico 1-m lidar agreed with CUDEM to 0.03-0.34 m),
so `offset` (added to bring a source to ~MSL) is 0 for all of them.

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
    "cudem3-hawaii": dict(CUDEM3, id=20, year="2024",
                          path="dem/NCEI_third_Topobathy_Hawaii_9429/NCEI_third_Topobathy_Hawaii_EPSG-4326.vrt",
                          datum="LMSL (Hawaii local mean sea level)"),
    "cudem3-guam": dict(CUDEM3, id=20, year="2026",
                        path="dem/NCEI_third_Topobathy_Guam_9463/NCEI_third_Topobathy_Guam_m9463_EPSG-6325.vrt",
                        datum="GUVD04 (Guam local mean sea level)"),
    "cudem3-cnmi": dict(CUDEM3, id=21, year="2026",
                        path="dem/NCEI_third_Topobathy_CNMI_9561/NCEI_third_Topobathy_CNMI_m9561_EPSG-6325.vrt",
                        datum="NMVD03 (Northern Marianas local mean sea level)"),
    "cudem3-samoa": dict(CUDEM3, id=20, year="2024",
                         path="dem/NCEI_third_Topobathy_AmSam_9461/NCEI_third_Topobathy_AmSam_EPSG-4269.vrt",
                         datum="ASVD02 (American Samoa local mean sea level)"),
    "cudem3-pr": dict(CUDEM3, id=20, year="2024",
                      path="dem/NCEI_third_Topobathy_PuertoRico_9524/NCEI_third_Topobathy_PuertoRico_EPSG-4269.vrt",
                      datum="PRVD02 (Puerto Rico local mean sea level)"),
    "cudem3-usvi": dict(CUDEM3, id=21, year="2024",
                        path="dem/NCEI_third_Topobathy_USVI_9528/NCEI_third_Topobathy_USVI_EPSG-4269.vrt",
                        datum="VIVD09 (Virgin Islands local mean sea level)"),
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
}

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
