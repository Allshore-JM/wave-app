Crops of the published GSHHG coast-v1 files (tools/coast; GSHHG 2.3.7, Wessel & Smith 1996, LGPL-3.0-or-later,
see tools/coast/LICENSE-GSHHG.txt) used by tests/ui/tools.test.js:

- `oahu-t1.bin`: tier 1 (full resolution, cell 5) pieces around Oahu, from `static/coast/v1/f/20_-160.bin`.
- `hawaii-t0.bin`: tier 0 (intermediate, cell 30) pieces of the Hawaiian islands, from `static/coast/v1/world-i.bin`.
- `maui-t1.bin`: tier 1 pieces around Honolua Bay (the whole Maui piece, 1,774 vertices), from `static/coast/v1/f/20_-160.bin`;
  placement there equals placement on the full chunk (G20 re-check R4: the bay-head land click).
- `land_parity.json`: 500 points round Oahu with their land / water answer on `oahu-t1.bin`; the server's
  `point_forecast.land_parity` (tests/test_point_forecast.py) and the page's `inLand` (tests/ui/tools.test.js) must both give
  them (G22 re-check R-A24/25).
