# Overlays — G9 adversarial review (direction fields, plan section 21 phase B) — 2026-09-25

Gate G9 of plan section 21, before the job-only merge. Phase B adds two data fields for the Phase C animation, built
on `feat/overlays-dirs` @ 36ae37a (off production 5ea31e7). No client or page changes.

- `pdir` = NOAA `DIRPW:surface` (primary / peak wave direction, degrees true, the direction the dominant waves come
  FROM), published full + half, coastal fill by nearest model cell (never a mean of angles; asserted at import).
- `wdir` = GFS 10 m wind direction FROM, `atan2(-u, -v) mod 360` from the earth-relative u/v the job already fetches,
  half resolution only.
- Circular coding inside the existing u8-linear-v2 formula (lo 0, hi 360): `q = 1 + (round(v * 254 / 360) mod 254)`,
  so 0 and 360 share code 1, code 255 is never used, error at most 360/508 = 0.709 degrees.
- Manifest fields gain `resolutions`, `circular` and `convention`; the stats sidecar gains `pdir_mask_mismatch` (cells
  with waves but no direction, or a direction without a height) and `pdir_missing_calm` (flat calm, hs = 0).

One fresh-context reviewer at HIGH effort (Opus 5.5), no access to the author's reasoning. It decoded 7 real NOAA steps
from two cycles (2026092512 f000/f024/f120/f240, 2026092500 f000/f072/f180) with its own JPEG2000 decoder and ran 14
mutants on a copy of the job.

## Findings

Report: scratch `g9-report.md`. **0 P0, 0 P1, 2 P2, 5 P3**; verdict: fit for the job-only merge.

| # | Sev | Finding | Outcome |
|---|---|---|---|
| P2-1 | P2 | The wind-direction wiring was not pinned: the test fixture returned an independent random grid for every GRIB key, so `wind_dir_from(v, u)` (a mirrored wind direction) passed every test. | Fixed: `test_each_published_field_decodes_to_its_own_grib_record` feeds per-key constants (HTSGW 2 m, PERPW 12 s, DIRPW 45, UGRD +5, VGRD 0) and decodes the published PNGs: half/wdir = 270, pdir = 45 (full and half), hs/tp/wind their constants, each within half a code. |
| P2-2 | P2 | New hard dependency on the workflow's pinned eccodes 2.48 that was only checked on 2.41: if 2.48 named paramId 260233 differently, every step would fail and hs/tp/wind would stop updating too. A dispatch dry run was proposed. | Checked without a dispatch: python-eccodes 2.48.0 with library 2.48.0 (the pinned versions) in a scratch venv, on the real records of run 2026092512 f000/f120/f240: DIRPW reports `dirpw`, paramId 260233, "Degree true", and the job's own `check_identity` passes (PERPW too). The first scheduled run after the merge is watched; rollback tag `prod-pre-dirs`. |
| P3-1 | P3 | The fill rollback wording said "hs and tp"; pdir is now filled too. | Fixed in README, `run.py` docstring, `encode.py` docstring and the test comment. |
| P3-2 | P3 | The coverage stat checks the model grids, not the published frames: after the fill, 37-159 cells per step are drawn for hs with no pdir, all hs code 1 (0 m) in the Arctic pack, because calm cells seed the hs fill but not the pdir fill. | Documented (README, `run.py` comment). No visible effect: Phase C draws arrows only where hs >= 0.1 m. |
| P3-3 | P3 | The manifest's `encoding_spec` clamp semantics ("<= lo" / ">= hi") do not hold for circular fields. | `encoding_spec.circular_fields` added; README says a reader must branch on `circular`. Phase C readout note. |
| P3-4 | P3 | `wind_dir_from` could return 360.0 after the float32 cast; an exact calm reads 180. | Re-wrapped to 0 after the cast (`test_wind_dir_from_never_returns_360`); the calm case documented. |
| P3-5 | P3 | Mutation survivors: the summary warning (M6), the linear coder through `encode_frame` (M8), the calm threshold (M11). | Tests added: `test_pdir_mismatch_warns_in_the_step_summary`; 359.5 and 360 through `encode_frame` code 1 and `q.max() <= 254`; a 0.05 m cell without a direction counts as a mismatch, not a calm. |

## Verified correct by the reviewer

1. DIRPW identity: shortName `dirpw`, paramId 260233, "Primary wave direction", "Degree true", surface level 1,
   grid_jpeg, one record per .idx (record 7 of 19); 0.91 MB per step (+74 MB NOAA download per run).
2. DIRPW is FROM: in wind-sea-dominated cells (63-76 k per step) the difference to the wind-from direction has median
   17-22 degrees and 0.00 % beyond 135 degrees on all 7 steps. Near coasts only 4-5 % point from land, and those are
   offshore-wind seas (85-90 % with the wind also from land). Spot values from the sea at SF, Oregon and Florida.
3. Wind direction against NCEP's own WDIR (wind > 3 m/s, ~530 k cells per step): median 0.15-0.28 degrees, 0 cells
   beyond 90 degrees.
4. Circular coding on real DIRPW: round-trip max 0.7087 degrees, codes 1..254 only, 0 and 360 both code 1.
5. Model-grid coverage: `pdir_mask_mismatch` = 0 on all 7 steps; calm cells without a direction 8-116 per step.
6. Fill: every filled pdir cell equals a model DIRPW value within 5 cells; the import-time assert refuses a mean fill
   for any circular field; wind and wdir are never filled.
7. Guard: `fill_key` changes (fields and methods), so the live run cannot be `--force`-rebuilt (exit 2, no writes), a
   scheduled tick while it is live does nothing, and a complete old-code run is re-pointed without pdir. The first run
   with direction data is the first new NOAA cycle after the merge. `FILL_VERSION` need not change (hs/tp pixels are
   unchanged).
8. Live client compatibility (asset 2.7.6 and every older version): fields are read by name, `m.fill` only
   type-checked, no iteration over `m.fields`; a new manifest is 6,784 B (cap 12,000). Node 88/88.
9. Publish, prune and retention: wdir writes only `half/wdir/fNNN.png`; all keys under `<RUN>/`, so pruning removes
   them with the run.
10. Costs: pdir 234-236 KB full + 83-84 KB half, wdir 155-156 KB half per step = +38.2 MB per run (+47 %), +243 objects
    per run; build expected about 500-550 s on the runner (live 382 s), far inside the 60-minute timeout.

## After the fixes

Job tests 57, full pytest 389; the eccodes 2.48 check script is scratch `g9/` (not committed). Record committed with the fixes on `feat/overlays-dirs`; job-only merge approved by the
owner ("continue with G9 and the job-only merge"), rollback tag `prod-pre-dirs` @ 5ea31e7.
