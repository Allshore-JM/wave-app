# G30a — adversarial review of the ECMWF overlay frames job (plan section 43, step 2)

Scope: `git diff ad4cebc..61addd1` on `feat/ecmwf-overlays`: the ECMWF open-data job (`tools/model_frames/ecmwf.py`,
`ecmwf_fetch.py`), the decode additions, the `Source` refactor of `run.py` / `publish.py`, the workflow, trigger and
keepalive wiring, tests and fixtures. One fresh-context reviewer (Opus 5.5, HIGH), 2026-10-15; independent decode with
eccodes 2.41 against the job's frames; own worktrees at 61addd1 and ad4cebc (removed); nothing written to R2 or GitHub.

Result: **0 P0, 1 P1, 1 P2, 6 P3.**

## Findings and outcomes

| ID | Pri | Finding | Outcome |
|---|---|---|---|
| F1 | P1 | ecWAM keeps the sea-ice pack as wave points (swh median ~0.04 m) with a meaningless peak period up to 29 s; GFS-Wave leaves ice empty. At f072, 41,837 cells >= 22 s (GFS at the same valid time: 79); the unchanged client would paint both ice packs as the longest-period swell and the ice as 0-ft water. | FIXED: `ecmwf.mask_calm` leaves hs/tp/pdir empty where swh < 0.1 m (the job's calm line). Author's measurement, run 2026101412: ~78-82k cells a step emptied, 99 % poleward of 55 deg; GFS's frame is empty on 89 % of them; under-ice cells with >= 0.5 m (marginal ice zone) are kept (GFS draws 72 % of them). Masking by `sithick > 0` instead was measured and rejected: it would drop 25-34k real wave cells. Model cells >= 22 s left after the mask at f000/f150/f360: 0-2 a step (but see R1: f072 keeps 907 such cells under thin ice). Tests on three real ice cells (Arctic, Antarctic, a 2.1 m wave under thin ice). |
| F2 | P2 | The 0.1-deg fallback is unreachable: every path is `ifs/0p25/`; a withdrawal ends in a silent, green "no complete run" forever. | FIXED (visibility): `stale_fail_h = 30` for ECMWF: when no complete run is found and the live ECMWF run is older than 30 h (or none is live), the job exits 1 with a summary WARNING (the workflow fails; GitHub mails). GFS unchanged (exit 3, store never read). README: a resolution switch needs the path changed in `ecmwf_fetch._prefix`; the regrid does the rest. |
| F3 | P3 | 0.1-deg regrid: the angle rule keeps a direction where the scalar rule leaves the cell empty. | FIXED by F1: `mask_calm` empties pdir wherever hs is missing. |
| F4 | P3 | The credit lacks ECMWF's copyright statement, "Source: www.ecmwf.int" and the liability disclaimer. | FIXED: the manifest attribution is ECMWF's notice block (copyright with the year, source, CC BY 4.0 with the link, the modifications, the disclaimer, no endorsement). The map credit and the panel are step 3. |
| F5 | P3 | The client's 9-h stale banner would fire between 12-hourly ECMWF runs. | CARRIED to step 3 (plan: `staleS` 15 h per model). |
| F6 | P3 | data.ecmwf.int is tried only after the full AWS retry ladder (~20 s) per request: a failing mirror would push a run past the 60-min timeout; discovery has no fallback. | FIXED: two quick AWS tries per read, then the second host; after an AWS transport failure the mirror is skipped for 10 min (`AWS_COOLDOWN_S`). Discovery stays on the mirror (a listing failure exits 1, visible). |
| F7 | P3 | Mutant survivors that matter: M22 (identity check in `decode_ecmwf`), M40 (an AWS 404 not falling back), M13 (GRIB trailer), M07 (64 MiB span cap), M08 (offset 0). No test of sea-ice cells or regrid mask agreement. | FIXED: tests for each (identity through `_read`, 404 fallback without a cooldown, trailer, span cap, a real record at offset 0) plus the ice cells. M21 (angle tie), M35, M46 equivalent in practice. |
| F8 | P3 | Nits: README "~50 MB a run" (measured ~125 MB); the pp1d/swh mask difference is 521 cells (39 net); mwd 360.0002 counted as clamped; the GFS `--steps` message wording changed. | FIXED: README ~125 MB; directions wrapped to [0, 360) in `mask_calm`; the GFS message restored ("hourly 0..120, then every 3 h to 384"), pinned. |

## Confirmed by the reviewer
- Field identity: swh, pp1d ("Peak wave period"), mwd ("Mean wave direction", degrees true) at surface level 0; against
  the live GFS frame at the same valid time the direction difference has a median of 14 deg (FROM, like GFS) and the
  tp ratio ECMWF/GFS a median of 0.999 (a peak period).
- Geometry, no roll: values placed with eccodes' own latitude/longitude arrays equal the job's frames at steps 0, 72,
  150, 360 within half a quantum; every model cell coded; half = full[::2, ::2]; the fill inside the allow mask; spot
  checks at the dateline, Hawaii, Drake Passage, 50 S 90 E, North Atlantic, Caspian, Lake Superior.
- Discovery against the real bucket (pagination, 06Z/18Z never picked; AWS keeps runs back to 2023).
- Prefix isolation: only `ecmwf/0p25/v1/` touched; a deterministic GFS build identical at ad4cebc and 61addd1 (47 objects).
- The author's mask and fill counts and the dry-run code digest 9df7d39b80727806 reproduced.
- Encode ranges: no clamping in four steps (hs max 16.2 m, tp 1.03-28.97 s, wind max 105 kt).

## Fix round (author)
On top of 61addd1: `ecmwf.mask_calm` + `CALM_M`, `EcmwfSource.model` (notice block with the year), `stale_fail_h` and
`run._no_run`, `ecmwf_fetch` AWS tries / cooldown, the GFS `--steps` message, README, 11 new tests (test_ecmwf 48; job suite
188); 15 mutants of the fix round, all killed (the copyright year after an injectable clock). Real records (steps 0/150/360): every field within half a quantum of eccodes with the calm rule applied; GFS job
output still byte-identical to ad4cebc (58 objects). The temporary dry-run workflow and its check script are removed
before the merge (a test asserts it).

## Re-check of the fix round (the same reviewer, 61addd1..7d3276d)
Nothing above P3; F1, F2, F3, F4, F6, F7 and F8 confirmed fixed (F5 carried to step 3). Frames built through the job from
real records at f000/f072/f150/f360 with `sithick`: tp cells >= 22 s 3,028 / 44,030 / 38,752 / 24,374 -> 24 / 1,513 / 198
/ 189; every under-ice cell with swh >= 0.5 m kept in hs, tp and pdir (34k / 31k / 26k / 25k; one tp cell where pp1d itself
is missing); nothing outside the calm rule changed; wind/wdir byte-identical; GFS empty on 90 % of the cells the mask
empties (f072); calm open water emptied 571-1,236 cells a step, mostly coastal, 89-368 left empty after the fill (none near
Hawaii, at most 17 in the Mediterranean): the 0.1 m line stands. GFS build at 7d3276d byte-identical to ad4cebc. A real S3
"503 Slow Down" was met during the re-check (the cooldown will be used).

| ID | Pri | Finding | Outcome |
|---|---|---|---|
| R1 | P3 | The "0-2 cells >= 22 s a step" figure is wrong at f072: 1,513 such cells (907 model cells under ice with swh 0.1-0.36 m, 503 fill copies) vs GFS 79. | Record corrected; documented in the README as a limit (partly physical: long swell under ice). No stricter line (no ice concentration in the free data; `sithick` would drop real waves). |
| R2 | P3 | The coastal fill re-paints 2.9-3.8k emptied cells a step, mostly coastal pack ice poleward of 60 deg. | ACCEPTED, documented: the same one-cell limit the GFS fill has along ice-bound coasts. |
| R3 | P3 | The alarm fired at ~40 h, not 30 h (the 48-h discovery keeps finding the stale run: "already live", exit 0), then on every tick (~144 mails a day). | FIXED: `run._stale_check` runs on both paths (no run; newest run = live run); at most one failure every 6 h (`STALE_ALARM_EVERY_H`, record `<prefix>/stale-alarm.json`). Tests: 29 h no alarm, 31 h alarm + record, throttled, again after 6 h, the no-run path throttled. |
| R4 | P3 | Survivors: an unreadable pointer in `_no_run` returning 3; the disclaimer text not pinned; `<`/`<=` at 30 h (equivalent). | FIXED: both pinned. 5 mutants of the alarm rework, all killed. Job suite 191. |
