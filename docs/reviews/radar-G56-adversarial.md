# Radar layer (plan section 85): review gate G56

## G56a: the job

One fresh reviewer on `git diff e51c2eb..67ac1c2` (the job: `radar.py`, `mrms.py`, `opera.py`, the workflow, the Worker's list, tests).
Its result: **0 P0, 2 P1, 7 P2, 10 P3**. 59 mutants run, 33 survived. No wrong pixels on real files; deletes stay inside `radar/v1/<slot>/`.
The author re-ran its scripts on the same code and reproduced every failure before fixing anything.

### What it checked and found right
- C1: every pixel of one real file per domain (5 domains, about 49 M points) against eccodes: value difference 0, lattice mismatches 0,
  including Alaska's date-line side and Guam.
- C2: the corners by pyproj land on the grid's outer corners within 1 mm; 162 full rows of OPERA (1.59 M points, 74.995 N and 31.005 N
  included): index and value mismatches 0, forward vs pyproj 0.0000 m.
- C3 / C4: the code table, t0 and t1 against a naive block sort, every pixel of every written cell by the lat/lon formula, the bitsets: 0 wrong.
- C5: a chaos run (400 ticks, 10 kills, random put, delete and source failures): no pointer naming a missing object. C6: a retired slot
  cannot refill in the same tick; the delete prefix is exact. C8 / C9: pins, secrets scope, licence words in the credit.

### Findings and outcomes
| # | Finding | Outcome |
|---|---|---|
| P1-1 | A reader error other than ValueError (bad deflate, PNG bomb, damaged HDF5 chunk, string dataset, `data` as a group, array attributes) ended the whole tick: Europe starved for up to 24 h, exit 0, no alarm | Fixed: `_reading` turns anything a file does to a reader into ValueError; a test per row |
| P1-2 | A format break in one region stopped every later region and every older slot of that source | Fixed: at most `BAD_PER_TICK` (2) bad files of a region a tick, then that region alone is left; the others and older slots go on. A bad file counts toward a gap only when newer frames of the region read well, so a real format change burns no slots |
| P2-1 | A connection closed mid-body returned short data: a false "layout changed" mail and the source stopped | Fixed: the length is compared with `Content-Length`; short = asked again, then Down |
| P2-2 | HTTP 403 counted as "never published": a blocked runner gave up the whole day in three ticks | Fixed: only 404 is Missing (both buckets answer 404 for an absent key, checked live) |
| P2-3 | Alarms outside the try: a bucket refusing writes failed every run after 6 h, up to 144 mails a day | Fixed: an alarm that cannot be recorded is a summary line and a failed run only around 00:00 and 12:00 UTC (`rated`) |
| P2-4 | The 5-minute deadline was not tied to the workflow's limit (prune unbounded, boto 5 attempts x 60 s, the fetch timeout per read) | Fixed: `DEADLINE_S` checked before each region and each delete, at most `PRUNE_PER_TICK` (6) deletes, uploads stop at `HARD_S` (7 min), a whole file `FETCH_MAX_S` (2 min), the job's own boto client (10 s connect, 30 s read, 2 attempts; other jobs unchanged), workflow timeout 12 min |
| P2-5 | `gone` grew without limit and without an alarm when deletes failed | Fixed: one alarm when the oldest has waited over 6 h |
| P2-6 | A region going silent never alarmed (the alarm was per source) | Fixed, per region, on the author's call (the reviewer left it to the owner): one alarm per 12 h per region; told to the owner at the job-merge stop |
| P2-7 | 33 surviving mutants | Tests added for every real gap: see below |
| P3-1 | The first-ever tick alarmed | Fixed: the pointer's `started` is the clock for a region never seen |
| P3-2 | Corrupt state: uncaught or exit 2 every tick | Fixed: `good_pointer` checks shapes; a refused pointer fails only around 00:00 and 12:00 UTC; a corrupt alarm record is rated |
| P3-3 | A NaN corner passed the check | Fixed |
| P3-4 | A punctual cron run froze a frame at + 0 | Fixed: no MRMS frame before slot + 4:30 (`MRMS_READY_S`) |
| P3-5 | Lag text wrong | Fixed: 13-96 s, CONUS the latest |
| P3-6 | Objects of a slot never recorded stay forever | Recorded (very small; a monthly sweep would cover it) |
| P3-7 | Slots with `r: {}` in the pointer | Recorded in the job's header; the client must not count them as frames (step 3) |
| P3-8 | Nearest-source regridding never samples 7.6 % of OPERA's echo pixels (the lattice is coarser than 1 km north-south) | Recorded; for the owner at the job-merge stop |
| P3-9 | Dead constant, header wording, User-Agent "satellite waves", one shared `format` alarm kind | Fixed: the job has its own `_get` and User-Agent (no e-mail); alarm kinds per region |
| P3-10 | CI runs pillow 11.2.1 / numpy 2.2.6, the job 12.3.0 / 2.4.6 | Recorded (the repo's convention); the runner dry run used the job's own environment |

Author's addition during the fix round: `mrms.read` also treats Pillow's `DecompressionBombError` as a damaged picture, and `main` guards the alarm pass itself (rated).

### After the fix round
- The reviewer's scripts on the fixed code: reader rows, one region's break (other regions and older slots publish), a cut body (Down, no alarm), a 403 (0 of 138 slots given up), a refused bucket (0 failed runs off the half-day), first tick, corrupt pointers, `gone`.
- Chaos: 700 ticks, 29 kills: 0 violations, 0 orphan prefixes, 0 leaks.
- Read-back on a fresh real slot (202611060210): 0 mismatches over 6 regions and 3 tiers (12,000 t0 + 1,380 t1 + 6,000 t2 samples).
- Tests: `test_radar.py` 91 -> 186; the job suite 697 + 1 skipped (one wall-clock test of the spectral job failed once under the full run's load and passes alone, as in section 83).
- Mutants: 55 of the fix round, all killed (4 first survived and were pinned: the delete limit in `main`, the bomb at the reader, a retired slot on a quiet tick, a local build's alarms). The reviewer's set: 52 of the 59 still apply, 51 killed after one more pin; 1 survivor equivalent (`raw <= nodata`: values below it already read as no coverage); 7 no longer match (their code was replaced; the fix-round set covers it). The author's first set: 38 of 39 applicable killed; 1 equivalent (the slot-level deadline, now also checked before each region).

For the job-only merge: writes ~135-138 objects a slot on a quiet night (~580k a month beside ~800k today, so ~1.4 M: ~$1.7 a month); the ceiling 410 a slot (1.77 M a month, ~$7).
