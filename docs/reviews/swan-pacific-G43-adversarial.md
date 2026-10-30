# G43: the Pacific-island SWAN tables and the SWAN freshness rule (plan section 64)

Branch `feat/swan-pacific` off production a2b6b4f. Test site `test` @ 75d0680 (the feature's site files + the test
branch's own logo lines). Production untouched through the review.

## The build (2026-10-30, Fable 5.1 HIGH)

- Commits: cea7286 (the code: app.py, forecast.js UI 1.26.9, the template's one variable, tests, fixtures, README),
  94754f8 (the page golden, its own commit), f1030fc (two exact-edge pins).
- `SWAN_STATIONS` gained 52200 (cdip121), 52202 (cdip196), 52211 (cdip197), 51209 (cdip189), 52212 (cdip219). Nothing
  else was needed for the tables: the fetch, the parser, the wind back-fill, the zone, the cycle text, the model bar,
  the head's rule, the overlay note and the no-script select all key on the map. The three Guam / Saipan buoys are NWPS
  stations too: GFS | SWAN | NWPS there (`tests/test_parse_nwps.py`'s "no station is both" pin changed).
- The freshness rule (the NWPS rule of section 59): regions hawaii / mariana / samoa / palau with one representative
  table each; `_SWAN_UPDATED[region]` = {updated, ts (last good read), failed, error} written by every table fetch and by
  `_swan_probe` (one HEAD of the representative table), started by `_swan_tick()` from the live scheduler's pass (one
  region per pass, the oldest record first, record over 5 min old, a failure retried after a minute, one thread at a
  time; `SWAN_PROBE=0` off); `swan_age_h` / `swan_offered` from the held record only (a record whose last good read is
  over 6 h old decides nothing); `resolve_model(..., nwps_ok, swan_ok)` checks a station's membership unconditionally
  before the caller's judgement; the payload judges `swan_ok` once, carries `swan_age_hours`, and falls back to the GFS
  table when the SWAN table's own cycle line is over 72 h old; `parse_swan`'s cache key carries the held publish time
  (the first build is filed under the time it recorded too); `_old_run_wind_final` = the NWPS or the SWAN 48-h rule;
  `index()`'s `swan_stations` = the offered stations, `old_run_note` (was `nwps_old_note`) from either model's age;
  forecast.js `follow` keeps both lists in step, `oldNote` from `swan_age_hours` under SWAN (`OLD_RUN_H` 36); `/healthz`
  `swan`; the fork reset.
- Found by the first test and fixed before the commit: `resolve_model` with `swan_ok=True` at a non-SWAN station
  answered SWAN (the NWPS branch had the same shape); the membership is checked first now, for both.
- Tests: tests/test_swan_pacific.py 17 (real fixtures: Guam's and Palau's tables cut to 30 rows), window.test.js + 1,
  early.test.js + 6 cases; pins moved. pytest 1,878 (one load-sensitive g42 trickle test fails under the full run and
  passes alone, as before), Node 562. Mutants (scratch swan64/mut64.py, a scratch copy): 27, 25 killed, the two
  survivors (the payload rule at exactly 72.0 h, the wind rule at exactly 48.0 h) pinned in f1030fc.
- Fact: all 19 PacIOOS tables carried one Last-Modified (2026-10-29 23:00:01 GMT) today: the regions share one publish
  time now; the per-region record is a safeguard.

## Test site (2026-10-30, UI 1.26.9 live 18:29 UTC)

- API (scratch swan64/v64_api.py): all 17 SWAN answers; the five Pacific tables = an independent parse of the downloaded
  tables (175 / 175 / 175 / 175 / 271 rows, 0 mismatches in the partitions, the combined height and the hour in the
  station's zone), zones Pacific/Guam / Saipan / Pago_Pago / Palau, wind complete (R2 winds back-filled), age 19.5 h;
  the model matrix (NWPS at 52202 / 52211, GFS for NWPS at 51209, GFS for SWAN at 46001); /healthz swan (four regions
  read, probes running).
- Browser (headless Chrome, trusted presses; scratch swan64/v/s64_site.js), desktop 1280x800 and phone 375x812: at each
  of the five stations GFS -> SWAN -> (NWPS at Guam / Saipan) -> GFS round trips with one request each, the bar
  GFS | SWAN | NWPS or GFS | SWAN, "SWAN · updated 20261029 23 UTC", the Location line and zone, 175 / 271 rows, no
  note at 19 h; 51201 unchanged; `?model=SWAN` at 46001 -> GFS (the head asked GFS); a `?model=SWAN` link at 52202;
  render=full at 52212 (271 rows, the meta line); no page sideways scroll on the phone; console clean.
- Owner awareness (pre-existing, the GFS path): 52212's GFS Location line reads "52212      ( 7.69N 134.67E)" with
  NOAA's bulletin spacing; the SWAN line is tidy.
- The freshness rule, shown on local servers of the feature code with the SWAN clock moved (scratch swan64/v/run_shifted64.py +
  s64_age.js; headless Chrome, trusted presses): every region 40 h older -> 52202 and 52212 under SWAN carry "· 60 h old" in the
  header span and on the meta line (the table's own cycle line is 60 h old at that clock), GFS and NWPS at the same station carry no
  note, the SWAN table comes back from the page cache; phone the same. The Pacific regions 73 h older with Hawaii fresh, on a
  FRESH server: the first `?model=SWAN` request at 52202 is answered GFS (the read-time fallback: the fetched table's cycle line is
  over 72 h old), the page then lists 12 offered stations (the five Pacific ones out), the bar shows GFS | NWPS at 52202 (SWAN
  withdrawn, NWPS kept), Palau's bar is hidden (GFS only), while 51201 is served its SWAN table ("updated 20261102 00 UTC" at
  that clock, no note); `/healthz` swan: hawaii 19.7 h, the three Pacific regions 92.7 h, four probes started.
- Found on the first +73 h run and fixed @ 14e8cb2: `index()` asked `swan_offered()` per station at different instants, so a
  probe recording a region in between gave `swan_available: true` with the station missing from the 16-entry list (one request).
  `swan_offered_set()` judges all 17 under one lock read; the flag and the list come from that set. Pinned: the set equals the
  per-station rule in every state (incl. an expired held record and the exact 72.0-h edge: that mutant survived without the pin);
  5 mutants, 5 killed. After the fix the +73 h run's first page listed 12 with the flag false.
- The probe cadence on the test site (/healthz every 30 s for 13 min, no SWAN request from the watcher): each region re-read
  every ~300-335 s by one probe per scheduler pass (probes 3 -> 11; hawaii / mariana / palau / samoa read ages cycle 0 -> ~310 s).
- Full suites at 14e8cb2: pytest 1,812 + the 55 of the load-sensitive G42 file run alone; Node 562. The test branch's own suite
  at 75d0680: 1,400 passed + the five failures it always has (its logo lines vs the page golden x4, the numpy-import test).

## G43: one fresh-context reviewer (2026-10-30, Fable 5.1 MAX; report scratch g43/g43-report.md)

**0 P0, 0 P1, 1 P2, 6 P3.** Reviewed f1030fc and 14e8cb2. Held up with its own evidence: its own parse of the five
downloaded PacIOOS tables = the test site's served rows (971 rows, 0 mismatches; zones, Location, wind complete); one
publish time per region (19 HEADs); the live model matrix on the test site (6 stations x 8 `?model=` values: page state =
API = head lists = bar = select) and the same matrix on a fake clock in five states (240 cases, 0 disagreements across
server / index / head / loader) plus render=full in six states; the > 72 h fallback with `parse_bull` failing; the record
under concurrency (a fetch and a probe at once, a failed probe during a fetch, four ticks -> one probe, a dead thread
frees the slot, the due order, the fork reset); the cache key following the held time; headless Chrome against its
worktree with a mutable clock (52202 hidden at load, back in the open tab through real presses with "· 60 h old" in the
span and the meta line, withdrawn again, the NWPS and SWAN notes never both, the phone's note span). 32 mutants, 27
killed; the 5 survivors were test gaps (F2, F7).

| # | Finding | Outcome |
|---|---|---|
| F1 (P2) | A SWAN table >= 48 h old with blank early wind cells answered `wind_complete: false` (the payload line still read `model == "NWPS"`), so every open tab re-asked `/api/forecast` every minute for the ~475 KB answer through the 48-72 h window while the server kept the table 30 min; the plan said true | FIXED @ 563eccb: `model in ("NWPS", "SWAN") and _old_run_wind_final(...)`; pinned at 30 h (false) / 50 h (true). With it the heal constant became 36 h (the reviewer's context note: the GFS run covering the table's first shown row leaves R2's 8-run window ~37 h after the publish, so between 37 and 48 h the blanks were "healable" and refetched every 5 min for nothing); the exact-edge pin moved to 36.0 h. A deviation from the plan's 48 h, on record |
| F2 (P3) | `render=full`: the list came from one `swan_offered_set()` read while the flag was overwritten by the payload's own judgement; mutant M20 survived | FIXED: when the read's judgement differs from the held set's, the set is judged again and a withdrawal drops the region's siblings too; pinned (the inline fallback at 72.5 h: flag false, the three Mariana stations out, 14 listed) |
| F3 (P3) | `_swan_record` wrote `updated` unconditionally: a fetch finishing after a probe regressed the held time to the older table | FIXED: `max(held, updated)`, a region's table never gets older; pinned |
| F4 (P3) | `index()` used `min(_NWPS_NOTE_H, _SWAN_NOTE_H)` for both models' notes | FIXED: the payload's model's own threshold; pinned with `_NWPS_NOTE_H` 10 |
| F5 (P3) | An epoch-0 Last-Modified hid the region while the cycle line and /healthz treated it as none | FIXED: a falsy time counts as none in `_swan_record` too; pinned |
| F6 (P3, design risk) | One record per region, written by every table: a lagging nest's table would flip the region's rule every <= 5 min | RECORDED. F3's never-regress rule removes the flip (the lagging table's own cycle line still withdraws it per request). The premise holds today (every table 23:00:01-02 GMT) |
| F7 (P3) | Test gaps: M11 (the API error answer's flag), M19 (a tick with nothing due), M20 (F2); M27 / M31 near-equivalent | PINNED: M11, M19, M20 |

Fix-round mutants (scratch swan64/mut_fix.py, scratch copies): 9 of 9 killed. The reviewer's reading of 14e8cb2's
evidence (the 16-entry list was probably the page's own `follow()` mutating the seed, not the race) is plausible; the fix
stands on its own (two reads at two instants). Not finished by the reviewer: the overlay's run tail under SWAN, the probe
cadence over a full 5 min on the test site (the author's watcher covered it: ~300-335 s per region), the wind cells vs
the bucket's winds.bin (non-blank in every row checked).

## Test site after the fix round (2026-10-30, 19:18 UTC)

- `test` @ e6cc13a (cherry-picks of 14e8cb2 and 563eccb; its own copy of the test file taken whole), redeployed 19:18 UTC.
  The API check again: all 17 SWAN answers, the five Pacific tables = an independent parse (175 / 175 / 175 / 175 / 271 rows,
  0 mismatches), zones, wind complete, age 20.3 h; the model matrix; `/healthz` swan four regions read, four probes; a
  render=full page at 52202: SWAN, flag true, 17 listed, no note (the table is fresh).
- The fix round on the shifted local servers of the fixed code: +40 h, 52202 under SWAN "· 60 h old" in the span and the
  meta line, the API answer `wind_complete` true at 60.3 h (F1); +73 h with Hawaii fresh: the first SWAN request answered
  GFS, 52202 out of the list, GFS | NWPS, Palau hidden, 51201 keeps SWAN; render=full at 52202: GFS, flag false, 12 listed
  with every Mariana / Samoa / Palau station out (F2's region rule), at 51201: SWAN, flag true, 12 listed.
