# G25 — adversarial review of "live buoys on the map at once" (plan section 36)

Reviewed: `feat/live-bg` @ 92d3c1e (buoy_sources.py stale-while-revalidate + ThreadRunner + fork reset, app.py background
scheduler + never-waiting route + `/healthz`, static_ui/livelist.js UI 1.17.0, templates/index.html, start.sh, README, tests)
against production `Live-Buoy-Update` @ ce87c96, on the test site wave-app-clean.onrender.com (branch `test` @ a773e25).
Two fresh-context reviewers (Fable 5.1, MAX), 2026-10-04: reviewer A — the server (concurrency, the provider rules, the
golden, the route, the scheduler, fork safety, `/healthz`, 52 mutants; no browser); reviewer B — the test site as a visitor
sees it, the client code and its mutants (50 of the module, 15 of the template's loader block), 50 minutes of polling.
Their reports and artefacts are in the session scratch directory `g25/a/` and `g25/b/` (brief: `g25/brief.md`).

**Result: 0 P0, 2 P1, 1 P2, 16 P3.** The core held under everything both reviewers threw at it (~26,000 randomised
requests against a real scheduler and runner with failing feeds: no deadlock, no request over 129 ms, every body valid and
consistent with the memo; the golden byte-identical with the same fetch schedule; on the test site 1,086 polls through two
background refreshes: route p50 173 ms, 0 partial answers, 0 errors, 304s working; every failure drill in the real page
ended in the designed state). The two P1s are both about what happens in the first minutes after a restart — the very case
this release exists for — and both are fixed in the fix round below.

## Findings and outcomes

| # | P | Finding (the reviewer's evidence, condensed) | Outcome |
|---|---|---|---|
| A-1 | P1 | A click on a REMEMBERED marker of a provider that has no list yet holds a server thread for that provider's whole list fetch: `detail()`/`latest()` of AODN, MI-IE, CEFAS and CMEMS call `self.list_stations()`, which on a cold provider waits on the scheduler's in-flight fetch (or fetches inline if no job is running); `/api/buoys/<id>/latest` runs it on the request thread. Measured with the REAL provider classes and a slow fake HTTP: `/latest` waited the whole 1.5 s list fetch; four concurrent clicks on four cold providers held four threads (MI-IE = the list fetch PLUS its direct per-station fallback). Before this release no marker existed until every list had loaded, so the path was unreachable; now the page draws the remembered list at once while the server's providers are still cold (AODN 18 s, CMEMS 34 s, MI-IE ~128 s on the test site; ~250 s per MI-IE click with its 3 x 40 s timeouts twice over). Three or four clicks in the first two minutes after a deploy = every gthread held. Confirmed by the author (A's harness re-run: identical numbers). | FIX: with the service on, a cold provider never waits on a request thread — `/api/buoys/<id>/latest` and `/components` answer 503 `{"error": "The buoy list is still loading", "retry": true}` with `Retry-After: 5` and `no-store` (and queue the refresh); the page's live window retries quietly (spinner stays) up to ~2 min before showing an error; as defence in depth the providers' cold path under the service queues the refresh and returns the empty list instead of waiting. The pre-existing per-station fallback fetch (MI-IE) on a request thread is recorded below (P3). |
| A-2 | P1 | `/healthz` 503-until-complete can make Render restart a RESTARTED instance in a loop and cuts traffic on every restart: Render's docs (fetched 2026-10-04) say a running instance failing consecutive checks for 15 s stops receiving traffic and for 60 s is restarted; only deploys get the 15-minute window. `/healthz` is 503 until the served list is complete or 90 s passed; complete needs every provider's first answer (CMEMS 34 s; MI-IE's [] at ~128 s on a day its ERDDAP times out, i.e. today). A's timeline harness on the real `healthz()`: MI-IE at 128 s -> first 200 at 90 s, 90 s of consecutive 503 (traffic cut at 15 s, restart at 60 s, loop until MI-IE recovers); a healthy day -> 34 s of 503 = a ~20 s site-wide traffic cut on every restart that did not exist before. The author re-ran the harness: identical. The author's own production watch today showed a Render settings change restarting production (a redeploy): restarts are not rare. | FIX: `/healthz` answers 200 once the scheduler has completed a pass (the process serves pages and partial lists: it is functioning); 503 only before that, while `stalled` (no pass for LIVE_STALL_SEC: a restart is the right remedy), while builds keep failing (A-6), and — for deploys — while the served list is incomplete during a short grace `LIVE_WARM_DEADLINE_SEC`, now default 10 s and clamped to at most 14 s (under Render's 15 s rule), documented with Render's rules in the README and the plan. Deploys lose the "wait until every agency answered" beyond that grace; the remembered list and the partial answers cover it. Owner informed. |
| A-3 | P2 | `tests/test_concurrent_traffic.py::test_sixteen_threads_with_the_background_service_on` is timing-dependent and leaks: its final `_live_tick` queues refreshes on the real ThreadRunner that keep running after the test returns (the lock assertion fails when one is in flight; the golden replay, which compares `fetch_calls` per step, failed in 2 of 11 follow-on runs). Author: 4 failures in 8 runs of the two files together. This produced false "kills" in both reviewers' and the author's mutation runs. | FIX: the test drains the runner (no queued job, no provider pending or in flight) before its assertions and before returning; the author's mutation scripts re-run on the fixed suite. |
| A-4 | P3 | `_notify_publish` runs INSIDE `_refresh_lock` (`_refresh_locked` is only called under it) though the docstrings say "outside every lock"; a listener touching the publishing provider would self-deadlock. Harmless with the one listener (`_live_wake`). | FIX: the callers notify after releasing `_refresh_lock`; the docstring then holds. |
| A-5 | P3 | Env knobs unguarded: `LIVE_TICK_SEC=abc` -> import error (the deploy fails at the import check); `LIVE_STATIONS_EDGE_TTL=inf` -> OverflowError -> the list route 500s; `LIVE_TICK_SEC=0`/`-1`/`nan` -> ~70k scheduler passes per second; `=inf` -> the scheduler thread dies and is restarted (and dies) on every request. | FIX: one `_env_number` helper (default on a bad value, clamped to a sane range) for every knob; `LIVE_STATIONS_EDGE_TTL` catches OverflowError and treats nan as 0; tests. |
| A-6 | P3 | A memo build that keeps failing is invisible: the route serves `[]` partial-all forever while `/healthz` says ok after the deadline with `errors` climbing (no known trigger today; before the release the same fault was a visible 500). | FIX: consecutive build failures counted; `/healthz` reports `build_failing` and answers 503 from the third in a row (self-healing by restart, like `stalled`). |
| A-7 | P3 | `stop_live_background` + `start_live_background` overlapping a pass can leave 2-3 scheduler threads looping (tests-only API). | FIX: the loop exits when it is no longer the recorded scheduler thread. |
| A-8 | P3 | `get_buoy_providers()` has no lock: the first request's scheduler thread and the route can build two provider sets (20 warm-up jobs: every feed downloaded twice at boot); construction takes 0.045 ms, so rare. | FIX: a lock. |
| A-9 | P3 | `status()["age_s"]` after a kept failure = TTL - 300 (not the list's age); `good_age_s` is right. | FIX: `age_s` from the time the list in hand was published. |
| A-10 | P3 | Nits: `refresh()` ("fetch NOW") returns the kept snapshot without fetching inside the retry window; a legitimately empty feed bumps its version on every refresh (a rebuild for identical bytes); `/healthz` "never triggers work" holds from the second request (K-7); wall-clock due-ness under a stepped clock (theoretical); a 304 can carry the partial header. | FIX the docstrings; the rest ACCEPTED and recorded (the version is a publish counter by design: the golden pins it). |
| A-11 | P3 | Mutation: 22 of A's 52 mutants survive the repo's suite — equivalents and test gaps; the gaps worth closing: an UNBOUNDED `_LIVE_WAKE.wait()` passes (nothing checks that passes happen without a wake), a tz-load failure killing the loop, the fork reset of `_RUNNER_LOCK` / the wake event, the restart of a thread that died by exception, the daemon flag, the warm tick never switching to the normal tick, a double tz load. | FIX: pins for each. |
| B-1 | P3 | The "loading…" note flashes on a first visit and moves the tools/gear by 12 px: painted when the legend is created (~200 ms) and cleared when the list lands (~300-330 ms), around the first paint. On a return visit the HTTP-cached answer clears "cached list" within a few ms. | FIX: "loading…" / "cached list" shown only if the list has not landed within 300 ms (retrying / partial / unavailable stay immediate). |
| B-2 | P3 | "loading more…" when nothing has been drawn (a restart with no remembered list: every source missing, `[]`). | FIX: the partial status only when something is drawn; else "loading…". |
| B-3 | P3 | After PARTIAL_MAX (40 partial answers, ~9 min) the note clears although the list is incomplete and polling stops for good (reachable only when the server stays partial > 9 min). | FIX: after the schedule's end the page keeps asking every 60 s (for up to ~2 h) and the note stays "loading more…". |
| B-4 | P3 | The note is part of the checkbox's accessible name ("Live buoys cached list"); the live region starts hidden (`:empty { display: none }`), which some screen readers do not announce. Contrast is fine (4.7:1). "cached list" is understandable; "last known positions" would be plainer. | FIX: the note `aria-hidden` (a visual hint; the checkbox keeps the name "Live buoys"); the wording stays the owner's "cached list". |
| B-5 | P3 | A fixed 8 s deadline that restarts the download could starve a steadily slow server (the head request cannot be aborted and keeps downloading: two requests per visitor from 10 s on). Sizes are not the issue (18.3 KB on the wire); only a server slower than 8 s per answer is (the free test instance's spin-up; production answers in 0.1-0.3 s). | FIX: the deadline grows after two timeouts (8, 8, 16, 30 s). |
| B-6 | P3 | Test depth: 12 of B's 50 module mutants survive — real gaps: the 48 h literal, the deadline's `abort()`, a meta-only change in `contentKey`, a second `start()`, an empty-name row, the longitude bound; the template's loader block is pinned by strings only (B's 8-test Node harness kills 15/15 of its mutants, the needle test 9/15). | FIX: the pins; B's loader harness adopted as `tests/ui/livelist-page.test.js` (CI list). |
| B-7 | P3 | `/healthz` `runner.queued` counts running jobs too (steady state "queued 1, running 1" during MI-IE's retry). | FIX: `waiting` = queued minus running, both shown. |
| B-8 | P3 | The performance mark fires with the layer unticked (the list is loaded and stored; nothing drawn until the box is ticked, which is right). | ACCEPTED: the mark means "first list available"; documented. |
| B-9 | P3 | Server memory on the free instance: RSS 89.8 MB -> 96.8 (first hour) -> 109.9 after the first refresh cycle (+12 MB high-water mark) -> 111.8 after the second (+2 MB) -> 110.2; VmHWM 111.8 MB. Not a leak signature after two cycles; inconclusive for a day. | RECORD: watch the test service's memory graph for a day before the production merge (the author watches `/healthz` `rss_kb` over the re-check); nothing to change. |

## Confirmed correct (the reviewers' evidence, summarised)
- Concurrency (A): a harness with the real ThreadRunner(3), the real scheduler thread at 10-30 ms ticks, 25 % failing
  fetches, random hard and 90 % expiries, and chaos of `refresh()` / schedule / wake / tick / stop / start: 12x8x60 and
  30x16x60 rounds = ~26,000 live-stations requests (6,802 partial), 4,300 `/latest`, 2,900 `/healthz`; worst 129 / 113 /
  80 ms; every body a valid list whose ETag = md5(body); no served source ever named missing; partial always no-store,
  complete always `max-age=900`; no lock or flag left held, runner drained, memo current, no deadlock (a watchdog never
  fired). Lock-order audit: no inversion; the only callback under a lock is A-4.
- The golden replays byte for byte (11 steps: bodies, ETags, headers, the fetch schedule; the fixture unchanged); the
  background path's complete answer equals the inline path's bytes and ETag; a middle provider missing equals the inline
  answer with it raising (the merge is a stable priority sort). The inline branch is the ce87c96 route text.
- The provider rules: due at exactly 90 % of the TTL; retry 300 s after any failure (the first too); the kept list for 6 h
  then `[]` published once and the version held; a dead feed one version; a short non-empty list accepted; an empty list
  after a good one kept; cold singleflight against an in-flight job. The route: `_LIVE_EMPTY` before the first build, the
  partial header = the served bytes, a wake only on a key change, cold-only scheduling in warm order, the edge TTL only on
  complete answers, its parsing and cap. The scheduler: no lost wake-up (the clear at the top is right), bounded waits,
  the warm/normal tick switch, exceptions in a tick / the tz load / a listener never end the loop, one thread under a 16x20
  request storm, the fallback costs 0.3 us per request, a dead thread is restarted; a failed tz load degrades builds to
  "UTC" (visible as `tz_loaded: false`). `/healthz`: the 503/200/stalled/off rules, no work after the first request,
  0.28 ms, no-store. Fork reset: every module-level lock and event in buoy_sources.py and app.py (except `_POINT_BUILDS`,
  request threads only) replaced; with every lock held, a job in flight and two queued the reset frees all, drops the
  runner, clears the flags and keeps the lists; all ten real providers in the WeakSet; no thread starts at import;
  gunicorn's source confirms the master imports the app only under `preload_app`. Bounds: the queue holds at most one job
  per provider; a build of 562 markers = 4 ms of copies + 79 ms merge + 2.6 MB; ~24-60 log lines per hour per dead feed;
  main + 4 + 1 + 3 threads. start.sh and the README accurate. A's 52 mutants: no survivor is a code defect; the author's four
  scripts re-run: the recorded survivors confirmed equivalent (the false kills from A-3 aside).
- The visitor (B): first visit, empty storage, warm server, 1280x800: ONE list request (the head's), 107 ms, 18,579 B on
  the wire / 285,300 B decoded; first markers at 331 ms; DCL 313 ms; 562 rows stored (55 KB); console clean. Return visit:
  first markers 177 ms from the remembered list, the head request answered by the HTTP cache, note cleared by +1.5 s.
  Phone 375x812: 221 / 155 ms. Production (old code, warm): markers by ~620 ms; the release's gain is the cold / slow-server
  case (177.6 s on the idled test instance), where the remembered list now draws at ~0.2 s regardless of the server.
- Headers from outside: `Cache-Control: public, max-age=900`, `cdn-cache-control: no-store` (edge TTL unset: confirmed),
  the ETag (Cloudflare weakens it for identity answers; the server strips `W/`), 304 in 127-139 ms for both forms.
- Failure drills in the real page: a network error -> "cached list" -> "retrying…" at +7 ms, retries at 2.0 / 6.0 / 14.0 s;
  503 then 200 -> cleared at 2.0 s and stored; a hang -> aborted at 8.0 s, retried at 10.0 s; bad JSON and a non-array ->
  retried then stored; a partial answer (316 rows, AODN + CMEMS missing) with a remembered list -> the union of 562 drawn at
  once, "loading more…", polls at 3.0 and 6.0 s, storage untouched through the partial phases, stored on the complete
  answer; a restart simulation (every source missing) -> all 562 remembered markers, polled on schedule; a hidden tab -> the
  retry deferred and resumed on `visibilitychange`; storage: 47 h 59 m old drawn, 48 h 01 m not; +4 min skew drawn, +6 min
  not; wrong version / `t` as a string / not JSON / `s` not an array -> nothing drawn, no error; garbage rows dropped; a
  200,000-row list drawn in 256 ms; `getItem` throwing -> "loading…"; a `__proto__` key harmless; localStorage full
  (49 MB) -> the page still draws and clears the note. Markers from the remembered list open their windows (NDBC 51201 with
  the NOAA summary and components; AODN Collaroy with Australia/Sydney times and spectra); a fake id -> the panel's own
  error text; a marker dropped by the fresh list -> gone, the open window stays, no errors. `info.same` keeps the marker
  DOM nodes; a renamed station rebuilds them. Layer unticked at load: loaded and stored, nothing drawn until ticked. A
  code-driven legend rebuild re-paints the note; a checkbox click never rebuilds the list (Leaflet 1.9.4 source read).
  Layout: the note one 11 px line, the legend 144 px wide (150 with the longest text), 78 -> 90 px tall; phone: 15-21 px
  from the overlay box, no sideways scroll; the maximised forecast window 8 px below the corner with and without a note.
- Server from outside, 20:40-21:30 UTC: 1,086 `/healthz` + 1,086 list samples at <= 1 request/s: list p50 173 ms, p95 270,
  max 628 (an identity 285 KB answer); 304 p50 138 ms; `/healthz` p50 140 ms; 0 errors, 0 partial answers; through both
  background refresh cycles the route stayed 128-457 ms; the ETag changed four times (= the rebuilds); 180 of 183
  conditional requests got 304; memo `current` false in 3 samples only (the <= 2 s publish-to-rebuild gaps), `complete`
  always; builds 0.86-1.49 s in the scheduler thread; no stall; MI-IE retried every 300 s and stayed at version 1 with `[]`.
  No HTML injection path added (static legend string; note, panel header and errors via textContent). The module 4.5 KB
  gzipped. K-5 quantified: at most 17 or 27 doubled markers for the seconds one of an AODN/AusWaves pair lags; none seen.

## Could not check
A real `fork()` (Windows Python has no `register_at_fork`; the hooks were exercised directly); the test service's actual
preload mechanism (not in the repo; the freeze signature is consistent with fork-after-import and no other mechanism was
found); Render's grace for a restarted instance (the docs state only the deploy exception) — made moot by the A-2 fix; a
real restart or deploy of the test service during the review; `LIVE_BREAK_PROVIDERS` on Render; `/healthz` on production
(old code); a CMEMS refresh (3 h TTL); the browser's revalidation of a > 900 s cached answer; a true cold first visit on a
phone; real screen-reader behaviour; the memory trend beyond two cycles.

## The fix round (step 7)
Server: A-1 (503 + Retry-After on a cold provider's `/latest` and `/components`; the providers' cold path under the
service queues and returns empty instead of waiting), A-2 (`/healthz` 200 after the first pass; 503 before it, when
stalled, when builds keep failing, and during a <= 14 s deploy grace while incomplete; README + plan), A-3 (drain the
runner in the concurrent test; re-run the mutation scripts), A-4 (notify outside the lock), A-5 (env guards), A-6
(`build_failing`), A-7 (one scheduler thread), A-8 (providers lock), A-9 (`age_s`), A-10 (docstrings), A-11 (the pins),
B-7 (`waiting`). Client (UI 1.17.1): the page's live window retries a 503 "still loading" answer quietly (A-1), B-1 (the
note after 300 ms), B-2 ("loading…" until something is drawn), B-3 (slow polling after the schedule, the note kept), B-4
(`aria-hidden`), B-5 (growing deadline), B-6 (pins + the loader harness). Then: the page golden in its own commit, the
test site, a short fresh re-check at MAX (the two P1s, the concurrent test's stability, the client changes), the test
service's memory graph after a day (B-9), and the owner's go-ahead for production (tag `prod-pre-livebg` @ ce87c96; the
owner sets Render's Health Check Path `/healthz` on production right after the merge).

Recorded for later, not in this round: the pre-existing per-station fallback fetch of MI-IE's `latest()` on a request
thread (3 x 40 s when its ERDDAP is down) — a detail request's own, shorter timeout without retries; tz-tagging of the
merged list without waiting for the TimezoneFinder load (first served markers ~5 s instead of ~34 s after a start on a
CPU-starved instance; K-2); a content-based version for providers (no rebuild for identical lists).

## Re-check of the fix round (2026-10-04)
Two fresh reviewers at MAX on `feat/live-bg` @ 4613030 (db3f91b + golden) and the test site @ f290b78: R1 the server
(no browser), R2 the client and the visitor's view. **0 P0, 0 P1, 4 P2, 10 P3** (R1 0/0/3/6, R2 0/0/1/4). Every G25
item was confirmed FIXED with evidence, except A-11 (one of its eight pins missing: the time-zone finder built twice,
RA17; pinned since) and B-9 (memory, open).

What the reviewers confirmed:
- R1: every click on a cold provider answers the 503 contract in at most 1.9 ms server-side on the real feeds (294
  answers); after a start `/healthz` is never at 503 for more than 10 s, also on a dead-MI-IE day; the concurrent test is
  stable (64 of 64 runs, also under CPU load); about 43,000 randomised list requests, 8,741 `/latest` and 5,649 `/healthz`
  under stop/start chaos found no deadlock, no request over 120 ms and no inconsistent body; the live-stations golden is
  byte-identical and the inline path unchanged.
- R2: the window's quiet retry on the real page against local servers (503 in 10 ms, `Retry-After` honoured, the render
  without an error state, a switch to another buoy stops the loop, late answers ignored); B-1 to B-6 frame by frame, in
  Node with fake timers and with 57 + 15 mutants; the test site over 21 minutes: 420 `/healthz` answers all 200 and 420
  list answers all complete (562 markers).

| # | P | Finding | Outcome |
|---|---|---|---|
| R1 N-1 | P2 | `build_failing` answered 503, so one odd feed record (a CEFAS platform with a numeric id and no description fails every merged build) would make Render restart the whole site in a loop while the feed carried it. | FIXED @ 42a7476: reported in the body, never a 503; a station name is always text. |
| R1 N-2 | P2 | MI-IE's per-station fallback held a request thread for about 2 minutes (3 x 40 s on its retrying session) right after its list fetch failed. | FIXED @ cc3b1ea (the owner asked for it before production): no direct request while the list's last refresh failed; otherwise one attempt with 3 s / 8 s timeouts and no retries. |
| R1 N-3 | P2 | `LIVE_TICK_SEC` accepted up to 600 s while the stall limit was 180 s: a quiet site would read "stalled" for most of every cycle and Render would restart it. | FIXED @ 42a7476: the tick is clamped to 1-120 s and the stall limit is max(180 s, 3 ticks). |
| R2 N-1 | P2 | A unit change while the live window waited showed the previous buoy's numbers under this buoy's name (the root cause is also in production's code; the quiet retry made the wait long). | FIXED @ 75fa516: opening a window forgets what the previous buoy rendered; an error hides the generic panel. |
| R1 N-4 | P3 | The fork reset missed the providers lock and the pass counters. | FIXED @ 42a7476. |
| R1 N-5 | P3 | Docs: this record's `/components` claim; README wording ("no request ever waits"), the section title's version, a misplaced sentence, the `/healthz` docstring. | FIXED (README @ 75fa516 and 42a7476; the record is corrected below). |
| R1 N-6 | P3 | The deploy watch after the fix round did not show a zero-downtime switch: two 10 s timeouts (22:04:40 and 22:04:50 UTC), then the new instance answered at uptime 17.8 s. The free service had most likely idled, so there was no old instance to keep. | ACCEPTED as a correction of the author's claim; the next test-site deploy is watched with the instance kept awake first. |
| R1 N-7 | P3 | `/healthz` stayed 200 while every refresh worker hung on a slow-drip feed (timeouts are per read, not overall). | FIXED @ 42a7476: `fetch_stuck` (every worker held by one fetch for 15 minutes) answers 503; providers report `in_flight_s`. |
| R1 N-8 | P3 | Test hygiene and mutation gaps: idle worker threads left by a test; the non-blocking switch cleared outside the service lock; no pins for the tick's lower bound, the default grace, the build-failure count, a first failure's age, the runner's `waiting` or the finder's singleton. | FIXED @ 42a7476: `ThreadRunner.shutdown()`, the switch under the lock, a pin for each. |
| R1 N-9 | P3 | A NaN position from any feed made the merged list invalid JSON for every visitor (pre-existing). | FIXED @ 42a7476: positions that are not finite numbers are dropped. |
| R2 N-2 | P3 | The window's retry loop was weakly pinned (no cap, the `Retry-After` clamp and default, the stale check after the fetch). | FIXED @ 75fa516: harness pins; 12 of 12 window mutants killed. |
| R2 N-3 | P3 | An MI-IE window could spin about 4 minutes and end on a Cloudflare 524 HTML page shown as a JSON parse error; the give-up text read as still trying. | FIXED @ cc3b1ea + 75fa516: the short fallback; a non-JSON answer is an ordinary failure; the wait is explained under the spinner; the give-up says the buoy's data is not available yet. |
| R2 N-4 | P3 | The growing deadline still drops an answer slower than 30 s. | ACCEPTED: not reachable on the normal path (0.1-0.3 s). |
| R2 N-5 | P3 | A README sentence sat on the wrong bullet. | FIXED @ 75fa516. |

Correction to the record above: the A-1 outcome and "The fix round" say `/latest` AND `/components` answer 503 for a
cold provider. Only `/latest` does, and that is right: `/components` never needs a provider's list (the spectra code is a
static table).

Mutation after the fixes: the re-check fixes 20 of 20 mutants killed (tick clamp, stall limit, the `/healthz` rule, the
fork reset, finite positions, names as text, the in-flight time, the runner's shutdown; one first survived and got a
pin: a stopped runner starts workers again for new jobs); the finder's singleton 2 of 2; the Irish fallback 6 of 6; the
window 12 of 12. Suites at 42a7476: pytest 824 passed, Node 409 passed.

Still open before production: B-9 (the test service's memory over a day, on Render's graph) and a restart of the test
service watched from outside under the new `/healthz` rule (N-6). Accepted for the owner's awareness: on a CPU-starved
instance the first served markers wait for the time-zone finder's load (about 29-34 s on the free test instance, K-2);
the page shows the remembered list meanwhile.
