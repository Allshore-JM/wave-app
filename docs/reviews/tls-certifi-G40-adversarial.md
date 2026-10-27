# Section 57: the TLS trust list after eMudhra's compromise (G40)

Plan: `swirling-spinning-snowflake.md`, section 57. Branch `fix/tls-certifi` off `Live-Buoy-Update` @ a58e14a.
Render's advisory (2026-10-26): eMudhra's stolen intermediate keys (disclosed 2026-10-19); Python sites should require
certifi 2026.10.19 or later and redeploy; two test hosts, `dv-g3b-emudhra.we-have-a-problem.com` and
`dv-g3b-aaa-emudhra.we-have-a-problem.com`, must both be refused by the service's own HTTP client.

## Step 2: test-site verification (2026-10-27, Opus 5.5 at MAX)

Target: the site files of `fix/tls-certifi` (cff8f28: `requirements.txt` with `certifi>=2026.10.23`, `app.py` without
the extra CA bundle and with `/healthz` -> `tls`, `certs/` removed, `tests/test_swan_cert.py`), cherry-picked onto `test`
@ 895bee5; then an empty commit @ c556894 for a later rebuild. Scratch: `tls57/v2` (watch.py, capture.py, compare.py,
smoke.js).

### The site

- **First deploy** (test @ 895bee5, 17:17:56 UTC): 42 polls of `/robots.txt` and `/healthz` across the switch, 0 failed.
  The new instance's `/healthz` -> `tls`: `{"certifi": "2026.10.23", "bundle_is_certifi": true, "env_override": []}`.
- **Later rebuild** (test @ c556894, an empty commit, so Render built with its cache, 19:45:02 UTC): the new instance
  from 19:46:06, 44 polls 0 failed, the same `tls` on every poll of both instances. On the rebuilt instance every live
  feed (NDBC, CDIP, QLD, AODN, AusWaves, MI-IE, CEFAS, SMHI, RWS, CMEMS) and every wind feed (NDBC, METAR, CO-OPS, NWS,
  MADIS) published with no error; `/healthz` holds no error and no "ssl", "certificate" or "verify" anywhere; a tide
  window (1612340, harmonic, 1,585 points) and the SWAN table of 51201 (wind complete) answered.
- The Render build log was not read (the dashboard asks for a sign-in). `/healthz` reports the certifi the running
  process imported, which is what decides what it trusts.
- **Same answers as production**: 65 forecast answers (30 GFS compact, 6 classic, 5 Metric, 5 UTC, 12 SWAN, 3 lake,
  2 forecast points, 2 removed stations) on run 2026102712, captured 19:41-19:42 UTC from both sites: 65 identical. The
  12 SWAN tables (PacIOOS: the request that used the removed root) carry wind in every row.
- **Browser** (headless Chrome with its own profile; desktop 1280x800 and phone 375x812): 51201 GFS 385 rows, 51201 and
  51208 SWAN 175 rows, wind in every row, no error box, no sideways scroll; console 0 problems; 38 of 38 checks.
- **Test-branch suite**: 1,201 passed plus the five failures the test branch always has (its logo lines against the
  page golden x4, the numpy-import test).

### GitHub's runners (the temporary dry run, four runs)

A push-triggered workflow on the branch (no secrets, nothing published) ran `tools/tls_check.py` in the three Python
environments the jobs use, and curl the way coast-build uses it. Run 4 (b48113e) is the final state:

| Environment | Versions | Result |
|---|---|---|
| publishers (micromamba `frames` env) | python 3.11.16, openssl 3.6.5, certifi + ca-certificates 2026.10.23, botocore 1.43.118 | certifi, conda's OpenSSL default and botocore's file refuse both test hosts; real hosts verify (3 unreachable from the runner: reported as such) |
| coast-build's pip jobs | python 3.11.17, openssl 3.0.13, certifi 2026.10.23, botocore 1.43.119 (botocore uses certifi's file) | both test hosts refused, real hosts verify |
| the site's environment | python 3.11.9 + `requirements.txt`: certifi 2026.10.23, requests 2.34.2 | both refused; `_tls_health()` = `{"certifi": "2026.10.23", "bundle_is_certifi": true, "env_override": []}` |
| Node | `setup-node` "24" with `check-latest` | v24.22.1 |
| curl (Ubuntu 24.04) | `--cacert "$CA" --capath "$NOCAPATH"` | both test hosts exit 60 (refused); GitHub, raw.githubusercontent and GeoNames pass |

Findings fixed during the step (each on the branch, with pins in `tests/model_frames/test_tls_pins.py`):

- **V2-1: the runners' own list trusts a compromised chain.** On 2026-10-27 Ubuntu 24.04's `/etc/ssl/certs` still
  trusted `dv-g3b-emudhra` (curl exit 0). coast-build's curl downloads (GSHHG, Natural Earth, GeoNames) used it; the
  GeoNames file has no pinned digest. Fixed @ d44d5a5: every curl gets certifi's file.
- **V2-2: `--cacert` alone is not enough.** Ubuntu's curl keeps reading its built-in CA folder `/etc/ssl/certs` when only
  `--cacert` is given: `dv-g3b-emudhra` exit 0 again. Fixed @ b48113e: `--capath` an empty directory; run 4 shows exit 60
  for both test hosts with the fix and exit 0 for `dv-g3b-emudhra` with `--cacert` alone or the runner's default.
- **V2-3: a network failure is not a certificate result.** A runner that could not reach a real host (PacIOOS,
  CoastWatch at times) failed the whole check and skipped the steps after it. Fixed @ d4d46e7 / b48113e: a real host is
  asked twice and then reported as unreachable; a test host that cannot be reached still fails (nothing was proved); the
  curl step runs even when the Python check failed.

The temporary workflow is removed @ ef85bfb, with a test that it stays gone; model-frames-tests now also runs the pin
tests when `tools/tls_check.py` or `coast-build.yml` changes.

### Corrections to the step-1 notes

- Commit 5d78043 holds only the PEM deletion: its `git add` named the already deleted `certs` path, failed with its error
  hidden, and the commit went ahead with what was staged. Its message describes more. The real step-1 change is
  cff8f28 (checked with `git show --stat`).
- "GitHub started no run" was wrong: 5d78043 and 36fd9dd changed no workflow, so no workflow had a reason to run;
  cff8f28 ran normally.

### Notes

- Production looks for a new NOAA run only when someone asks it for a forecast (section 53: the run is cached 30
  minutes and re-checked on demand). With no visitor for 2.5 hours it stayed on the 06Z run until one request. By
  design; noted because the comparison waited on it.
- The laptop (owner's go-ahead, step 5) still has certifi 2025.04.26, which trusted `dv-g3b-emudhra` in the local check.

## G40: the review (2026-10-27, one fresh reviewer at HIGH)

Target: `a58e14a..2793eb2` and the test site. Report (scratch): `g40/g40-report.md`. **0 P0, 0 P1, 1 P2, 6 P3.**
Held up: every HTTPS client of the site is a default requests session (no urllib / ssl / boto / curl / subprocess on
the site, no `verify=False` or CA variable anywhere in the repo); `/healthz` -> `tls` cannot throw and leaks nothing;
setup-micromamba puts `create-args` into the cache key, so the pins force a rebuilt environment; every curl carries
`--cacert` and `--capath` and an empty `$CA` fails rather than falling back; certifi 2026.10.23 drops several roots
(DigiCert Global Root CA, Entrust, GTS R2 ...) yet all 31 upstream hosts checked verify under it; both test hosts are
refused with code 20. The author re-checked P2-1 (pip 25.2's `index_command.py`: truststore plus its vendored certifi),
P3-1 (the code) and P3-2 (`requests.certs.where is certifi.where` -> True).

| # | Finding | Outcome |
|---|---|---|
| P2-1 | pip (Render, runners) trusts the machine's list plus pip's own certifi copy, micromamba most likely the runner's list; both still trusted a compromised chain; no install is hash-pinned. Build-time, needs the stolen keys and a network position; existed before | OWNER (2026-10-27): document it. README "Known limitation: the installers"; hash-locked installs would be a section of their own |
| P3-1 | `tls_check` counted any verification failure as "refused": an expired (code 10) or wrong-host (62) certificate passed, and both test certificates expire on 2027-05-03 | Fixed: only codes 2, 19, 20, 21 pass; anything else is "bad certificate (code N: ...)" and fails; tests for 10 and 62; README notes that only `dv-g3b-emudhra` separates old and new certifi |
| P3-2 | `bundle_is_certifi` was always true (`requests.certs.where` is `certifi.where`) | Fixed: it compares the CA file a request really uses after requests applies the environment (`merge_environment_settings`); false when `REQUESTS_CA_BUNDLE` / `CURL_CA_BUNDLE` points elsewhere; tests |
| P3-3 | The pin tests missed 5 of 10 regressions (a session `.verify`, `verify=False` in another module, a CA variable in `start.sh`, a curl in another workflow, a later certifi downgrade) | Fixed: `tests/test_swan_cert.py` scans every site module for any way to change verification and the site's start files for a CA variable; `test_tls_pins.py` scans every workflow (each curl, no wget, every certifi named is the pin, no CA variable). The reviewer's 5 survivors and 4 mutants of the fixes: 9 of 9 killed |
| P3-4 | The README said model-frames-tests runs `tests/test_swan_cert.py`; it runs `tests/model_frames` only | Fixed wording: the site pins run with the site's suite (no workflow runs it) |
| P3-5 | `--real` lacked coast-build's download hosts and the R2 host; a non-certificate TLS failure on a real host was only a note | Fixed: github.com, release-assets.githubusercontent.com, raw.githubusercontent.com, download.geonames.org added; a TLS answer that is no verified handshake fails ("tls error"); only no answer at all is "unreachable"; R2 noted (its name holds the account id; the first publish proves it) |
| P3-6 | Only the `frames` environment was solved with the pins before the merge; a rollback restores the old cache keys and so the old trusting environments | Noted in the plan: each publisher's first run after the merge is checked (versions in its summary); a rollback brings the old trust back with the old code (expected) |

Local check of the fixed tool with certifi 2026.10.23: both test hosts refused, 26 real hosts verify (`--real`).
