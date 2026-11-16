"""The bathymetry build workflow (plan section 93b): tests on every push, a build only on a manual dispatch, the upload only
when asked, secrets only in the upload step, the inputs checked before anything runs, certifi's trust list everywhere."""
import os
import re
import shutil
import subprocess

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
WF = os.path.join(ROOT, ".github", "workflows", "bathy-build.yml")


def _text():
    return open(WF, encoding="utf-8").read()


def _steps(text):
    """{step name: its text} for the build job's named steps."""
    build = text.split("\n  build:\n", 1)[1]
    parts = re.split(r"\n      - name: ", build)
    return {p.split("\n", 1)[0].strip(): p for p in parts[1:]}


def test_tests_on_push_build_only_on_dispatch():
    text = _text()
    assert re.search(r"on:\n  push:\n    branches: \[\"\*\*\"\]", text)
    assert '"tools/bathy/**"' in text and '"tests/bathy/**"' in text and '".github/workflows/bathy-build.yml"' in text
    assert "python -m pytest -q tests/bathy" in text
    build = text.split("\n  build:\n", 1)[1]
    assert "if: github.event_name == 'workflow_dispatch'" in build and "needs: [tests, plan]" in build
    assert "'bathy-build'" in text and "cancel-in-progress: false" in text
    assert "permissions:\n  contents: read" in text


def test_the_inputs_are_checked_and_the_upload_is_asked_for():
    steps = _steps(_text())
    check = steps["Check the inputs"]
    assert "hawaii|marianas|samoa|prvi|gebco|hawaii-lidar|guam-lidar|pr-lidar|nwhi-lidar|pacific-sdb|us-me-nh|us-sne-ny|us-nj-ches|us-va-nc|us-sc-ga|us-fl-east|us-fl-south|us-gulf-east|us-gulf-west|us-ca-south|us-ca-north|us-or-wa|lake-superior|lake-superior-dem|lake-michigan|lake-michigan-dem|lake-huron|lake-huron-dem|lake-stclair|lake-stclair-dem|lake-erie|lake-erie-dem|lake-ontario|lake-ontario-dem)" in check
    assert '[[ "$VERSION" =~ ^[1-9][0-9]*$ ]]' in check
    up = steps["Upload to R2"]
    assert "if: github.event.inputs.upload == 'true'" in up
    assert '[ "$REPLACE" = "true" ] && CMD+=(--replace)' in up
    # the regions the workflow accepts are the builder's regions
    import sys
    sys.path.insert(0, os.path.join(ROOT, "tools", "bathy"))
    import bathy_sources as S
    assert set(re.search(r"case \"\$REGION\" in ([a-z|-]+)\)", check).group(1).split("|")) == set(S.REGIONS)


def test_upload_and_replace_are_tick_boxes_and_only_builds_wait_for_each_other():
    """G61a: the two switches are booleans (a typed "True" can never mean yes or no by accident). Section 98: dispatched
    runs share one concurrency group (one writer of regions.json at a time); every push run gets its own group, so the
    tests never queue behind an hour-long build; inside a run the regions build one at a time and keep going on a failure."""
    text = _text()
    for name in ("upload", "replace"):
        block = re.search(r"\n      %s:\n((?:        [^\n]*\n)+)" % name, text).group(1)
        assert "type: boolean" in block and "default: false" in block, name
    head, build = text.split("\n  build:\n", 1)
    group = re.search(r"\nconcurrency:[^\n]*\n  group: ([^\n]+)\n  cancel-in-progress: false\n", head).group(1)
    assert group == ("${{ github.event_name == 'workflow_dispatch' && 'bathy-build' || "
                     "format('bathy-tests-{0}', github.run_id) }}")
    assert "concurrency:" not in build                  # a job-level group would cancel the waiting matrix jobs
    assert "max-parallel: 1" in build and "fail-fast: false" in build
    assert "region: ${{ fromJSON(needs.plan.outputs.regions) }}" in build


def _version_check():
    line = [l.strip() for l in _steps(_text())["Check the inputs"].splitlines() if "VERSION" in l and "=~" in l]
    assert len(line) == 1
    return line[0]


@pytest.mark.parametrize("version, ok", [("1", True), ("3", True), ("12", True), ("0", False), ("01", False),
                                         ("1a", False), ("", False), ("-1", False), ("1 2", False), ("1.0", False)])
def test_the_version_check_runs_as_written(version, ok):
    bash = shutil.which("bash")
    if not bash:
        pytest.skip("no bash here")
    rc = subprocess.run([bash, "-c", _version_check()], env=dict(os.environ, VERSION=version),
                        capture_output=True).returncode
    assert (rc == 0) == ok, (version, rc)


def test_secrets_only_in_the_upload_step():
    text = _text()
    steps = _steps(text)
    for name, body in steps.items():
        if name != "Upload to R2":
            assert "secrets." not in body, name
    assert text.count("secrets.") == 4 and steps["Upload to R2"].count("secrets.R2_") == 4
    assert "secrets." not in text.split("\n  build:\n", 1)[0]          # never in the tests job


def test_the_download_uses_certifis_list_and_every_action_is_pinned():
    text = _text()
    curls = [l for l in (m.split(" #", 1)[0] for m in re.findall(r"[^\n]*\bcurl\b[^\n]*", text))
             if re.search(r"\bcurl\b", l) and not l.strip().startswith("#")]
    assert len(curls) == 1 and '--cacert "$CA" --capath "$NOCAPATH"' in curls[0]
    assert "if: matrix.region == 'gebco'" in _steps(text)["Download GEBCO 2026 (region gebco)"]
    for pin in re.findall(r"pip install ([^\n]+)", text):
        assert "certifi==2026.10.23" in pin.split()
    pips = re.findall(r"pip install ([^\n]+)", text)
    assert len(pips) == 2 and all("pyproj==3.8.0" in p.split() for p in pips)   # the DEP grid's latitudes (bathy_dep)
    for use in re.findall(r"uses: ([^\s]+)", text):
        assert re.fullmatch(r"[\w./-]+@[0-9a-f]{40}", use), use


def test_the_build_writes_its_numbers_to_the_summary_and_keeps_the_exit_code():
    build = _steps(_text())["Build and check"]
    assert "/usr/bin/time -v" in build and 'exit "$rc"' in build and "|| rc=$?" in build
    assert '--gebco gebco_2026_geotiff.zip' in build


def test_the_temporary_dry_run_workflow_is_gone():
    """Step 2's runner measurement (plan section 93b) was temporary: it must not stay on the default branch."""
    assert not os.path.exists(os.path.join(ROOT, ".github", "workflows", "bathy-dryrun.yml"))
    # and section 96's runner measurement of the four lidar regions
    assert not os.path.exists(os.path.join(ROOT, ".github", "workflows", "lidar-dryrun.yml"))
    # and section 97's runner measurement of the Pacific satellite depths
    assert not os.path.exists(os.path.join(ROOT, ".github", "workflows", "sdb-dryrun.yml"))
    # and section 98's runner measurement of the mainland and Great Lakes regions
    assert not os.path.exists(os.path.join(ROOT, ".github", "workflows", "mainland-dryrun.yml"))


def _list_step():
    plan = _text().split("\n  plan:\n", 1)[1].split("\n  build:\n", 1)[0]
    return plan.split("        run: |\n", 1)[1]


def test_the_region_list_names_the_builders_regions():
    import sys
    sys.path.insert(0, os.path.join(ROOT, "tools", "bathy"))
    import bathy_sources as S
    step = _list_step()
    assert set(re.search(r"case \"\$REGION\" in ([a-z|-]+)\)", step).group(1).split("|")) == set(S.REGIONS)
    assert "${{ matrix.region }}" in _text() and "github.event.inputs.region }}" in _text().split("\n  build:\n")[0]


@pytest.mark.parametrize("given, out", [
    ("us-me-nh", '["us-me-nh"]'),
    ("us-me-nh,us-sne-ny", '["us-me-nh","us-sne-ny"]'),
    ("us-me-nh, us-sne-ny  lake-erie", '["us-me-nh","us-sne-ny","lake-erie"]'),
    ("lake-erie-dem,lake-erie", '["lake-erie-dem","lake-erie"]'),
    ("us-me-nh,us-me-nh", None), ("us-me-nh,nowhere", None), ("", None), (" , ", None), ("*", None),
    ("hawaii;rm", None)])
def test_the_region_list_runs_as_written(tmp_path, given, out):
    bash = shutil.which("bash")
    if not bash:
        pytest.skip("no bash here")
    script = "\n".join(l[10:] for l in _list_step().splitlines())
    res = subprocess.run([bash, "-c", script], cwd=tmp_path, capture_output=True, text=True,
                         env=dict(os.environ, REGIONS=given, GITHUB_OUTPUT=str(tmp_path / "o"),
                                  GITHUB_STEP_SUMMARY=str(tmp_path / "s")))
    if out is None:
        assert res.returncode != 0, (given, res.stdout)
    else:
        assert res.returncode == 0, (given, res.stdout, res.stderr)
        assert (tmp_path / "o").read_text().strip() == "regions=" + out
