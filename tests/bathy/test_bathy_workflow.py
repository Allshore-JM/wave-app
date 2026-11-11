"""The bathymetry build workflow (plan section 93b): tests on every push, a build only on a manual dispatch, the upload only
when asked, secrets only in the upload step, the inputs checked before anything runs, certifi's trust list everywhere."""
import os
import re

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
    assert "if: github.event_name == 'workflow_dispatch'" in build and "needs: tests" in build
    assert "group: bathy-build" in text and "cancel-in-progress: false" in text
    assert "permissions:\n  contents: read" in text


def test_the_inputs_are_checked_and_the_upload_is_asked_for():
    steps = _steps(_text())
    check = steps["Check the inputs"]
    assert "hawaii|marianas|samoa|prvi|gebco)" in check and "*[!0-9]*" in check
    up = steps["Upload to R2"]
    assert "if: github.event.inputs.upload == 'true'" in up
    assert '[ "$REPLACE" = "true" ] && CMD+=(--replace)' in up
    # the regions the workflow accepts are the builder's regions
    import sys
    sys.path.insert(0, os.path.join(ROOT, "tools", "bathy"))
    import bathy_sources as S
    assert set(re.search(r"case \"\$REGION\" in ([a-z|]+)\)", check).group(1).split("|")) == set(S.REGIONS)


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
    assert "if: github.event.inputs.region == 'gebco'" in _steps(text)["Download GEBCO 2026 (region gebco)"]
    for pin in re.findall(r"pip install ([^\n]+)", text):
        assert "certifi==2026.10.23" in pin.split()
    for use in re.findall(r"uses: ([^\s]+)", text):
        assert re.fullmatch(r"[\w./-]+@[0-9a-f]{40}", use), use


def test_the_build_writes_its_numbers_to_the_summary_and_keeps_the_exit_code():
    build = _steps(_text())["Build and check"]
    assert "/usr/bin/time -v" in build and 'exit "$rc"' in build and "|| rc=$?" in build
    assert '--gebco gebco_2026_geotiff.zip' in build


def test_the_temporary_dry_run_workflow_is_gone():
    """Step 2's runner measurement (plan section 93b) was temporary: it must not stay on the default branch."""
    assert not os.path.exists(os.path.join(ROOT, ".github", "workflows", "bathy-dryrun.yml"))
