"""Two causes of "Run failed" mail that were not data problems (checked 2026-11-05):

1. the micromamba set-up step: without a fixed address the action asks GitHub's API for the newest release and checks
   the file with ONE request that is never retried (8 failed runs in ten days); every publisher now names the address;
2. the prune after a publish: model-points published its run on 2026-11-03 and then exited 1 because the bucket timed
   out while deleting the old run (a 1,000-key DeleteObjects, five 60-s read time-outs). A prune after a publish is a
   warning now, and deletes go in smaller batches.
Offline.
"""
import glob
import inspect
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
sys.path.insert(0, HERE)
import publish as P  # noqa: E402

WF = os.path.join(ROOT, ".github", "workflows")
URL_RE = re.compile(r"^https://github\.com/mamba-org/micromamba-releases/releases/download/"
                    r"(\d+\.\d+\.\d+-\d+)/micromamba-linux-64$")


def _workflows():
    out = {}
    for path in sorted(glob.glob(os.path.join(WF, "*.yml"))):
        text = open(path, encoding="utf-8").read()
        if "setup-micromamba" in text:
            out[os.path.basename(path)] = text
    return out


def test_every_micromamba_step_names_one_fixed_download_address():
    wfs = _workflows()
    assert len(wfs) >= 11 and "model-frames.yml" in wfs and "model-points.yml" in wfs
    urls = {}
    for name, text in wfs.items():
        assert text.count("mamba-org/setup-micromamba@") == 1, name
        found = re.findall(r"^ +micromamba-url: (\S+)$", text, flags=re.M)
        assert len(found) == 1, f"{name}: one micromamba-url line"
        assert URL_RE.match(found[0]), f"{name}: a versioned release file, not 'latest'"
        urls[name] = found[0]
        step = text.split("mamba-org/setup-micromamba@", 1)[1].split("\n      - ", 1)[0]
        assert "micromamba-url:" in step, f"{name}: the address belongs to the set-up step"
        # the action refuses an address together with either of these, and 'latest' is what failed
        assert "micromamba-version" not in text and "download-micromamba" not in text, name
    assert len(set(urls.values())) == 1, f"one micromamba for every publisher (one cached environment): {urls}"


def test_the_temporary_micromamba_dry_run_workflow_is_gone():
    assert not os.path.exists(os.path.join(WF, "mm-dryrun.yml"))


def test_safe_prune_passes_the_result_through():
    said = []
    assert P.safe_prune(lambda: ["2026110100"], "bucket", said.append) == ["2026110100"]
    assert P.safe_prune(lambda: [], "bucket", said.append) == [] and said == []


def test_safe_prune_turns_a_failure_into_one_redacted_warning(capsys):
    said = []

    def boom():
        raise RuntimeError('Read timeout on endpoint URL: "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com/'
                           'secret-bucket?delete"')

    assert P.safe_prune(boom, "secret-bucket", said.append) is None
    out = capsys.readouterr().out
    assert len(said) == 1 and said[0] in out
    line = said[0]
    assert line.startswith("WARNING: prune failed (RuntimeError: ") and "the next run prunes again" in line
    assert "0123456789abcdef" not in line and "secret-bucket" not in line and "cloudflarestorage" not in line
    assert P.safe_prune(boom, "secret-bucket") is None                 # no summary writer: printed only


def test_safe_prune_does_not_hide_a_keyboard_interrupt():
    def stop():
        raise KeyboardInterrupt()

    try:
        P.safe_prune(stop)
    except KeyboardInterrupt:
        return
    raise AssertionError("a cancelled run must stay cancelled")


class _Client:
    def __init__(self, fail_deletes=False):
        self.objects, self.batches, self.fail_deletes = {}, [], fail_deletes

    def put_object(self, Bucket, Key, Body, ContentType, CacheControl):
        self.objects[Key] = {"body": Body, "ct": ContentType, "cc": CacheControl}

    def get_object(self, Bucket, Key):
        import io
        if Key not in self.objects:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.objects[Key]["body"])}

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None, **kw):
        return {"Contents": [{"Key": k} for k in sorted(self.objects) if k.startswith(Prefix)], "IsTruncated": False}

    def delete_objects(self, Bucket, Delete):
        if self.fail_deletes:
            raise TimeoutError('Read timeout on endpoint URL: "https://x.r2.cloudflarestorage.com/bucket?delete"')
        self.batches.append(len(Delete["Objects"]))
        for o in Delete["Objects"]:
            self.objects.pop(o["Key"], None)
        return {}


def test_deletes_go_in_batches_well_under_the_api_limit():
    assert 1 <= P.DELETE_BATCH <= 500
    c = _Client()
    st = P.Store(c, "bucket")
    keys = [f"p/{i:05d}" for i in range(2 * P.DELETE_BATCH + 7)]
    for k in keys:
        st.put(k, b"x", "x", P.IMMUTABLE)
    assert st.delete_keys(keys, "p") == len(keys)
    assert c.batches == [P.DELETE_BATCH, P.DELETE_BATCH, 7] and not c.objects


def test_a_delete_failure_after_the_publish_leaves_the_run_live_and_the_job_green(monkeypatch, capsys):
    import gfs_winds as W
    import test_gfs_winds as T
    monkeypatch.setattr(W, "MIN_MEMBERS", 2)
    monkeypatch.setattr(W, "MIN_ARCHIVE_BYTES", 100)
    monkeypatch.setattr(W.time, "sleep", lambda s: None)
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    old = ["2026102306", "2026102312", "2026102318", "2026102400", "2026102406", "2026102412", "2026102418",
           "2026102500", "2026102506"]
    c = _Client(fail_deletes=True)
    st = P.Store(c, "bucket")
    for run in old:
        st.put(W.index_key(run), b"{}", "application/json", P.IMMUTABLE)
    st.put(P.latest_key(W.PREFIX), json.dumps({"run": old[-1]}).encode(), "application/json", P.POINTER)
    assert W.main([], opener=T.FakeNoaa({T.RUN: T.three()}), store=st, now=T.now_after(hours=1)) == 0
    out = capsys.readouterr().out
    assert "WARNING: prune failed (TimeoutError" in out and "cloudflarestorage" not in out
    assert json.loads(c.objects[P.latest_key(W.PREFIX)]["body"])["run"] == T.RUN, "the new run is live"
    assert W.index_key(old[0]) in c.objects, "nothing was deleted this time"
    c.fail_deletes = False                                             # the next run's prune clears the leftovers
    assert W.prune(st) and W.index_key(old[0]) not in c.objects and W.index_key(T.RUN) in c.objects


def test_every_prune_after_a_publish_goes_through_safe_prune():
    """The jobs' main() routines: a bare prune call there is the 2026-11-03 failure waiting to happen again."""
    import gfs_spectral
    import gfs_winds
    import nwps
    import points
    import run
    import sst
    call = re.compile(r"\b(?:P\.)?(prune|prune_office|prune_legacy)\(")
    for mod in (run, points, sst, gfs_winds, gfs_spectral, nwps):
        lines = [ln for ln in inspect.getsource(mod.main).splitlines() if call.search(ln) and not ln.lstrip().startswith("#")]
        assert lines, f"{mod.__name__}.main prunes after it publishes"
        for ln in lines:
            assert "safe_prune(" in ln, f"{mod.__name__}: {ln.strip()}"
