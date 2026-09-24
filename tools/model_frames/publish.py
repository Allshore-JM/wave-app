"""Object-storage side of the job: immutable frames, a versioned manifest, then the pointer.

Layout (PREFIX = gfswave/0p25/v1 -- the version segment changes whenever the encoding or
layout changes, so an immutable key is never rewritten with different bytes):
  <PREFIX>/<RUN>/<field>/f<HHH>.png                 full 1440x721      (immutable)
  <PREFIX>/<RUN>/half/<field>/f<HHH>.png            361x720 subsample  (immutable)
  <PREFIX>/<RUN>/manifest-<published>.json          one per publish    (immutable)
  <PREFIX>/<RUN>/partial-<published>.json           partial builds; NEVER pointed to
  <PREFIX>/latest.json  -> {run, manifest, complete: true, ...}  (max-age=300, written LAST)
  <PREFIX>/failed/<RUN>.json                        attempt log for a cycle that failed to build
RUN = YYYYMMDDHH (UTC cycle). The pointer only ever names a COMPLETE run.
"""
import json
import re
import time

PREFIX = "gfswave/0p25/v1"
IMMUTABLE = "public, max-age=31536000, immutable"
POINTER = "public, max-age=300"
LATEST_KEY = f"{PREFIX}/latest.json"


def run_key(run_dt):
    return run_dt.strftime("%Y%m%d%H")


def frame_key(run, field, step, half=False):
    return f"{PREFIX}/{run}/{'half/' if half else ''}{field}/f{step:03d}.png"


def manifest_key(run, published_utc, complete=True):
    stamp = published_utc.replace("-", "").replace(":", "")
    return f"{PREFIX}/{run}/{'manifest' if complete else 'partial'}-{stamp}.json"


def stats_key(run, published_utc):
    stamp = published_utc.replace("-", "").replace(":", "")
    return f"{PREFIX}/{run}/stats-{stamp}.json"


def files_template(run):
    """Client builds frame URLs from this: {res} is "" (full) or "half/", {field}, {step:03d}."""
    return f"{PREFIX}/{run}/{{res}}{{field}}/f{{step:03d}}.png"


def failed_key(run):
    return f"{PREFIX}/failed/{run}.json"


def _is_missing(exc):
    code = getattr(exc, "response", {}).get("Error", {}).get("Code", "") if hasattr(exc, "response") else ""
    return code in ("NoSuchKey", "404", "NotFound") or exc.__class__.__name__ in ("NoSuchKey", "KeyError")


class Store:
    """Thin wrapper over an S3-compatible client (boto3 for R2; a fake in tests)."""

    def __init__(self, client, bucket):
        self.client, self.bucket = client, bucket

    def put(self, key, body, content_type, cache_control):
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body,
                               ContentType=content_type, CacheControl=cache_control)

    def get_json(self, key):
        """Parsed JSON, or None ONLY when the key does not exist. Other errors propagate."""
        try:
            r = self.client.get_object(Bucket=self.bucket, Key=key)
        except Exception as exc:                             # noqa: BLE001
            if _is_missing(exc):
                return None
            raise
        return json.loads(r["Body"].read().decode("utf-8"))

    def list_keys(self, prefix):
        keys, token = [], None
        while True:
            kw = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            r = self.client.list_objects_v2(**kw)
            keys += [o["Key"] for o in r.get("Contents", [])]
            token = r.get("NextContinuationToken") if r.get("IsTruncated") else None
            if not token:
                return keys

    def list_runs(self):
        """{run: has_manifest} for every run prefix present."""
        runs = {}
        for k in self.list_keys(PREFIX + "/"):
            rest = k[len(PREFIX) + 1:]
            name = rest.split("/", 1)[0]
            if name.isdigit() and len(name) == 10:
                runs.setdefault(name, False)
                if rest.startswith(f"{name}/manifest-"):
                    runs[name] = True
        return runs

    def delete_keys(self, keys, what="keys"):
        for i in range(0, len(keys), 1000):                 # DeleteObjects hard limit
            batch = [{"Key": k} for k in keys[i:i + 1000]]
            r = self.client.delete_objects(Bucket=self.bucket, Delete={"Objects": batch, "Quiet": True})
            if r.get("Errors"):
                raise RuntimeError(f"delete errors under {what}: {r['Errors'][:3]}")
        return len(keys)

    def delete_prefix(self, prefix):
        return self.delete_keys(self.list_keys(prefix), prefix)


def publish_frame(store, run, field, step, encoded):
    store.put(frame_key(run, field, step), encoded["full"], "image/png", IMMUTABLE)
    store.put(frame_key(run, field, step, half=True), encoded["half"], "image/png", IMMUTABLE)


def publish_manifest(store, run, manifest, stats=None):
    """Write the (slim) manifest under a fresh immutable key, its stats sidecar first; flip the
    pointer ONLY for a complete run. The manifest must stay small (r2.dev serves it uncompressed):
    per-frame byte sizes and statistics live in the sidecar."""
    if stats is not None:
        skey = stats_key(run, manifest["published_utc"])
        store.put(skey, json.dumps(stats, separators=(",", ":"), sort_keys=True, allow_nan=False).encode(),
                  "application/json", IMMUTABLE)
        manifest = dict(manifest, stats=skey)
    body = json.dumps(manifest, separators=(",", ":"), sort_keys=True, allow_nan=False).encode()
    mkey = manifest_key(run, manifest["published_utc"], complete=manifest["complete"])
    store.put(mkey, body, "application/json", IMMUTABLE)
    if not manifest["complete"]:
        return mkey, False
    pointer = {"run": run, "manifest": mkey, "complete": True, "encoding": manifest["encoding"],
               "published_utc": manifest["published_utc"], "frames": len(manifest["frames"])}
    store.put(LATEST_KEY, json.dumps(pointer, separators=(",", ":"), sort_keys=True).encode(),
              "application/json", POINTER)
    return mkey, True


def prune(store, keep=4):
    """Keep the newest `keep` COMPLETE runs (plus whatever latest.json names); delete every other
    run prefix, including manifest-less leftovers of crashed builds that are older than the
    newest complete run. Returns the runs actually deleted."""
    if keep < 1:
        raise ValueError("keep must be >= 1")
    latest = store.get_json(LATEST_KEY) or {}
    runs = store.list_runs()
    complete = sorted(r for r, ok in runs.items() if ok)
    protect = set(complete[-keep:]) | ({latest["run"]} if latest.get("run") else set())
    newest_complete = complete[-1] if complete else None
    deleted = []
    for run in sorted(runs):
        if run in protect:
            continue
        if not runs[run] and newest_complete and run >= newest_complete:
            continue                                    # a build that may still be in progress
        store.delete_prefix(f"{PREFIX}/{run}/")
        deleted.append(run)
    return deleted


LEGACY_KEYS = ("gfswave/0p25/latest.json",)            # the pre-v1 public pointer
LEGACY_PREFIXES = ("gfswave/0p25/2026092212/",)        # the one run published under the pre-v1 layout


def prune_legacy(store):
    """One-time cleanup of the pre-v1 layout, restricted to the objects that were actually left
    there (a stale pointer and one run of frames). Nothing else outside the versioned prefix is
    ever touched -- a future gfswave/0p25/v2/ in particular. A no-op once they are gone."""
    keys = []
    for prefix in LEGACY_PREFIXES:
        keys += store.list_keys(prefix)
    for key in LEGACY_KEYS:
        keys += [k for k in store.list_keys(key) if k == key]
    return store.delete_keys(keys, "legacy") if keys else 0


_ENDPOINT_RE = re.compile(r"""https?://[^ \t"'<>]*[.]r2[.]cloudflarestorage[.]com[^ \t"'<>]*""")
_HEX32_RE = re.compile(r"(?<![0-9a-fA-F])[0-9a-f]{32}(?![0-9a-fA-F])")


def redact(message, bucket=None, limit=500):
    """Error text destined for the PUBLIC bucket: botocore messages can embed the R2 endpoint URL
    (which carries the account id), the bucket name and the key; strip the URL, any bare 32-hex
    token (account id, access key id) and the bucket name, then cap the length."""
    s = _ENDPOINT_RE.sub("<r2-endpoint>", str(message))
    s = _HEX32_RE.sub("<redacted>", s)
    if bucket:
        s = s.replace(bucket, "<bucket>")
    return s[:limit]


def notready_key(run):
    return f"{PREFIX}/notready/{run}.json"


def record_notready(store, run, message):
    """Count NotReady exits for a run the listing called complete (an object that is listed but
    never served, or that vanished); the workflow summary warns once the count is high."""
    prev = store.get_json(notready_key(run)) or {"run": run, "count": 0}
    prev["count"] += 1
    prev["last_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    prev["last_error"] = redact(message, store.bucket)
    store.put(notready_key(run), json.dumps(prev, sort_keys=True).encode(), "application/json", POINTER)
    return prev


def record_failure(store, run, message):
    prev = store.get_json(failed_key(run)) or {"run": run, "attempts": 0}
    prev["attempts"] += 1
    prev["last_attempt_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    prev["last_error"] = redact(message, store.bucket)
    store.put(failed_key(run), json.dumps(prev, sort_keys=True).encode(), "application/json", POINTER)
    return prev


def r2_client(account_id, access_key, secret_key):
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3", endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name="auto",
        config=Config(retries={"max_attempts": 5, "mode": "standard"},
                      request_checksum_calculation="when_required",
                      response_checksum_validation="when_required"))
