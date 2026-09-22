"""Object-storage side of the job: immutable frames, manifest, then the atomic `latest.json`.

Layout (PREFIX = gfswave/0p25):
  <PREFIX>/<RUN>/<field>/f<HHH>.png          full 1440x721   (immutable)
  <PREFIX>/<RUN>/half/<field>/f<HHH>.png     720x361         (immutable)
  <PREFIX>/<RUN>/manifest.json                                (immutable)
  <PREFIX>/latest.json  -> {"run": "<RUN>", "manifest": "<key>"}   (max-age=300, written LAST)
RUN = YYYYMMDDHH (UTC cycle). A run only becomes visible when every object exists.
"""
import json

PREFIX = "gfswave/0p25"
IMMUTABLE = "public, max-age=31536000, immutable"
POINTER = "public, max-age=300"


def run_key(run_dt):
    return run_dt.strftime("%Y%m%d%H")


def frame_key(run, field, step, half=False):
    return f"{PREFIX}/{run}/{'half/' if half else ''}{field}/f{step:03d}.png"


def manifest_key(run):
    return f"{PREFIX}/{run}/manifest.json"


LATEST_KEY = f"{PREFIX}/latest.json"


class Store:
    """Thin wrapper over an S3-compatible client (boto3 for R2; a fake in tests)."""

    def __init__(self, client, bucket):
        self.client, self.bucket = client, bucket

    def put(self, key, body, content_type, cache_control):
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body,
                               ContentType=content_type, CacheControl=cache_control)

    def get_json(self, key):
        try:
            r = self.client.get_object(Bucket=self.bucket, Key=key)
        except Exception:                                    # noqa: BLE001 - missing/any
            return None
        return json.loads(r["Body"].read().decode("utf-8"))

    def list_runs(self):
        runs = set()
        token = None
        while True:
            kw = {"Bucket": self.bucket, "Prefix": PREFIX + "/", "Delimiter": "/"}
            if token:
                kw["ContinuationToken"] = token
            r = self.client.list_objects_v2(**kw)
            for cp in r.get("CommonPrefixes", []):
                name = cp["Prefix"][len(PREFIX) + 1:].strip("/")
                if name.isdigit() and len(name) == 10:
                    runs.add(name)
            token = r.get("NextContinuationToken")
            if not token:
                break
        return sorted(runs)

    def delete_prefix(self, prefix):
        token = None
        while True:
            kw = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            r = self.client.list_objects_v2(**kw)
            keys = [{"Key": o["Key"]} for o in r.get("Contents", [])]
            if keys:
                self.client.delete_objects(Bucket=self.bucket, Delete={"Objects": keys})
            token = r.get("NextContinuationToken")
            if not token:
                break


def publish_frame(store, run, field, step, encoded):
    store.put(frame_key(run, field, step), encoded["full"], "image/png", IMMUTABLE)
    store.put(frame_key(run, field, step, half=True), encoded["half"], "image/png", IMMUTABLE)


def publish_manifest_then_pointer(store, run, manifest):
    """Order matters: every frame is already stored; write the manifest, THEN flip the pointer."""
    mkey = manifest_key(run)
    store.put(mkey, json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode(),
              "application/json", IMMUTABLE)
    pointer = {"run": run, "manifest": mkey, "published_utc": manifest["published_utc"]}
    store.put(LATEST_KEY, json.dumps(pointer, separators=(",", ":"), sort_keys=True).encode(),
              "application/json", POINTER)


def prune(store, keep=4):
    """Delete whole run prefixes older than the newest `keep`, never the run `latest.json` names."""
    latest = store.get_json(LATEST_KEY) or {}
    runs = store.list_runs()
    for old in runs[:-keep] if len(runs) > keep else []:
        if old == latest.get("run"):
            continue
        store.delete_prefix(f"{PREFIX}/{old}/")
    return runs[:-keep] if len(runs) > keep else []


def r2_client(account_id, access_key, secret_key):
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3", endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name="auto",
        config=Config(retries={"max_attempts": 5, "mode": "standard"}))
