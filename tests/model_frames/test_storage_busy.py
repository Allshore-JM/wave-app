"""publish.is_storage_busy: which storage errors are 'try again next tick' rather than a failed run that mails the owner.
model-rrfs 2026-11-26 15:30 UTC failed on R2's InternalError during a multipart UploadPart, wrapped by boto3 in an
S3UploadFailedError that has no .response; the next tick was green. No network, no boto3 needed (the CI env has none):
the wrappers below carry boto3's own class name and message shape; the real classes are checked where boto3 is here."""
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)
import publish as P  # noqa: E402

# the failure record's own text (rrfs/v1/failed/2026112612.json), bucket name redacted as the record has it
RRFS_20261126 = ("Failed to upload /tmp/rrfs-2026112612-bd2m9nqi/ak.bin to <bucket>/rrfs/v1/2026112612/ak.bin: An error "
                 "occurred (InternalError) when calling the UploadPart operation (reached max retries: 5): We encountered "
                 "an internal error. Please try again.")


class ClientError(Exception):
    def __init__(self, code, op="PutObject"):
        super().__init__(f"An error occurred ({code}) when calling the {op} operation: x")
        self.response = {"Error": {"Code": code}}


class S3UploadFailedError(Exception):
    """boto3.exceptions.S3UploadFailedError: a message only, no .response."""


def _wrapped(code, chained=True):
    """What boto3's upload_file raises: S3UploadFailedError raised while handling the ClientError."""
    try:
        try:
            raise ClientError(code, "UploadPart")
        except ClientError as e:
            msg = f"Failed to upload /tmp/x.bin to b/k.bin: {e}"
            if chained:
                raise S3UploadFailedError(msg)
            raise S3UploadFailedError(msg) from None
    except S3UploadFailedError as w:
        return w


def test_the_known_busy_codes_and_r2s_internal_error():
    for code in ("ServiceUnavailable", "SlowDown", "TooManyRequests", "RequestLimitExceeded", "InternalError"):
        assert P.is_storage_busy(ClientError(code)), code
    for code in ("AccessDenied", "NoSuchBucket", "InvalidAccessKeyId", "EntityTooLarge"):
        assert not P.is_storage_busy(ClientError(code)), code
    assert not P.is_storage_busy(RuntimeError("x")) and not P.is_storage_busy(ValueError("InternalError"))


def test_boto3s_upload_wrapper_is_read_through():
    assert P.is_storage_busy(_wrapped("InternalError"))
    assert P.is_storage_busy(_wrapped("ServiceUnavailable"))
    assert not P.is_storage_busy(_wrapped("AccessDenied"))


def test_the_code_comes_from_the_chain_when_the_message_lacks_it():
    try:
        try:
            raise ClientError("InternalError", "UploadPart")
        except ClientError:
            raise S3UploadFailedError("Failed to upload x")              # implicit chain: __context__
    except S3UploadFailedError as w:
        assert P.is_storage_busy(w)
    assert P.is_storage_busy(_cause(ClientError("SlowDown")))           # explicit: raise ... from
    assert not P.is_storage_busy(_cause(ClientError("AccessDenied")))


def _cause(err):
    try:
        raise S3UploadFailedError("Failed to upload x") from err
    except S3UploadFailedError as w:
        return w


def test_only_boto3s_phrase_names_the_code():
    assert not P.is_storage_busy(S3UploadFailedError("Failed to upload /tmp/(InternalError)/x.bin to b: disk full"))


def test_the_wrapper_without_a_chain_is_read_from_its_message():
    assert P.is_storage_busy(_wrapped("InternalError", chained=False))
    assert not P.is_storage_busy(_wrapped("AccessDenied", chained=False))
    assert P.is_storage_busy(S3UploadFailedError(RRFS_20261126)), "the 2026-11-26 failure"
    assert not P.is_storage_busy(S3UploadFailedError("Failed to upload x: disk full"))


def test_only_the_wrapper_is_unwrapped():
    """A real failure raised while handling a busy error stays a failed run."""
    try:
        try:
            raise ClientError("InternalError")
        except ClientError:
            raise KeyError("a bug in the handler")
    except KeyError as e:
        assert e.__context__ is not None and not P.is_storage_busy(e)
    assert not P.is_storage_busy(RuntimeError(RRFS_20261126)), "the message alone is read only off boto3's wrapper"


def test_the_rrfs_tick_tries_again_without_a_failure_record(monkeypatch):
    from test_rrfs import COORDS, NOW, J, _fresh_pointer, store
    st = store()
    _fresh_pointer(st, [{"cycle": "2026111712", "kind": "run", "stations": "x"},
                        {"cycle": "2026111712", "kind": "cycle", "stations": "x"}])
    head = lambda url: "t13z" in url                                        # noqa: E731
    calls = []

    def blip(*a, **k):
        calls.append(1)
        raise S3UploadFailedError(RRFS_20261126)
    monkeypatch.setattr(J, "build", blip)
    assert J.main([], head=head, store=st, now=NOW, coords=COORDS, log=lambda s: None) == 3
    assert st.get_json("rrfs/v1/failed/2026111713.json") is None
    assert J.main([], head=head, store=st, now=NOW, coords=COORDS, log=lambda s: None) == 3 and len(calls) == 2


def test_with_real_boto3_classes():
    exc_mod = pytest.importorskip("boto3.exceptions")
    ce_mod = pytest.importorskip("botocore.exceptions")
    ce = ce_mod.ClientError({"Error": {"Code": "InternalError", "Message": "Please try again."}}, "UploadPart")
    assert P.is_storage_busy(ce)
    try:
        try:
            raise ce
        except ce_mod.ClientError as e:
            raise exc_mod.S3UploadFailedError(f"Failed to upload a to b: {e}")
    except exc_mod.S3UploadFailedError as w:
        assert P.is_storage_busy(w)
    assert P.is_storage_busy(exc_mod.S3UploadFailedError(RRFS_20261126))
