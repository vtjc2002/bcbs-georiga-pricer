import gzip
import json
from types import SimpleNamespace

import pytest
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

from etl import azure_publish
from etl.source import SourceError


class Blob:
    def __init__(self):
        self.data = None
        self.metadata = {}
        self.etag = "initial"
        self.content_settings = None

    def upload_blob(self, data, overwrite=False, metadata=None, **options):
        if self.data is not None and not overwrite:
            raise ResourceExistsError("already exists")
        if options.get("etag") and options["etag"] != self.etag:
            raise SourceError("concurrent edit")
        self.data = data.read() if hasattr(data, "read") else data
        self.metadata = metadata or {}
        self.content_settings = options.get("content_settings")
        self.etag += "x"

    def get_blob_properties(self):
        if self.data is None:
            raise ResourceNotFoundError("missing")
        return SimpleNamespace(metadata=self.metadata, etag=self.etag)


class Container:
    def __init__(self):
        self.blobs = {}

    def get_blob_client(self, name):
        return self.blobs.setdefault(name, Blob())


class Lease:
    def __init__(self, _):
        self.held = False

    def acquire(self, **_):
        self.held = True

    def renew(self):
        assert self.held

    def release(self):
        self.held = False


def local_snapshot(tmp_path):
    root = tmp_path
    snapshot = "a" * 64
    folder = root / "public" / "snapshots" / snapshot
    folder.mkdir(parents=True)
    (folder / "manifest.json.gz").write_bytes(gzip.compress(
        json.dumps({"snapshot": snapshot, "sources": [{"id": "b" * 64}]}).encode(), mtime=0))
    (folder / "page.json.gz").write_bytes(gzip.compress(b'{"value":1}', mtime=0))
    (root / "public" / "active.json").write_text(json.dumps({
        "snapshot": snapshot, "manifest": f"snapshots/{snapshot}/manifest.json",
    }))
    source = root / "files" / ("b" * 64)
    source.mkdir(parents=True)
    (source / "complete.json").write_text('{"complete":true}')
    return root, snapshot


def test_publish_is_immutable_and_idempotent(tmp_path, monkeypatch):
    monkeypatch.setattr(azure_publish, "BlobLeaseClient", Lease)
    root, snapshot = local_snapshot(tmp_path)
    private, serving = Container(), Container()
    azure_publish.publish(root, private, serving)
    assert json.loads(serving.get_blob_client("active.json").data)["snapshot"] == snapshot
    azure_publish.publish(root, private, serving)
    assert len(serving.blobs) == 3
    assert private.get_blob_client("files/" + "b" * 64 + "/complete.json").content_settings.cache_control == "private, no-store"
    assert serving.get_blob_client(f"snapshots/{snapshot}/page.json").content_settings.cache_control.endswith("immutable")
    page = serving.get_blob_client(f"snapshots/{snapshot}/page.json")
    assert page.content_settings.content_encoding == "gzip"
    assert gzip.decompress(page.data) == b'{"value":1}'
    assert page.data == (root / "public" / "snapshots" / snapshot / "page.json.gz").read_bytes()


def test_failed_upload_preserves_previous_pointer(tmp_path, monkeypatch):
    monkeypatch.setattr(azure_publish, "BlobLeaseClient", Lease)
    root, snapshot = local_snapshot(tmp_path)
    private, serving = Container(), Container()
    pointer = serving.get_blob_client("active.json")
    pointer.upload_blob(b'{"snapshot":"previous"}')
    conflicting = serving.get_blob_client(f"snapshots/{snapshot}/page.json")
    conflicting.upload_blob(b'{"wrong":"content"}', metadata={"sha256": "wrong"})
    with pytest.raises(SourceError, match="conflict"):
        azure_publish.publish(root, private, serving)
    assert pointer.data == b'{"snapshot":"previous"}'


def test_incomplete_checkpoint_is_not_published(tmp_path, monkeypatch):
    monkeypatch.setattr(azure_publish, "BlobLeaseClient", Lease)
    root, _ = local_snapshot(tmp_path)
    (root / "files" / ("b" * 64) / "complete.json").unlink()
    serving = Container()
    with pytest.raises(SourceError, match="incomplete"):
        azure_publish.publish(root, Container(), serving)
    assert serving.get_blob_client("active.json").data is None


def test_oversized_public_upload_is_rejected_before_upload(tmp_path):
    path = tmp_path / "oversized.json"
    path.write_bytes(b"x" * (4 * 1024 * 1024 + 1))
    container = Container()
    with pytest.raises(SourceError, match="uncompressed object limit"):
        azure_publish.upload_immutable(container, "oversized.json", path, public=True)
    assert not container.blobs
