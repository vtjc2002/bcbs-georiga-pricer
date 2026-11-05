import gzip
import hashlib
import io
import json
import logging
import threading
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit

from azure.core import MatchConditions
from azure.core.exceptions import AzureError, ResourceExistsError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobLeaseClient, BlobServiceClient, ContentSettings

from .source import SourceError, read_gzip_json

LOG = logging.getLogger("georgia-prices")


@contextmanager
def publication_lease(blob):
    try:
        blob.upload_blob(b"", overwrite=False)
    except ResourceExistsError:
        pass
    lease = BlobLeaseClient(blob)
    lease.acquire(lease_duration=60)
    stop = threading.Event()
    errors = []

    def renew():
        while not stop.wait(20):
            try:
                lease.renew()
            except AzureError as error:
                # The foreground must fail before publishing if the lease is lost.
                errors.append(error)
                stop.set()

    thread = threading.Thread(target=renew, daemon=True)
    thread.start()

    def assert_owned():
        if errors:
            raise SourceError("Publication lease renewal failed; active snapshot was not replaced") from errors[0]
        lease.renew()

    try:
        yield assert_owned
    finally:
        stop.set()
        thread.join()
        lease.release()


def upload_immutable(container, name, path, public=False):
    payload = None
    with Path(path).open("rb") as stream:
        if public:
            if str(path).endswith(".json.gz"):
                if Path(path).stat().st_size > 4 * 1024 * 1024 + 65536:
                    raise SourceError("Compressed public upload exceeds the supported object limit")
                read_gzip_json(path)
                payload = stream.read()
            else:
                content = stream.read(4 * 1024 * 1024 + 1)
                if len(content) > 4 * 1024 * 1024:
                    raise SourceError("Public upload exceeds the 4 MiB uncompressed object limit")
                payload = gzip.compress(content, compresslevel=6, mtime=0)
            digest = hashlib.sha256(payload).hexdigest()
        else:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
    content_type = "application/json" if public or str(path).endswith(".json") else "application/octet-stream"
    blob = container.get_blob_client(name)
    try:
        with (Path(path).open("rb") if payload is None else io.BytesIO(payload)) as stream:
            blob.upload_blob(
                stream, overwrite=False, metadata={"sha256": digest},
                content_settings=ContentSettings(
                    content_type=content_type,
                    content_encoding="gzip" if public else None,
                    cache_control="public, max-age=31536000, immutable" if public else "private, no-store"),
                max_concurrency=2,
            )
    except ResourceExistsError:
        if blob.get_blob_properties().metadata.get("sha256") != digest:
            raise SourceError(f"Immutable blob content conflict: {name}") from None


def publish(root, private, serving):
    root = Path(root)
    active = json.loads((root / "public" / "active.json").read_text())
    snapshot = active["snapshot"]
    if not isinstance(snapshot, str) or len(snapshot) != 64 or any(c not in "0123456789abcdef" for c in snapshot):
        raise SourceError("Invalid local snapshot identity")
    public = root / "public" / "snapshots" / snapshot
    manifest = read_gzip_json(public / "manifest.json.gz")
    if manifest["snapshot"] != snapshot or active["manifest"] != f"snapshots/{snapshot}/manifest.json":
        raise SourceError("Local publication manifest does not match snapshot")
    with publication_lease(private.get_blob_client("publication.lock")) as assert_owned:
        pointer = serving.get_blob_client("active.json")
        try:
            previous = pointer.get_blob_properties().etag
        except ResourceNotFoundError:
            previous = None
        for source in manifest["sources"]:
            identity = source["id"]
            if not isinstance(identity, str) or len(identity) != 64 or any(c not in "0123456789abcdef" for c in identity):
                raise SourceError("Invalid source checkpoint identity")
            folder = root / "files" / source["id"]
            if not (folder / "complete.json").is_file():
                raise SourceError("A source checkpoint is incomplete")
            for path in sorted(folder.rglob("*")):
                if path.is_file():
                    upload_immutable(private, "files/" + source["id"] + "/" + path.relative_to(folder).as_posix(), path)
        for path in sorted(public.rglob("*.json.gz")):
            name = path.relative_to(public).as_posix().removesuffix(".gz")
            upload_immutable(serving, f"snapshots/{snapshot}/" + name, path, public=True)
        assert_owned()
        options = {"overwrite": True, "etag": previous, "match_condition": MatchConditions.IfNotModified} if previous else {"overwrite": False}
        pointer.upload_blob(
            json.dumps(active).encode(),
            content_settings=ContentSettings(content_type="application/json", cache_control="no-cache"),
            **options,
        )
    LOG.info("Published immutable snapshot %s; keep its predecessor for rollback", snapshot)


def publish_command(args):
    for account in (args.private_account, args.serving_account):
        if urlsplit(account).scheme != "https" or not urlsplit(account).hostname.endswith(".blob.core.windows.net"):
            raise SourceError("Production storage endpoints must be HTTPS Azure Blob account URLs")
    with DefaultAzureCredential() as credential:
        with BlobServiceClient(args.private_account, credential=credential) as private:
            with BlobServiceClient(args.serving_account, credential=credential) as serving:
                publish(args.output, private.get_container_client("private"),
                        serving.get_container_client("lookup"))
