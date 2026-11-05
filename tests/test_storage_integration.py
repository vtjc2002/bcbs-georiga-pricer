import json
import os
import time
import gzip
import io
import uuid

import pytest
from azure.core.exceptions import ServiceRequestError
from azure.storage.blob import BlobServiceClient

from etl.azure_publish import publish
from .test_publication import local_snapshot
from .test_synapse import Client
from .test_etl import fixture
from etl.synapse import ingest_source
from etl.synapse_job import upload_public


@pytest.mark.skipif(not os.environ.get("AZURITE_CONNECTION_STRING"), reason="Isolated Azurite not requested")
def test_real_blob_lease_and_conditional_publication(tmp_path):
    with BlobServiceClient.from_connection_string(os.environ["AZURITE_CONNECTION_STRING"]) as service:
        for attempt in range(30):
            try:
                service.get_service_properties()
                break
            except ServiceRequestError:
                if attempt == 29:
                    raise
                time.sleep(1)
        private = service.create_container("private")
        serving = service.create_container("lookup", public_access="blob")
        root, snapshot = local_snapshot(tmp_path)
        publish(root, private, serving)
        pointer = serving.download_blob("active.json").readall()
        assert json.loads(pointer)["snapshot"] == snapshot
        page = serving.get_blob_client(f"snapshots/{snapshot}/page.json")
        assert page.get_blob_properties().content_settings.content_encoding == "gzip"
        assert page.download_blob().readall() == b'{"value":1}'
        publish(root, private, serving)
        assert serving.download_blob("active.json").readall() == pointer
        lease = private.get_blob_client("publication.lock").acquire_lease(lease_duration=15)
        try:
            from azure.core.exceptions import HttpResponseError
            with pytest.raises(HttpResponseError):
                publish(root, private, serving)
            assert serving.download_blob("active.json").readall() == pointer
        finally:
            lease.release()


@pytest.mark.skipif(not os.environ.get("AZURITE_CONNECTION_STRING"), reason="Isolated Azurite not requested")
def test_synapse_remote_checkpoint_and_gzip_public_objects():
    import pyarrow.parquet as pq
    with BlobServiceClient.from_connection_string(os.environ["AZURITE_CONNECTION_STRING"]) as service:
        private = service.create_container("synapse-" + uuid.uuid4().hex)
        client = Client(gzip.compress(json.dumps(fixture()).encode()))
        item = dict(path="https://example.test/a.gz", url="https://example.test/a.gz?signed", plans=[])
        result = ingest_source(item, private, client)
        names = [b.name for b in private.list_blobs()]
        price = next(name for name in names if "/prices/" in name)
        data = private.get_blob_client(price).download_blob().readall()
        assert str(pq.read_table(io.BytesIO(data)).to_pylist()[0]["amount"]) == "123.45000000"
        assert any(name.startswith("checkpoints/") for name in names)
        assert ingest_source(item, private, client)["prefix"] == result["prefix"]
        assert client.gets == 1
        payload = gzip.compress(b'{"amount":"123.45000000"}', mtime=0)
        upload_public(private, "public-fixture.json", payload)
        upload_public(private, "public-fixture.json", payload)
        blob = private.get_blob_client("public-fixture.json")
        assert blob.get_blob_properties().content_settings.content_encoding == "gzip"
        assert blob.download_blob().readall() == b'{"amount":"123.45000000"}'
