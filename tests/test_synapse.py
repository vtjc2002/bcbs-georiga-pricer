import gzip
import io
import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone

import pyarrow.parquet as pq
import pytest
from azure.core.exceptions import ResourceNotFoundError

from etl.source import SourceError
from etl.stream import ingest
from etl.synapse import RemoteParquetSink, digest, ingest_source, put_json, sql_plan
from etl.synapse_serving import code_objects, group_objects, pages, prices, provider_objects
from scripts.build_synapse import build
from .test_etl import fixture


class MemoryBlob:
    def __init__(self, container, name):
        self.container, self.name = container, name

    def upload_blob(self, data, **kwargs):
        assert self.name not in self.container.data
        self.container.data[self.name] = data.read() if hasattr(data, "read") else data
        self.container.metadata[self.name] = kwargs.get("metadata", {})
        self.container.modified[self.name] = datetime.now(timezone.utc)

    def get_blob_properties(self):
        if self.name not in self.container.data:
            raise ResourceNotFoundError("missing")
        return SimpleNamespace(size=len(self.container.data[self.name]),
                               metadata=self.container.metadata[self.name],
                               etag=digest(self.container.data[self.name].hex()))

    def download_blob(self):
        return SimpleNamespace(readall=lambda: self.container.data[self.name])


class MemoryContainer:
    def __init__(self):
        self.data, self.metadata, self.modified = {}, {}, {}

    def get_blob_client(self, name):
        return MemoryBlob(self, name)

    def list_blobs(self, name_starts_with):
        return [SimpleNamespace(name=name, last_modified=self.modified[name])
                for name in sorted(self.data) if name.startswith(name_starts_with)]

    def delete_blob(self, name):
        del self.data[name]


def decode(objects):
    return {name: json.loads(gzip.decompress(payload)) for name, payload in objects}


def test_remote_sink_rotates_and_releases_local_parts(tmp_path):
    remote = MemoryContainer()
    sink = RemoteParquetSink(tmp_path, remote, "normalized/source/attempt", batch_rows=2, part_bytes=1)
    data = fixture()
    data["provider_references"][0]["provider_groups"][0]["npi"] = list(range(1000000000, 1000000101))
    result = ingest(io.BytesIO(json.dumps(data).encode()), tmp_path, "a" * 64, sink=sink)
    assert result["counts"]["members"] == 101
    assert not list(tmp_path.rglob("*.parquet"))
    assert not sink.writers
    members = [pq.read_table(io.BytesIO(value)).to_pylist() for name, value in remote.data.items()
               if "/members/" in name]
    assert len(members) == 51
    assert sum(map(len, members)) == 101
    assert all(len(batch) <= 2 for batch in members)
    assert len({name.split("/")[-2] for name in remote.data}) == 7


def test_remote_sink_does_not_truncate_sql_join_keys(tmp_path):
    sink = RemoteParquetSink(tmp_path, MemoryContainer(), "prefix")
    with pytest.raises(SourceError, match="lossless SQL"):
        sink.add("groups", ref_key="a" * 257, group_id="1", networks=[])
    assert not list(tmp_path.rglob("*.parquet"))


class Response:
    def __init__(self, raw, headers):
        self.raw, self.headers = io.BytesIO(raw), headers

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def raise_for_status(self):
        pass


class Client:
    def __init__(self, content):
        self.content = content
        self.headers = {"ETag": '"strong"', "Content-Length": str(len(content)), "Last-Modified": "date"}
        self.gets = 0

    def head(self, *args, **kwargs):
        return Response(b"", self.headers)

    def get(self, *args, **kwargs):
        self.gets += 1
        return Response(self.content, self.headers)


def test_cloud_checkpoint_reuses_source_but_refreshes_plan_mappings():
    container = MemoryContainer()
    client = Client(gzip.compress(json.dumps(fixture()).encode()))
    item = dict(path="https://example.test/source.json.gz", url="https://example.test/source.json.gz?secret",
                plans=[{"plan_name": "A"}])
    first = ingest_source(item, container, client)
    assert client.gets == 1
    second = ingest_source(dict(item, plans=[{"plan_name": "B"}]), container, client)
    assert client.gets == 1
    assert first["prefix"] == second["prefix"]
    assert second["plans"] == [{"plan_name": "B"}]
    assert not any(b"?secret" in value for name, value in container.data.items() if name.endswith(".json"))


def test_corrupt_cloud_source_never_commits_checkpoint():
    container = MemoryContainer()
    client = Client(gzip.compress(json.dumps(fixture()).encode())[:-5])
    with pytest.raises((EOFError, OSError)):
        ingest_source(dict(path="https://example.test/a.gz", url="https://example.test/a.gz", plans=[]),
                      container, client)
    assert not any(name.startswith("checkpoints/") for name in container.data)


def test_weak_validators_force_body_read_instead_of_cached_success():
    container = MemoryContainer()
    client = Client(gzip.compress(json.dumps(fixture()).encode()))
    client.headers["ETag"] = 'W/"weak"'
    item = dict(path="https://example.test/a.gz", url="https://example.test/a.gz", plans=[])
    first = ingest_source(item, container, client)
    second = ingest_source(item, container, client)
    assert client.gets == 2
    assert first["prefix"] != second["prefix"]
    assert not any(name.startswith("checkpoints/") for name in container.data)


def test_expired_source_link_refresh_is_bounded_and_errors_hide_signatures(monkeypatch):
    import requests
    import etl.synapse as synapse
    monkeypatch.setattr(synapse, "publication_lease", lambda _: nullcontext(lambda: None))
    calls = []

    def prepare(*args):
        calls.append(args)
        if len(calls) == 1:
            response = requests.Response()
            response.status_code = 403
            raise requests.HTTPError("https://example.test/a.gz?signature=secret", response=response)
        return {"run_id": "a" * 32}, ["SELECT 1;"]

    monkeypatch.setattr(synapse, "prepare", prepare)
    result = synapse.run_ingestion(MemoryContainer(), "testaccount", {})
    assert len(calls) == 2
    assert result["scripts"] == [{"type": "NonQuery", "text": "SELECT 1;"}]
    def failed_request(*args):
        raise requests.ConnectionError("https://example.test/a.gz?signature=secret")

    monkeypatch.setattr(synapse, "prepare", failed_request)
    with pytest.raises(SourceError) as error:
        synapse.run_ingestion(MemoryContainer(), "testaccount", {})
    assert "secret" not in str(error.value)


def test_sql_plan_scopes_sources_and_materializes_no_npi_cross_product():
    sources = [{"prefix": f"normalized/{'a' * 64}/{'b' * 32}"},
               {"prefix": f"normalized/{'c' * 64}/{'d' * 32}"}]
    statements = sql_plan("testaccount", "f" * 32, sources)
    text = "\n".join(statements)
    assert text.count("CREATE VIEW") == 7
    assert "varchar(256) COLLATE Latin1_General_100_BIN2_UTF8" in text
    assert "decimal(28,8)" in text
    assert "unresolved provider references" in text
    assert "LEFT(e.set_key,64)=LEFT(g.ref_key,64)" in text
    assert "JOIN [" + "r" + "f" * 32 + "].members" not in statements[-1]
    assert "CREATE EXTERNAL TABLE" in statements[-1]
    assert all(not statement.startswith("CREATE VIEW") or statement.count("CREATE VIEW") == 1
               for statement in statements)
    with pytest.raises(SourceError):
        sql_plan("testaccount", "f" * 32, [{"prefix": "normalized/x';DROP DATABASE prices;--"}])


def edge(group="source:1", network="A", **kwargs):
    return dict(bucket=digest("CPT:00123")[:2], code_type="CPT", code="00123", code_version="2026",
                arrangement="ffs", price_key="source:c1:s1:p1", set_key="source:c1:s1",
                amount=Decimal("123.45000000"), price_type="negotiated", expiration="9999-12-31",
                service_codes=["11"], billing_class="professional", setting="outpatient", modifiers=["26"],
                additional_information=None, public_group=group, networks=[network], **kwargs)


def test_partition_reducers_preserve_existing_public_contract():
    code = digest("CPT:00123")
    objects = decode(code_objects(iter([edge(), edge("source:2", "B")])))
    price = objects[f"codes/{code}/0.json"][0]
    assert price["amount"] == "123.45000000"
    assert price["group_networks"] == {"source:1": ["A"], "source:2": ["B"]}
    assert price["groups"] == ["source:1", "source:2"]
    assert "bucket" not in price and "public_group" not in price
    assert objects[f"catalogue/{code[:2]}.json"]["CPT:00123"] == {
        "key": code, "pages": 1, "count": 1}
    bucket = digest("1111111111")[:3]
    providers = decode(provider_objects(iter([
        dict(bucket=bucket, npi="1111111111", public_group="source:1"),
        dict(bucket=bucket, npi="1111111111", public_group="source:2"),
    ])))
    assert providers[f"providers/{bucket}.json"]["1111111111"] == ["source:1", "source:2"]
    groups = decode(group_objects(iter([dict(public_group="source:1", code_type="CPT", code="00123")])))
    assert groups[f"groups/{digest('source:1')}/0.json"][0]["code"] == "00123"


def test_cloud_pages_enforce_rows_bytes_and_price_context_bounds():
    result = decode(pages("codes/test", ({"id": number} for number in range(201))))
    assert result["codes/test/index.json"] == {"pages": 3, "count": 201}
    result = decode(pages("large", ({"context": "x" * 1100000} for _ in range(3))))
    assert result["large/index.json"]["pages"] == 3
    with pytest.raises(SourceError, match="4 MiB"):
        list(pages("bad", [{"context": "x" * 4194304}]))
    with pytest.raises(SourceError, match="bounded"):
        list(prices(edge(f"source:{i}") for i in range(20002)))


def test_artifact_builder_wires_serverless_sql_before_publication(tmp_path):
    root = Path(__file__).resolve().parents[1]
    files = build(root, tmp_path, "test-workspace", "testprivate", "testpublic")
    activity = files["GeorgiaMonthly.json"]["properties"]["activities"]
    assert files["GeorgiaMonthly.json"]["properties"]["concurrency"] == 1
    nested = activity[1]["typeProperties"]["ifTrueActivities"]
    assert nested[0]["name"] == "Transform" and nested[0]["type"] == "Script"
    assert nested[1]["dependsOn"] == [{"activity": "Transform", "dependencyConditions": ["Succeeded"]}]
    assert activity[1]["typeProperties"]["ifFalseActivities"] == []
    assert ".output.status.Output.result.exitValue" in nested[0]["typeProperties"]["scripts"]["value"]
    assert ".output.status.Output.result.exitValue" in nested[1]["typeProperties"]["parameters"][
        "run_id"]["value"]["value"]
    assert files["ServerlessPrices.json"]["properties"]["typeProperties"]["authenticationType"] == \
        "SystemAssignedManagedIdentity"
    assert files["GeorgiaSchedule.json"]["properties"]["typeProperties"]["recurrence"]["schedule"][
        "monthDays"] == [3, 10, 17]
    assert "code/" in files["bundle.json"]["blob"]
    first = (tmp_path / "etl.zip").read_bytes()
    build(root, tmp_path, "test-workspace", "testprivate", "testpublic")
    assert first == (tmp_path / "etl.zip").read_bytes()


def test_retention_preserves_current_previous_recent_and_invalidate_old_checkpoints(monkeypatch):
    import etl.synapse_job as job
    monkeypatch.setattr(job, "publication_lease", lambda _: nullcontext(lambda: None))
    private, serving = MemoryContainer(), MemoryContainer()
    now = datetime.now(timezone.utc)
    old = now - timedelta(days=30)
    for digit in "1234":
        prefix = f"normalized/{digit * 64}/{'a' * 32}"
        run = dict(run_id=digit * 32, snapshot=digit * 64, sources=[dict(prefix=prefix)])
        put_json(private, f"runs/{digit * 32}/run.json", run)
        put_json(private, f"{prefix}/prices/000000.parquet", {})
        put_json(private, f"checkpoints/{digit * 64}.json", {"prefix": prefix})
        put_json(serving, f"snapshots/{digit * 64}/manifest.json", {})
        if digit != "4":
            for container in (private, serving):
                for name in container.data:
                    if digit * 32 in name or digit * 64 in name:
                        container.modified[name] = old
    put_json(serving, "active.json", dict(run_id="1" * 32, snapshot="1" * 64,
                                         previous_run="2" * 32, previous_snapshot="2" * 64))
    result = job.retain(private, serving, now=now)
    assert result["deleted_objects"] == 4
    assert not any("3" * 32 in name or "3" * 64 in name for name in private.data)
    assert not any("3" * 64 in name for name in serving.data)
    for digit in "124":
        assert f"checkpoints/{digit * 64}.json" in private.data
        assert f"snapshots/{digit * 64}/manifest.json" in serving.data


def test_retention_missing_previous_manifest_deletes_nothing(monkeypatch):
    import etl.synapse_job as job
    monkeypatch.setattr(job, "publication_lease", lambda _: nullcontext(lambda: None))
    private, serving = MemoryContainer(), MemoryContainer()
    put_json(serving, "active.json", dict(run_id="1" * 32, snapshot="1" * 64))
    with pytest.raises(SourceError, match="nothing was deleted"):
        job.retain(private, serving)
    assert not private.data
