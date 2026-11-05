"""Synapse driver ingestion and SQL planning; raw documents never enter Spark rows."""

import base64
import gzip
import hashlib
import json
import logging
import re
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

from azure.core.credentials import AccessToken
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import ContentSettings
import requests

from .azure_publish import publication_lease, upload_immutable
from .schema import SCHEMAS
from .source import HashingReader, SourceError, discover_index, read_index, select_files, session
from .stream import ParquetSink, ingest

LOG = logging.getLogger("georgia-prices")
VERSION = "synapse-1"
MAX_JSON = 4 * 1024 * 1024
KEY_FIELDS = {"ref_key", "subgroup_key", "group_id", "service_key", "set_key", "price_key"}


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def encode(value):
    return json.dumps(value, separators=(",", ":"), default=str).encode()


def read_json(container, name):
    try:
        blob = container.get_blob_client(name)
        if blob.get_blob_properties().size > MAX_JSON:
            raise SourceError("Operational JSON exceeds 4 MiB")
        return json.loads(blob.download_blob().readall())
    except ResourceNotFoundError:
        return None


def put_json(container, name, value):
    payload = encode(value)
    if len(payload) > MAX_JSON:
        raise SourceError("Operational JSON exceeds 4 MiB")
    container.get_blob_client(name).upload_blob(
        payload, overwrite=False,
        content_settings=ContentSettings(content_type="application/json", cache_control="private, no-store"),
    )


class SynapseCredential:
    def get_token(self, *scopes, **kwargs):
        from notebookutils import mssparkutils
        token = mssparkutils.credentials.getToken("Storage")
        # The SDK needs the expiry for refresh; signature validation belongs to Azure.
        payload = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        return AccessToken(token, int(claims["exp"]))


class RemoteParquetSink(ParquetSink):
    """Upload each bounded batch, then unlink it; at most one part is on disk."""

    def __init__(self, path, container, prefix, batch_rows=10000, part_bytes=32 * 1024 * 1024):
        super().__init__(path, batch_rows=batch_rows)
        self.container = container
        self.prefix = prefix
        self.part_bytes = part_bytes

    def add(self, table, **row):
        for name, value in row.items():
            limit = 256 if name in KEY_FIELDS else 32 if name == "npi" else None
            if limit and (not isinstance(value, str) or len(value.encode()) > limit):
                raise SourceError(f"{name} exceeds the lossless SQL key contract")
        super().add(table, **row)

    def flush(self, table):
        import pyarrow as pa
        import pyarrow.parquet as pq
        folder = self.path / table
        folder.mkdir(exist_ok=True)
        path = folder / f"{self.parts[table]:06d}.parquet"
        if table not in self.writers:
            self.writers[table] = pq.ParquetWriter(path, SCHEMAS[table], compression="snappy")
        self.writers[table].write_table(pa.Table.from_pylist(self.rows[table], schema=SCHEMAS[table]),
                                       row_group_size=self.batch_rows)
        if path.stat().st_size >= self.part_bytes:
            self.commit(table)
        self.rows[table].clear()
        self.sizes[table] = 0

    def commit(self, table):
        self.writers.pop(table).close()
        path = self.path / table / f"{self.parts[table]:06d}.parquet"
        upload_immutable(self.container, f"{self.prefix}/{table}/{path.name}", path)
        path.unlink()
        self.parts[table] += 1

    def close(self):
        for table in SCHEMAS:
            if self.rows[table] or (not self.parts[table] and table not in self.writers):
                self.flush(table)
            if table in self.writers:
                self.commit(table)


def ingest_source(item, private, client):
    filename = Path(item["path"]).name
    with client.head(item["url"], timeout=(15, 120), allow_redirects=True) as response:
        response.raise_for_status()
        headers = {k: response.headers.get(k) for k in ("ETag", "Content-Length", "Last-Modified")}
    etag = headers["ETag"]
    reusable = bool(etag and not etag.startswith("W/") and headers["Content-Length"])
    identity = digest(json.dumps([VERSION, item["path"], headers], sort_keys=True))
    cache = f"checkpoints/{identity}.json"
    previous = read_json(private, cache) if reusable else None
    if previous:
        LOG.info("Reusing completed source %s", filename)
        return dict(previous, plans=item["plans"])
    attempt = uuid.uuid4().hex
    prefix = f"normalized/{identity}/{attempt}"
    LOG.info("Streaming %s directly to private Parquet", filename)
    with tempfile.TemporaryDirectory(prefix="georgia-ingest-") as directory:
        sink = RemoteParquetSink(directory, private, prefix)
        with client.get(item["url"], stream=True, timeout=(15, 120)) as response:
            response.raise_for_status()
            if etag and response.headers.get("ETag") != etag:
                raise SourceError("Source changed between HEAD and GET")
            reader = HashingReader(response.raw)
            with gzip.GzipFile(fileobj=reader) as raw:
                result = ingest(raw, directory, identity, sink=sink)
            if headers["Content-Length"] and reader.bytes != int(headers["Content-Length"]):
                raise SourceError("Compressed source length changed")
    result.update(id=identity, prefix=prefix, filename=filename, headers=headers,
                  sha256=reader.digest.hexdigest(), compressed_bytes=reader.bytes,
                  plans=item["plans"])
    # This is a complete parse checkpoint, not permission to publish: SQL checks follow.
    if reusable:
        put_json(private, cache, result)
    else:
        put_json(private, f"{prefix}/complete.json", result)
    return result


def sql_plan(account, run_id, sources):
    if not re.fullmatch(r"[a-z0-9]{3,24}", account) or not re.fullmatch(r"[0-9a-f]{32}", run_id):
        raise SourceError("Invalid SQL storage account or run identity")
    if not sources:
        raise SourceError("SQL transformation requires at least one source")
    schema = "r" + run_id
    sql = [
        "IF NOT EXISTS (SELECT 1 FROM sys.database_scoped_credentials WHERE name=N'WorkspaceIdentity') "
        "THROW 51000, 'Run bootstrap.sql to configure the WorkspaceIdentity credential', 1;",
        f"CREATE SCHEMA [{schema}];",
        f"CREATE EXTERNAL DATA SOURCE [{schema}] WITH (LOCATION="
        f"'https://{account}.dfs.core.windows.net/private', CREDENTIAL=WorkspaceIdentity);",
        f"CREATE EXTERNAL FILE FORMAT [{schema}] WITH "
        "(FORMAT_TYPE=PARQUET, DATA_COMPRESSION='org.apache.hadoop.io.compress.SnappyCodec');",
    ]
    for table, arrow_schema in SCHEMAS.items():
        def sql_type(name):
            if name == "amount":
                return "decimal(28,8)"
            if name in KEY_FIELDS:
                return "varchar(256) COLLATE Latin1_General_100_BIN2_UTF8"
            if name == "npi":
                return "varchar(32) COLLATE Latin1_General_100_BIN2_UTF8"
            return "varchar(max) COLLATE Latin1_General_100_BIN2_UTF8"
        columns = ", ".join(f"[{field.name}] {sql_type(field.name)}" for field in arrow_schema)
        queries = []
        for source in sources:
            prefix = source["prefix"]
            if not re.fullmatch(r"normalized/[0-9a-f]{64}/[0-9a-f]{32}", prefix):
                raise SourceError("Invalid immutable source prefix")
            queries.append(
                f"SELECT * FROM OPENROWSET(BULK '{prefix}/{table}/*.parquet', "
                f"DATA_SOURCE='{schema}', FORMAT='PARQUET') WITH ({columns}) AS data")
        sql.append(f"CREATE VIEW [{schema}].[{table}] AS " + " UNION ALL ".join(queries) + ";")
    checks = [
        ("duplicate provider groups",
         f"SELECT LEFT(ref_key,64) source,group_id FROM [{schema}].groups "
         "GROUP BY LEFT(ref_key,64),group_id HAVING COUNT_BIG(*)>1"),
        ("unresolved provider references",
         f"SELECT e.set_key FROM [{schema}].set_groups e LEFT JOIN [{schema}].groups g "
         "ON e.group_id=g.group_id AND LEFT(e.set_key,64)=LEFT(g.ref_key,64) WHERE g.ref_key IS NULL"),
        ("sets without providers",
         f"SELECT s.set_key FROM [{schema}].sets s LEFT JOIN [{schema}].set_groups e "
         "ON s.set_key=e.set_key WHERE e.set_key IS NULL"),
        ("sets without prices",
         f"SELECT s.set_key FROM [{schema}].sets s LEFT JOIN [{schema}].prices p "
         "ON s.set_key=p.set_key WHERE p.set_key IS NULL"),
        ("duplicate provider links",
         f"SELECT set_key,group_id FROM [{schema}].set_groups GROUP BY set_key,group_id HAVING COUNT_BIG(*)>1"),
        ("orphan memberships",
         f"SELECT m.ref_key FROM [{schema}].members m LEFT JOIN [{schema}].groups g "
         "ON m.ref_key=g.ref_key WHERE g.ref_key IS NULL"),
    ]
    for message, query in checks:
        sql.append(f"IF EXISTS ({query}) THROW 51000, '{message}', 1;")
    sql.append(f"""
CREATE EXTERNAL TABLE [{schema}].[rate_edges]
WITH (LOCATION='runs/{run_id}/rate_edges', DATA_SOURCE=[{schema}], FILE_FORMAT=[{schema}])
AS SELECT s.code_type,s.code,s.code_version,s.arrangement,
    p.price_key,p.amount,p.price_type,p.expiration,p.service_codes,p.billing_class,
    p.setting,p.modifiers,p.additional_information,p.set_key,
    CONCAT(LEFT(g.ref_key,64),':',g.group_id) public_group,g.networks
FROM [{schema}].prices p
JOIN [{schema}].sets r ON p.set_key=r.set_key
JOIN [{schema}].services s ON r.service_key=s.service_key
JOIN [{schema}].set_groups e ON p.set_key=e.set_key
JOIN [{schema}].groups g ON e.group_id=g.group_id AND LEFT(e.set_key,64)=LEFT(g.ref_key,64);
""")
    return sql


def prepare(private, account, config, index=None):
    with session() as client:
        location = index or discover_index(config, client)
        employer = read_index(location, client)
        selected = select_files(employer, config)
        if len(selected) != config["expected_file_count"]:
            raise SourceError("Selected Georgia file count changed; review metadata/source.json")
        sources = [ingest_source(item, private, client) for item in selected]
    snapshot = digest(json.dumps([VERSION, [(s["id"], s["sha256"], s["plans"]) for s in sources]],
                                 sort_keys=True))
    run_id = uuid.uuid4().hex
    run = dict(run_id=run_id, snapshot=snapshot, index_date=employer["last_updated_on"], sources=sources,
               prepared_at=datetime.now(timezone.utc).isoformat())
    put_json(private, f"runs/{run_id}/run.json", run)
    return run, sql_plan(account, run_id, sources)


def run_ingestion(private, account, config, index=None):
    with publication_lease(private.get_blob_client("synapse-ingestion.lock")) as assert_owned:
        try:
            run, statements = prepare(private, account, config, index)
        except requests.HTTPError as error:
            if index or error.response is None or error.response.status_code not in (401, 403):
                raise SourceError("Source request failed; previous public snapshot is unchanged") from None
            LOG.warning("Refreshing publisher discovery after an expired source link")
            try:
                run, statements = prepare(private, account, config)
            except requests.RequestException:
                raise SourceError("Source request failed after refreshing signed links") from None
        except requests.RequestException:
            raise SourceError("Source request failed; previous public snapshot is unchanged") from None
        assert_owned()
        # Script activity executes batches separately so CREATE VIEW is first in its batch.
        return {"run_id": run["run_id"], "scripts": [
            {"type": "NonQuery", "text": statement} for statement in statements]}
