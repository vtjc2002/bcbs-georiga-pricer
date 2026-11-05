"""Synapse Spark job phases called by the monthly pipeline."""

import gzip
import hashlib
import json
import re
import tempfile
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from azure.core import MatchConditions
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContentSettings

from .azure_publish import publication_lease
from .source import SourceError
from .synapse import MAX_JSON, SynapseCredential, encode, read_json, run_ingestion

LOG = logging.getLogger("georgia-prices")


def dictionaries(rows):
    for row in rows:
        yield row.asDict(recursive=True)


def code_partition(rows):
    from .synapse_serving import code_objects
    yield from code_objects(dictionaries(rows))


def provider_partition(rows):
    from .synapse_serving import provider_objects
    yield from provider_objects(dictionaries(rows))


def group_partition(rows):
    from .synapse_serving import group_objects
    yield from group_objects(dictionaries(rows))


def upload_public(serving, name, payload):
    if len(payload) > MAX_JSON + 65536 or len(gzip.decompress(payload)) > MAX_JSON:
        raise SourceError("Public artifact exceeds supported size")
    checksum = hashlib.sha256(payload).hexdigest()
    blob = serving.get_blob_client(name)
    try:
        blob.upload_blob(
            payload, overwrite=False, metadata={"sha256": checksum},
            content_settings=ContentSettings(content_type="application/json", content_encoding="gzip",
                                             cache_control="public, max-age=31536000, immutable"),
        )
    except ResourceExistsError:
        if blob.get_blob_properties().metadata.get("sha256") != checksum:
            raise SourceError("Immutable public artifact conflict") from None


def projection_frames(spark, lake, run):
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType
    # This path exists only after the pipeline's serverless SQL Script activity succeeds.
    edges = spark.read.parquet(f"{lake}/runs/{run['run_id']}/rate_edges/*.parquet")
    for name in ("networks", "service_codes", "modifiers"):
        edges = edges.withColumn(name, F.from_json(F.col(name), ArrayType(StringType())))
    invalid = edges.where(
        F.col("networks").isNull() | F.col("service_codes").isNull() | F.col("modifiers").isNull())
    if invalid.limit(1).count():
        raise SourceError("SQL Parquet lost array context")
    if not edges.limit(1).count():
        raise SourceError("Serverless SQL produced no prices")
    groups = spark.read.parquet(*[
        f"{lake}/{source['prefix']}/groups/*.parquet" for source in run["sources"]
    ]).withColumn("public_group", F.concat_ws(":", F.substring("ref_key", 1, 64), "group_id"))
    members = spark.read.parquet(*[
        f"{lake}/{source['prefix']}/members/*.parquet" for source in run["sources"]
    ])
    used = edges.select("public_group").distinct()
    providers = (members.join(groups.select("ref_key", "public_group"), "ref_key")
                 .join(used, "public_group").where(F.col("npi").rlike("^[1-9][0-9]{9}$"))
                 .select("npi", "public_group").distinct()
                 .withColumn("bucket", F.substring(F.sha2("npi", 256), 1, 3)))
    reverse = edges.select("public_group", "code_type", "code").distinct()
    reverse = reverse.withColumn("bucket", F.substring(F.sha2("public_group", 256), 1, 2))
    codes = edges.withColumn(
        "bucket", F.substring(F.sha2(F.concat_ws(":", "code_type", "code"), 256), 1, 2))
    return [
        ("codes", codes, ["bucket", "code_type", "code", "price_key", "public_group"], code_partition),
        ("providers", providers, ["bucket", "npi", "public_group"], provider_partition),
        ("groups", reverse, ["bucket", "public_group", "code_type", "code"], group_partition),
    ]


def artifact_frame(spark, frame, order, reducer, partitions):
    from pyspark.sql.types import StringType, StructType, StructField, BinaryType
    schema = StructType([StructField("name", StringType(), False),
                         StructField("payload", BinaryType(), False)])
    partitioned = frame.repartition(partitions, "bucket").sortWithinPartitions(*order)
    return spark.createDataFrame(partitioned.rdd.mapPartitions(reducer), schema)


def project(spark, private, serving, private_account, run_id, partitions=128, lake=None):
    from .synapse_serving import artifact
    import pyarrow.parquet as pq

    if not re.fullmatch(r"[0-9a-f]{32}", run_id) or not 1 <= partitions <= 4096:
        raise SourceError("Invalid run identity or Spark partition count")
    run = read_json(private, f"runs/{run_id}/run.json")
    if run is None:
        raise SourceError("Run manifest not found")
    lake = lake or f"abfss://private@{private_account}.dfs.core.windows.net"
    frames = projection_frames(spark, lake, run)
    snapshot = run["snapshot"]
    with publication_lease(private.get_blob_client("publication.lock")) as assert_owned:
        pointer = serving.get_blob_client("active.json")
        previous = read_json(serving, "active.json")
        try:
            previous_etag = pointer.get_blob_properties().etag
        except ResourceNotFoundError:
            previous_etag = None
        if previous and (previous["index_date"] > run["index_date"] or
                         previous.get("prepared_at", "") > run["prepared_at"]):
            raise SourceError("Refusing to replace a newer completed refresh")
        catalogue_buckets, provider_buckets, code_count = [], [], 0
        for kind, frame, order, reducer in frames:
            spool = f"runs/{run_id}/objects/{kind}"
            artifacts = artifact_frame(spark, frame, order, reducer, partitions)
            (artifacts.write.mode("errorifexists").option("maxRecordsPerFile", 16)
             .parquet(f"{lake}/{spool}"))
            # Do not collect partitions into driver memory. Each spool part has <=16 bounded objects.
            for blob in private.list_blobs(name_starts_with=spool + "/"):
                if not blob.name.endswith(".parquet"):
                    continue
                if blob.size > 70 * 1024 * 1024:
                    raise SourceError("Projection spool part exceeds bounded download size")
                with tempfile.TemporaryDirectory(prefix="georgia-publish-") as directory:
                    path = Path(directory) / "objects.parquet"
                    with path.open("wb") as stream:
                        private.get_blob_client(blob.name).download_blob().readinto(stream)
                    for batch in pq.ParquetFile(path).iter_batches(batch_size=1):
                        row = batch.to_pylist()[0]
                        name, payload = row["name"], row["payload"]
                        upload_public(serving, f"snapshots/{snapshot}/{name}", payload)
                        if name.startswith("catalogue/"):
                            catalogue_buckets.append(Path(name).stem)
                            code_count += len(json.loads(gzip.decompress(payload)))
                        elif name.startswith("providers/"):
                            provider_buckets.append(Path(name).stem)
                assert_owned()
        plans = {p["plan_name"]: p for source in run["sources"] for p in source["plans"]}
        public_sources = [dict(id=s["id"], filename=s["filename"], sha256=s["sha256"],
                               data_date=s["metadata"]["last_updated_on"],
                               schema_version=s["metadata"]["version"], plans=s["plans"])
                          for s in run["sources"]]
        manifest = dict(format_version=1, snapshot=snapshot, plans=list(plans.values()),
                        sources=public_sources, code_count=code_count,
                        provider_bucket_algorithm="sha256-prefix-3",
                        catalogue_buckets=sorted(catalogue_buckets), provider_buckets=sorted(provider_buckets),
                        disclaimer="Published negotiated rates, not your out-of-pocket cost. "
                        "Only selected Georgia network files are included; not all Georgia BCBS plans.")
        name, payload = artifact("manifest.json", manifest)
        upload_public(serving, f"snapshots/{snapshot}/{name}", payload)
        assert_owned()
        options = dict(overwrite=True, etag=previous_etag, match_condition=MatchConditions.IfNotModified) \
            if previous_etag else dict(overwrite=False)
        pointer.upload_blob(
            encode(dict(format_version=1, snapshot=snapshot, index_date=run["index_date"],
                        manifest=f"snapshots/{snapshot}/manifest.json", prepared_at=run["prepared_at"],
                        run_id=run_id, previous_run=previous.get("run_id") if previous else None,
                        previous_snapshot=previous["snapshot"] if previous else None)),
            content_settings=ContentSettings(content_type="application/json", cache_control="no-cache"),
            **options)
    return {"snapshot": snapshot, "code_count": code_count}


def retain(private, serving, now=None):
    cutoff = (now or datetime.now(timezone.utc)) - timedelta(days=7)
    with publication_lease(private.get_blob_client("synapse-ingestion.lock")):
        with publication_lease(private.get_blob_client("publication.lock")) as assert_owned:
            active = read_json(serving, "active.json")
            if not active or not active.get("run_id"):
                raise SourceError("Retention requires a published Synapse run pointer")
            keep_runs = {active["run_id"]}
            if active.get("previous_run"):
                keep_runs.add(active["previous_run"])
            manifests = {}
            for blob in private.list_blobs(name_starts_with="runs/"):
                if re.fullmatch(r"runs/[0-9a-f]{32}/run.json", blob.name):
                    run = read_json(private, blob.name)
                    manifests[run["run_id"]] = run
                    if blob.last_modified >= cutoff:
                        keep_runs.add(run["run_id"])
            if not keep_runs <= manifests.keys():
                raise SourceError("Retention cannot resolve a retained run; nothing was deleted")
            prefixes = {s["prefix"] for run_id in keep_runs for s in manifests[run_id]["sources"]}
            keep_snapshots = {active["snapshot"], active.get("previous_snapshot")}
            keep_snapshots.update(manifests[run_id]["snapshot"] for run_id in keep_runs)
            deletions = []
            # Invalidate obsolete checkpoints before deleting any corresponding data.
            for blob in private.list_blobs(name_starts_with="checkpoints/"):
                if not re.fullmatch(r"checkpoints/[0-9a-f]{64}.json", blob.name):
                    continue
                checkpoint = read_json(private, blob.name)
                if checkpoint["prefix"] not in prefixes and blob.last_modified < cutoff:
                    deletions.append(blob.name)
                else:
                    prefixes.add(checkpoint["prefix"])
            for name in deletions:
                assert_owned()
                private.delete_blob(name)
            deleted = len(deletions)
            for namespace in ("normalized/", "runs/"):
                for blob in private.list_blobs(name_starts_with=namespace):
                    if blob.last_modified >= cutoff:
                        continue
                    parts = blob.name.split("/")
                    eligible = (
                        namespace == "normalized/" and
                        re.fullmatch(r"normalized/[0-9a-f]{64}/[0-9a-f]{32}/[^/]+/[^/]+", blob.name) and
                        "/".join(parts[:3]) not in prefixes
                    ) or (
                        namespace == "runs/" and re.fullmatch(r"[0-9a-f]{32}", parts[1]) and
                        parts[1] not in keep_runs
                    )
                    if eligible:
                        assert_owned()
                        private.delete_blob(blob.name)
                        deleted += 1
            for blob in serving.list_blobs(name_starts_with="snapshots/"):
                parts = blob.name.split("/")
                if (len(parts) > 2 and re.fullmatch(r"[0-9a-f]{64}", parts[1]) and
                        parts[1] not in keep_snapshots and blob.last_modified < cutoff):
                    assert_owned()
                    serving.delete_blob(blob.name)
                    deleted += 1
            LOG.info("Retention removed %s obsolete objects; current and previous snapshots retained", deleted)
            return {"deleted_objects": deleted}


def execute(phase, private_account, serving_account, config, run_id="", partitions=128, spark=None):
    for account in (private_account, serving_account):
        if not re.fullmatch(r"[a-z0-9]{3,24}", account):
            raise SourceError("Invalid Azure storage account name")
    credential = SynapseCredential()
    with BlobServiceClient(f"https://{private_account}.blob.core.windows.net", credential) as raw:
        with BlobServiceClient(f"https://{serving_account}.blob.core.windows.net", credential) as public:
            private, serving = raw.get_container_client("private"), public.get_container_client("lookup")
            if phase == "ingest":
                result = run_ingestion(private, private_account, config)
                run = read_json(private, f"runs/{result['run_id']}/run.json")
                active = read_json(serving, "active.json")
                return dict(result, skip=bool(active and active["snapshot"] == run["snapshot"]
                                              and active["index_date"] == run["index_date"]))
            if phase == "retain":
                return retain(private, serving)
            if phase != "publish" or spark is None:
                raise SourceError("Expected ingest, publish (with Spark session), or retain phase")
            return project(spark, private, serving, private_account, run_id, partitions)
