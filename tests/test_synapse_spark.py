import gzip
import json
import os
from contextlib import nullcontext
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from etl.projection import build, connect
from etl.source import read_gzip_json
from etl.synapse import digest, put_json
from etl.synapse_job import artifact_frame, projection_frames
from etl.source import SourceError
from .test_synapse import MemoryContainer


@pytest.mark.skipif(not os.environ.get("RUN_SPARK_TESTS"), reason="Isolated Spark runtime not requested")
def test_real_spark_projection_matches_local_contract(tmp_path, monkeypatch):
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[2]").appName("georgia-synapse-fixture")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
             .config("spark.driver.bindAddress", "127.0.0.1").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        identity = "a" * 64
        prefix = f"normalized/{identity}/{'b' * 32}"
        folder = tmp_path / prefix
        from etl.stream import ingest
        import io
        from .test_etl import fixture
        result = ingest(io.BytesIO(json.dumps(fixture()).encode()), folder, identity)
        run_id = "c" * 32
        con = connect([folder], tmp_path / "scratch")
        try:
            rate_edges = con.execute("""
                SELECT s.code_type,s.code,s.code_version,s.arrangement,
                       p.price_key,p.amount,p.price_type,p.expiration,
                       to_json(p.service_codes) service_codes,p.billing_class,p.setting,
                       to_json(p.modifiers) modifiers,p.additional_information,p.set_key,
                       split_part(g.ref_key,':',1)||':'||g.group_id public_group,
                       to_json(g.networks) networks
                FROM prices p JOIN sets r USING(set_key) JOIN services s USING(service_key)
                JOIN set_groups e USING(set_key) JOIN groups g ON e.group_id=g.group_id
                    AND split_part(e.set_key,':',1)=split_part(g.ref_key,':',1)
            """).fetch_arrow_table()
            # DuckDB JSON has extension metadata that is irrelevant to the SQL output.
            rate_edges = pa.Table.from_arrays(list(rate_edges.columns), names=rate_edges.column_names)
        finally:
            con.close()
        destination = tmp_path / "runs" / run_id / "rate_edges"
        destination.mkdir(parents=True)
        pq.write_table(rate_edges, destination / "part.parquet")
        run = dict(run_id=run_id, sources=[dict(prefix=prefix)])
        objects = {}
        for kind, frame, order, reducer in projection_frames(spark, tmp_path.as_posix(), run):
            output = artifact_frame(spark, frame, order, reducer, 2)
            spool = tmp_path / ("spool-" + kind)
            output.write.option("maxRecordsPerFile", 16).parquet(spool.as_posix())
            for path in spool.glob("*.parquet"):
                for row in pq.read_table(path).to_pylist():
                    objects[row["name"]] = json.loads(gzip.decompress(row["payload"]))
        local = tmp_path / "local-public"
        build([folder], local, [], "fixture", tmp_path / "scratch-local")
        expected = {p.relative_to(local).as_posix().removesuffix(".gz"): read_gzip_json(p)
                    for p in local.rglob("*.json.gz") if p.name != "manifest.json.gz"}
        assert objects == expected
        assert objects[f"codes/{digest('CPT:00123')}/0.json"][0]["amount"] == "123.45000000"
        # Exercise the actual driver spool reader, immutable uploads and pointer gate too.
        import etl.synapse_job as job
        monkeypatch.setattr(job, "publication_lease", lambda _: nullcontext(lambda: None))

        class FileContainer(MemoryContainer):
            def get_blob_client(self, name):
                path = tmp_path / name
                if path.is_file():
                    def download():
                        def readinto(stream):
                            with path.open("rb") as source:
                                import shutil
                                shutil.copyfileobj(source, stream)
                        return SimpleNamespace(readinto=readinto)
                    return SimpleNamespace(download_blob=download)
                return super().get_blob_client(name)

            def list_blobs(self, name_starts_with):
                return [SimpleNamespace(name=p.relative_to(tmp_path).as_posix(), size=p.stat().st_size)
                        for p in (tmp_path / name_starts_with).rglob("*") if p.is_file()]

        private, serving = FileContainer(), MemoryContainer()
        source = dict(result, id=identity, prefix=prefix, sha256="d" * 64, filename="fixture.json.gz", plans=[])
        run.update(snapshot="e" * 64, index_date="2026-11-02", prepared_at="2026-11-05T00:00:00+00:00",
                   sources=[source])
        put_json(private, f"runs/{run_id}/run.json", run)
        job.project(spark, private, serving, "unused", run_id, partitions=2, lake=tmp_path.as_posix())
        active = serving.data["active.json"]
        assert json.loads(active)["snapshot"] == "e" * 64
        actual = {name.removeprefix(f"snapshots/{'e' * 64}/"): json.loads(gzip.decompress(payload))
                  for name, payload in serving.data.items() if name.endswith(".json") and name != "active.json"}
        assert {k: v for k, v in actual.items() if k != "manifest.json"} == expected
        assert actual["manifest.json"]["code_count"] == 1
        failed_id = "f" * 32
        failed_path = tmp_path / "runs" / failed_id / "rate_edges"
        failed_path.mkdir(parents=True)
        pq.write_table(rate_edges, failed_path / "part.parquet")
        put_json(private, f"runs/{failed_id}/run.json", dict(run, run_id=failed_id, snapshot="f" * 64))
        original_upload = job.upload_public
        uploads = []

        def fail_upload(*args):
            uploads.append(args[1])
            if len(uploads) == 3:
                raise SourceError("Injected interrupted upload")
            return original_upload(*args)

        monkeypatch.setattr(job, "upload_public", fail_upload)
        with pytest.raises(SourceError, match="interrupted"):
            job.project(spark, private, serving, "unused", failed_id, partitions=2, lake=tmp_path.as_posix())
        assert serving.data["active.json"] == active
    finally:
        spark.stop()
