import argparse
import gzip
import json
import logging
import os
import shutil
import sys
import time
import resource
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit

import requests

from .projection import build, connect, key, validate
from .source import HashingReader, SourceError, atomic_json, discover_index, read_index, select_files, session
from .stream import ingest
from .budget import DiskBudget

LOG = logging.getLogger("georgia-prices")
PIPELINE_VERSION = "2"
LOOKUP_VERSION = "2"


@contextmanager
def local_lock(root):
    # Kernel-managed locks release when a container is interrupted.
    import fcntl
    with (root / "run.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SourceError("Another local ETL run owns this output directory") from error
        yield


def transform(item, root, client, budget):
    filename = Path(urlsplit(item["path"]).path).name
    with client.head(item["url"], timeout=(15, 120), allow_redirects=True) as response:
        response.raise_for_status()
        headers = {name: response.headers.get(name) for name in ("ETag", "Last-Modified", "Content-Length")}
    validator = headers["ETag"]
    # Without a strong validator we must read the body rather than infer identity from its date.
    reusable = bool(validator and not validator.startswith("W/") and headers["Content-Length"])
    identity = key(json.dumps([PIPELINE_VERSION, item["path"], headers], sort_keys=True))
    folder = root / "files" / identity
    marker = folder / "complete.json"
    if reusable and marker.is_file():
        LOG.info("Reusing completed shard %s", filename)
        return folder, json.loads(marker.read_text())
    temporary = root / "files" / (identity + ".partial")
    if temporary.exists():
        shutil.rmtree(temporary)
        budget.check(force=True)
    temporary.mkdir(parents=True)
    LOG.info("Streaming %s (%s compressed bytes)", filename, headers["Content-Length"] or "unknown")
    started = time.monotonic()
    with client.get(item["url"], stream=True, timeout=(15, 120)) as response:
        response.raise_for_status()
        if validator and response.headers.get("ETag") != validator:
            raise SourceError(f"Source changed between HEAD and GET: {filename}")
        reader = HashingReader(response.raw)
        with gzip.GzipFile(fileobj=reader) as uncompressed:
            result = ingest(uncompressed, temporary, identity, check_disk=budget.check)
        expected = headers["Content-Length"]
        if expected and reader.bytes != int(expected):
            raise SourceError(f"Compressed length mismatch: {filename}")
    con = connect([temporary], root / "scratch")
    try:
        validate(con)
    finally:
        con.close()
    result.update({
        "id": identity, "filename": filename, "source_path": item["path"], "headers": headers,
        "sha256": reader.digest.hexdigest(), "compressed_bytes": reader.bytes,
        "plans": item["plans"], "elapsed_seconds": round(time.monotonic() - started, 3),
        "process_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
    })
    atomic_json(temporary / "complete.json", result)
    if folder.exists():
        old = json.loads(marker.read_text()) if marker.exists() else None
        if old is None or old["sha256"] != result["sha256"]:
            raise SourceError("A completed immutable source identity changed unexpectedly")
        shutil.rmtree(temporary)
    else:
        temporary.rename(folder)
    LOG.info("Completed %s: %s", filename, result["counts"])
    return folder, result


def run(args):
    config = json.loads(Path(args.config).read_text())
    location = args.index or os.environ.get("INDEX_URL") or config.get("index_url")
    root = Path(args.output).resolve()
    root.mkdir(parents=True, exist_ok=True)
    budget = DiskBudget(root, int(args.max_output_gib * 1024 ** 3))
    budget.check(force=True)
    with local_lock(root), session() as client:
        location = location or discover_index(config, client)
        index = read_index(location, client)
        selected = select_files(index, config)
        expected = config.get("expected_file_count")
        if expected is not None and len(selected) != expected:
            raise SourceError(f"Selected source coverage changed: expected {expected} files, found {len(selected)}. "
                              "Review and update metadata/source.json before publication.")
        if args.filename:
            selected = [item for item in selected if Path(urlsplit(item["path"]).path).name == args.filename]
            if not selected:
                raise SourceError("Requested filename is not a selected Georgia source")
        all_count = len(select_files(index, config))
        if args.max_files:
            selected = selected[:args.max_files]
        folders, sources = [], []
        for item in selected:
            folder, result = transform(item, root, client, budget)
            budget.check(force=True)
            # Plan mappings can change even when rate file bytes do not.
            result["plans"] = item["plans"]
            folders.append(folder)
            sources.append(result)
        atomic_json(root / "benchmark.json", {
            "pipeline_version": PIPELINE_VERSION, "selected_file_count": all_count,
            "processed_file_count": len(sources), "index_date": index.get("last_updated_on"),
            "sources": sources,
            "observed_output_high_water_bytes": budget.high_water_bytes,
        })
        if len(selected) != all_count or args.pilot:
            LOG.info("Pilot only: no active public snapshot was published")
            return
        content = [{k: row[k] for k in ("id", "sha256", "plans")} for row in sources]
        snapshot = key(json.dumps([PIPELINE_VERSION, LOOKUP_VERSION, content], sort_keys=True))
        target = root / "public" / "snapshots" / snapshot
        if not (target / "manifest.json.gz").exists():
            staging = root / "public" / "snapshots" / (snapshot + ".partial")
            if staging.exists():
                shutil.rmtree(staging)
                budget.check(force=True)
            public_sources = [{
                "id": row["id"], "filename": row["filename"],
                "data_date": row["metadata"]["last_updated_on"], "sha256": row["sha256"],
                "plans": row["plans"], "schema_version": row["metadata"]["version"],
            } for row in sources]
            build(folders, staging, public_sources, snapshot, root / "scratch", check_disk=budget.check)
            staging.rename(target)
        budget.check(force=True)
        atomic_json(root / "public" / "active.json", {
            "format_version": 1, "snapshot": snapshot,
            "manifest": f"snapshots/{snapshot}/manifest.json", "index_date": index.get("last_updated_on"),
        })
        LOG.info("Published local snapshot %s", snapshot)


def parser():
    result = argparse.ArgumentParser(description="Bounded TiC gzip to Parquet and public lookup files")
    subcommands = result.add_subparsers(dest="command", required=True)
    command = subcommands.add_parser("run")
    command.add_argument("--config", default="metadata/source.json")
    command.add_argument("--index", help="Current local employer index or HTTPS URL (never committed)")
    command.add_argument("--output", required=True)
    command.add_argument("--filename", help="Exact selected gzip filename for a pilot")
    command.add_argument("--max-files", type=int)
    command.add_argument("--pilot", action="store_true", help="Transform only; never publish public data")
    command.add_argument("--max-output-gib", type=float, default=3, help="Local output and scratch ceiling")
    command.set_defaults(action=run)
    command = subcommands.add_parser("test")
    command.set_defaults(action=lambda _: __import__("pytest").main(["-q", "tests"]))
    command = subcommands.add_parser("publish")
    command.add_argument("--output", required=True, help="Locally validated ETL output root")
    command.add_argument("--private-account", required=True)
    command.add_argument("--serving-account", required=True)
    from .azure_publish import publish_command
    command.set_defaults(action=publish_command)
    return result


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parser().parse_args()
    if getattr(args, "max_files", None) is not None and args.max_files < 1:
        raise SystemExit("--max-files must be positive")
    if getattr(args, "max_output_gib", 1) <= 0:
        raise SystemExit("--max-output-gib must be positive")
    try:
        code = args.action(args)
    except requests.RequestException as error:
        response = error.response
        status = response.status_code if response is not None else "transport failure"
        LOG.error("Source request failed (%s). Refresh expired signed links; "
                  "the previous active snapshot is unchanged.", status)
        return 1
    except (ValueError, OSError, EOFError) as error:
        LOG.error("%s: %s", type(error).__name__, error)
        return 1
    return code or 0


if __name__ == "__main__":
    sys.exit(main())
