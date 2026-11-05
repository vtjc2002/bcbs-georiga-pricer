import gzip
import hashlib
import io
import json
import os
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class SourceError(ValueError):
    pass


def session():
    client = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    client.mount("https://", HTTPAdapter(max_retries=retry))
    client.headers["User-Agent"] = "GeorgiaPriceChecker/0.1 (public TiC research)"
    return client


def require_https(url):
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise SourceError("Source URLs must use HTTPS without embedded credentials")
    return url


def read_index(location, client):
    limit = 32 * 1024 * 1024
    if location.startswith("https://"):
        require_https(location)
        with client.get(location, timeout=(15, 120), stream=True) as response:
            response.raise_for_status()
            chunks = []
            size = 0
            for block in response.iter_content(65536):
                size += len(block)
                if size > limit:
                    raise SourceError("Index exceeds the 32 MiB safety limit")
                chunks.append(block)
            content = b"".join(chunks)
    else:
        path = Path(location)
        if path.stat().st_size > limit:
            raise SourceError("Index exceeds the 32 MiB safety limit")
        content = path.read_bytes()
    if content.startswith(b"\x1f\x8b"):
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as stream:
            content = stream.read(limit + 1)
        if len(content) > limit:
            raise SourceError("Decompressed index exceeds the 32 MiB safety limit")
    return json.loads(content.decode("utf-8-sig"))


def discover_index(config, client):
    discovery = config.get("index_discovery")
    if not discovery:
        raise SourceError("No current index supplied and no publisher metadata discovery configured")
    metadata = read_index(require_https(discovery["metadata_url"]), client)
    candidates = {}
    for item in metadata.get("files", []):
        name = item.get("fileName", "")
        if name[10:] != "_" + discovery["filename_suffix"] or item.get("fileSchema") != "TABLE_OF_CONTENTS":
            continue
        if item.get("reportingEntityName") != config["publisher"]:
            raise SourceError("Discovered employer index publisher changed")
        stamp = date.fromisoformat(name[:10])
        path = f"{stamp.isoformat()}/tableOfContents/{name}"
        if item.get("filePath") != path or item.get("lastUpdatedOn") != stamp.isoformat():
            raise SourceError("Discovered employer index path or date is inconsistent")
        candidates.setdefault(stamp, set()).add(path)
    if not candidates:
        raise SourceError("Publisher metadata contains no matching employer index")
    latest = candidates[max(candidates)]
    if len(latest) != 1:
        raise SourceError("Publisher metadata contains ambiguous current employer indexes")
    return require_https(discovery["base_url"].rstrip("/") + "/" + next(iter(latest)))


def select_files(index, config):
    if index.get("reporting_entity_name") != config["publisher"]:
        raise SourceError("Index publisher does not match configured publisher")
    wanted = set(config["plan_names"])
    found = set()
    files = {}
    for structure in index.get("reporting_structure", []):
        plans = [p for p in structure.get("reporting_plans", []) if p.get("plan_name") in wanted]
        if not plans:
            continue
        for item in structure.get("in_network_files", []):
            if item.get("description") != config["file_description"]:
                continue
            url = require_https(item["location"])
            if config.get("allowed_hosts") and urlsplit(url).hostname not in config["allowed_hosts"]:
                raise SourceError("Georgia source hostname changed; review the source configuration")
            path = urlsplit(url)._replace(query="", fragment="").geturl()
            # A rotated signature is the same logical source, not a second file.
            entry = files.setdefault(path, {
                "url": url, "path": path, "description": item["description"], "plans": {},
            })
            for plan in plans:
                name = plan["plan_name"]
                found.add(name)
                entry["plans"][name] = {
                    key: plan.get(key) for key in ("plan_name", "plan_id_type", "plan_id", "plan_market_type")
                }
    if found != wanted:
        raise SourceError(f"Missing selected Georgia plan mappings: {sorted(wanted - found)}")
    if not files:
        raise SourceError("Index contains no selected Georgia files")
    return [dict(entry, plans=list(entry["plans"].values())) for _, entry in sorted(files.items())]


def read_gzip_json(path, limit=4 * 1024 * 1024):
    with gzip.open(path, "rb") as stream:
        content = stream.read(limit + 1)
    if len(content) > limit:
        raise SourceError("Compressed JSON exceeds the uncompressed object size limit")
    return json.loads(content)


def atomic_bytes(path, content):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(content)
    os.replace(temporary, path)


def atomic_json(path, value):
    atomic_bytes(path, json.dumps(value, separators=(",", ":"), ensure_ascii=True).encode())


class HashingReader:
    def __init__(self, raw):
        self.raw = raw
        self.digest = hashlib.sha256()
        self.bytes = 0

    def read(self, size=-1):
        data = self.raw.read(size)
        self.digest.update(data)
        self.bytes += len(data)
        return data
