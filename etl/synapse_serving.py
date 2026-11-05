"""Pure streaming reducers, shared by local tests and Spark partition execution."""

import gzip
import itertools
import json

from .synapse import MAX_JSON, digest, encode
from .source import SourceError


def artifact(name, value):
    payload = encode(value)
    if len(payload) > MAX_JSON:
        raise SourceError(f"Public object exceeds 4 MiB: {name}")
    return name, gzip.compress(payload, compresslevel=6, mtime=0)


def pages(folder, rows, page_size=100):
    page, size, number, count = [], 2, 0, 0
    for row in rows:
        length = len(encode(row)) + 1
        if length > MAX_JSON - 2:
            raise SourceError("A single public result exceeds 4 MiB")
        if page and (len(page) >= page_size or size + length > 2 * 1024 * 1024):
            yield artifact(f"{folder}/{number}.json", page)
            page, size, number = [], 2, number + 1
        page.append(row)
        size += length
        count += 1
    if page:
        yield artifact(f"{folder}/{number}.json", page)
        number += 1
    yield artifact(f"{folder}/index.json", {"pages": number, "count": count})


def prices(edges):
    for _, group in itertools.groupby(edges, lambda row: row["price_key"]):
        first = dict(next(group))
        first.pop("bucket", None)
        group_id = first.pop("public_group")
        networks = first.pop("networks") or []
        groups = [group_id]
        group_networks = {group_id: networks}
        all_networks = set(networks)
        size = len(encode(first)) + len(encode(group_networks))
        for edge in group:
            group_id = edge["public_group"]
            networks = edge["networks"] or []
            groups.append(group_id)
            group_networks[group_id] = networks
            all_networks.update(networks)
            size += len(encode({group_id: networks})) + len(group_id) + 8
            if len(groups) > 20000 or size > MAX_JSON - 1024:
                raise SourceError("A price exceeds bounded provider-group context")
        yield dict(first, groups=groups, group_networks=group_networks, networks=sorted(all_networks))


def code_objects(rows):
    for bucket, entries in itertools.groupby(rows, lambda row: row["bucket"]):
        catalogue = {}
        for (code_type, code), edges in itertools.groupby(entries, lambda row: (row["code_type"], row["code"])):
            code_key = digest(code_type + ":" + code)
            for name, payload in pages("codes/" + code_key, prices(edges)):
                yield name, payload
                if name.endswith("/index.json"):
                    info = json.loads(gzip.decompress(payload))
            catalogue[code_type + ":" + code] = dict(key=code_key, **info)
            if len(encode(catalogue)) > MAX_JSON:
                raise SourceError("Catalogue bucket exceeds 4 MiB")
        yield artifact(f"catalogue/{bucket}.json", catalogue)


def provider_objects(rows):
    for bucket, entries in itertools.groupby(rows, lambda row: row["bucket"]):
        providers, size = {}, 2
        for row in entries:
            size += len(row["npi"]) + len(row["public_group"]) + 16
            if size > MAX_JSON:
                raise SourceError("Provider bucket exceeds 4 MiB; review bucket resolution")
            providers.setdefault(row["npi"], []).append(row["public_group"])
        yield artifact(f"providers/{bucket}.json", providers)


def group_objects(rows):
    for group, entries in itertools.groupby(rows, lambda row: row["public_group"]):
        yield from pages("groups/" + digest(group), (
            {key: row[key] for key in ("public_group", "code_type", "code")} for row in entries
        ), page_size=200)
