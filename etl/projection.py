import gzip
import hashlib
import json
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

import duckdb

from .schema import SCHEMAS
from .source import atomic_bytes
from .stream import InvalidSource


def key(value):
    return hashlib.sha256(value.encode()).hexdigest()


def connect(folders, scratch, memory="512MB", scratch_limit="2GB"):
    con = duckdb.connect()
    con.execute("SET memory_limit = ?", [memory])
    con.execute("SET threads = 1")
    con.execute("SET enable_progress_bar = false")
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET temp_directory = ?", [str(Path(scratch).resolve())])
    con.execute("SET max_temp_directory_size = ?", [scratch_limit])
    for table in SCHEMAS:
        files = [str(file) for folder in folders for file in sorted((Path(folder) / table).glob("*.parquet"))]
        if not files:
            raise InvalidSource(f"Missing normalized table {table}")
        con.read_parquet(files).create_view(table)
    return con


def validate(con):
    checks = [
        ("duplicate provider group identifiers", """
            SELECT COUNT(*) FROM (
                SELECT split_part(ref_key, ':', 1), group_id FROM groups GROUP BY ALL HAVING COUNT(*) > 1
            )"""),
        ("unresolved provider references", """
            SELECT COUNT(*) FROM set_groups e LEFT JOIN groups g
              ON e.group_id=g.group_id AND split_part(e.set_key, ':', 1)=split_part(g.ref_key, ':', 1)
            WHERE g.ref_key IS NULL"""),
        ("rate sets without provider references", """
            SELECT COUNT(*) FROM sets s LEFT JOIN set_groups e USING(set_key) WHERE e.set_key IS NULL"""),
        ("rate sets without prices", """
            SELECT COUNT(*) FROM sets s LEFT JOIN prices p USING(set_key) WHERE p.set_key IS NULL"""),
        ("duplicate provider links", """
            SELECT COUNT(*) FROM (
                SELECT set_key, group_id FROM set_groups GROUP BY ALL HAVING COUNT(*) > 1
            )"""),
    ]
    for name, sql in checks:
        count = con.execute(sql).fetchone()[0]
        if count:
            raise InvalidSource(f"{name}: {count}")


def public_json(path, value, limit=4 * 1024 * 1024, check_disk=None):
    text = json.dumps(value, separators=(",", ":"), default=lambda v: str(v) if isinstance(v, Decimal) else v)
    size = len(text.encode())
    if size > limit:
        raise InvalidSource(f"Public object exceeds size limit: {Path(path).name}")
    path = Path(str(path) + ".gz")
    content = gzip.compress(text.encode(), compresslevel=6, mtime=0)
    if check_disk:
        check_disk(path, len(content))
    atomic_bytes(path, content)


def write_pages(root, folder, rows, page_size=100, check_disk=None):
    path = Path(root) / folder
    page = []
    count = page_number = 0
    page_bytes = 2
    for row in rows:
        size = len(json.dumps(row, default=str).encode()) + 1
        if size > 4 * 1024 * 1024:
            raise InvalidSource("A single public result exceeds the object size limit")
        if page and (len(page) >= page_size or page_bytes + size > 2 * 1024 * 1024):
            public_json(path / f"{page_number}.json", page, check_disk=check_disk)
            page_number += 1
            page = []
            page_bytes = 2
        page.append(row)
        page_bytes += size
        count += 1
    if page:
        public_json(path / f"{page_number}.json", page, check_disk=check_disk)
        page_number += 1
    public_json(path / "index.json", {"pages": page_number, "count": count}, check_disk=check_disk)
    return {"pages": page_number, "count": count}


def records(cursor):
    names = [item[0] for item in cursor.description]
    while batch := cursor.fetchmany(1000):
        for row in batch:
            yield dict(zip(names, row))


def build(folders, output, sources, snapshot, scratch, check_disk=None):
    output = Path(output)
    con = connect(folders, scratch)
    try:
        validate(con)
        con.execute("""
            CREATE TEMP VIEW public_edges AS
            SELECT e.set_key,
                split_part(g.ref_key, ':', 1) || ':' || g.group_id AS public_group,
                g.networks
            FROM set_groups e JOIN groups g ON e.group_id=g.group_id
                AND split_part(e.set_key, ':', 1)=split_part(g.ref_key, ':', 1)
        """)
        # A rate references provider groups, not an exploded row for every provider NPI.
        cursor = con.execute("""
            SELECT s.code_type, s.code, s.code_version, s.arrangement,
                   p.price_key, p.amount, p.price_type, p.expiration,
                   p.service_codes, p.billing_class, p.setting, p.modifiers,
                   p.additional_information, p.set_key, e.public_group, e.networks
            FROM prices p JOIN sets r USING(set_key) JOIN services s USING(service_key)
            JOIN public_edges e USING(set_key)
            ORDER BY s.code_type, s.code, p.price_key, e.public_group
        """)
        import itertools
        def prices():
            for _, edges in itertools.groupby(records(cursor), lambda row: row["price_key"]):
                row = next(edges)
                first_group = row.pop("public_group")
                groups = [first_group]
                networks = set(row.pop("networks") or [])
                group_networks = {first_group: sorted(networks)}
                for edge in edges:
                    groups.append(edge["public_group"])
                    networks.update(edge["networks"] or [])
                    group_networks[edge["public_group"]] = edge["networks"] or []
                    if len(groups) > 20000:
                        raise InvalidSource("A price exceeds the supported provider-group page size")
                yield dict(row, groups=groups, networks=sorted(networks), group_networks=group_networks)

        catalogue = defaultdict(dict)
        for (code_type, code), rows in itertools.groupby(prices(), lambda row: (row["code_type"], row["code"])):
            code_key = key(code_type + ":" + code)
            info = write_pages(output, "codes/" + code_key, rows, check_disk=check_disk)
            catalogue[code_key[:2]][code_type + ":" + code] = {"key": code_key, **info}
        for bucket, entries in catalogue.items():
            public_json(output / "catalogue" / f"{bucket}.json", entries, check_disk=check_disk)

        con.execute("CREATE TEMP TABLE used_groups AS SELECT DISTINCT public_group FROM public_edges")
        cursor = con.execute("""
            SELECT m.npi,
                split_part(g.ref_key, ':', 1) || ':' || g.group_id AS public_group
            FROM members m JOIN groups g USING(ref_key)
            JOIN used_groups u ON u.public_group=split_part(g.ref_key, ':', 1) || ':' || g.group_id
            WHERE regexp_full_match(m.npi, '[1-9][0-9]{9}')
            ORDER BY substr(sha256(m.npi), 1, 3), m.npi, public_group
        """)
        # Hash bucket sorting keeps only one bounded provider-index bucket resident.
        bucket_id = None
        bucket = {}
        bucket_bytes = 2
        provider_buckets = []
        previous = None
        for row in records(cursor):
            pair = (row["npi"], row["public_group"])
            if pair == previous:
                continue
            previous = pair
            current = key(row["npi"])[:3]
            if bucket_id is not None and current != bucket_id:
                public_json(output / "providers" / f"{bucket_id}.json", bucket, check_disk=check_disk)
                provider_buckets.append(bucket_id)
                bucket = {}
                bucket_bytes = 2
            bucket_id = current
            bucket_bytes += len(row["npi"]) + len(row["public_group"]) + 16
            if bucket_bytes > 4 * 1024 * 1024:
                raise InvalidSource("Provider index bucket exceeds 4 MiB; increase bucket resolution")
            bucket.setdefault(row["npi"], []).append(row["public_group"])
        if bucket_id is not None:
            public_json(output / "providers" / f"{bucket_id}.json", bucket, check_disk=check_disk)
            provider_buckets.append(bucket_id)

        cursor = con.execute("""
            SELECT DISTINCT e.public_group, s.code_type, s.code
            FROM public_edges e JOIN sets r USING(set_key) JOIN services s USING(service_key)
            ORDER BY e.public_group, s.code_type, s.code
        """)
        for group, rows in itertools.groupby(records(cursor), lambda row: row["public_group"]):
            write_pages(output, "groups/" + key(group), rows, page_size=200, check_disk=check_disk)

        plans = {}
        for source in sources:
            for plan in source["plans"]:
                plans[plan["plan_name"]] = plan
        manifest = {
            "format_version": 1, "snapshot": snapshot,
            "plans": list(plans.values()),
            "sources": sources, "code_count": sum(len(values) for values in catalogue.values()),
            "provider_bucket_algorithm": "sha256-prefix-3",
            "catalogue_buckets": sorted(catalogue),
            "provider_buckets": provider_buckets,
            "disclaimer": "Published negotiated rates, not your out-of-pocket cost. "
                          "Only selected Georgia network files are included; not all Georgia BCBS plans.",
        }
        public_json(output / "manifest.json", manifest, check_disk=check_disk)
        return manifest
    finally:
        con.close()
