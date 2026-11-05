import json
from collections import Counter
from decimal import Decimal
from pathlib import Path

import ijson
import pyarrow as pa
import pyarrow.parquet as pq

from .schema import SCHEMAS


class InvalidSource(ValueError):
    pass


class ParquetSink:
    """Bounded batches; every table exists even when its input is empty."""

    def __init__(self, path, batch_rows=10000, batch_bytes=4 * 1024 * 1024, check_disk=None):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        self.rows = {name: [] for name in SCHEMAS}
        self.sizes = Counter()
        self.parts = Counter()
        self.counts = Counter()
        self.batch_rows = batch_rows
        self.batch_bytes = batch_bytes
        self.writers = {}
        self.check_disk = check_disk

    def add(self, table, **row):
        size = len(json.dumps(row, default=str))
        if size > self.batch_bytes:
            raise InvalidSource(f"Oversized {table} record: {size} bytes")
        if self.rows[table] and self.sizes[table] + size > self.batch_bytes:
            self.flush(table)
        self.rows[table].append(row)
        self.sizes[table] += size
        self.counts[table] += 1
        if len(self.rows[table]) >= self.batch_rows:
            self.flush(table)

    def flush(self, table):
        folder = self.path / table
        folder.mkdir(exist_ok=True)
        data = pa.Table.from_pylist(self.rows[table], schema=SCHEMAS[table])
        path = folder / f"{self.parts[table]:06d}.parquet"
        if table not in self.writers:
            self.writers[table] = pq.ParquetWriter(path, SCHEMAS[table], compression="zstd")
        self.writers[table].write_table(data, row_group_size=self.batch_rows)
        if path.stat().st_size >= 64 * 1024 * 1024:
            self.writers.pop(table).close()
            self.parts[table] += 1
        self.rows[table].clear()
        self.sizes[table] = 0
        if self.check_disk:
            self.check_disk(path, path.stat().st_size)

    def close(self):
        for table in SCHEMAS:
            if self.rows[table] or (not self.parts[table] and table not in self.writers):
                self.flush(table)
        for table, writer in self.writers.items():
            writer.close()
            if self.check_disk:
                path = self.path / table / f"{self.parts[table]:06d}.parquet"
                self.check_disk(path, path.stat().st_size)
        self.writers.clear()


def small_value(events, event, value, budget=None):
    # Only small price/context objects use this collector, never service or group arrays.
    budget = budget if budget is not None else [0]
    budget[0] += len(str(value)) + 16
    if budget[0] > 1024 * 1024:
        raise InvalidSource("Nested price/context object exceeds 1 MiB")
    if event == "start_map":
        result = {}
        while True:
            _, kind, key = next(events)
            if kind == "end_map":
                return result
            if kind != "map_key":
                raise InvalidSource("Expected object key")
            _, child_kind, child = next(events)
            result[key] = small_value(events, child_kind, child, budget)
    if event == "start_array":
        result = []
        while True:
            _, kind, child = next(events)
            if kind == "end_array":
                return result
            result.append(small_value(events, kind, child, budget))
    if event in {"string", "number", "boolean", "null"}:
        return value
    raise InvalidSource(f"Unexpected JSON event {event}")


def strings(value, field):
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise InvalidSource(f"{field} must be an array of strings")
    return value


def ingest(stream, path, source_id, batch_rows=10000, check_disk=None, sink=None):
    sink = sink if sink is not None else ParquetSink(path, batch_rows=batch_rows, check_disk=check_disk)
    events = iter(ijson.parse(stream))
    metadata = {}
    seen = set()
    ref_number = subgroup_number = service_number = set_number = price_number = 0
    reference = organization = service = None
    scalar = {"string", "number", "boolean", "null"}
    for prefix, event, value in events:
        if "." not in prefix and event in scalar:
            metadata[prefix] = str(value) if value is not None else None
        if prefix == "provider_references" and event == "start_array":
            seen.add("provider_references")
        elif prefix == "in_network" and event == "start_array":
            seen.add("in_network")
        elif prefix == "provider_references.item" and event == "start_map":
            ref_number += 1
            reference = {"ref_key": f"{source_id}:r{ref_number}", "networks": []}
            subgroup_number = 0
        elif prefix == "provider_references.item.provider_group_id" and event in scalar:
            reference["group_id"] = str(value)
        elif prefix == "provider_references.item.network_name.item" and event == "string":
            reference["networks"].append(value)
        elif prefix == "provider_references.item.location":
            raise InvalidSource("External provider references are not supported by this adapter")
        elif prefix == "provider_references.item.provider_groups.item" and event == "start_map":
            subgroup_number += 1
            organization = {"subgroup_key": f"{reference['ref_key']}:s{subgroup_number}"}
        elif prefix == "provider_references.item.provider_groups.item.npi.item" and event in scalar:
            sink.add("members", ref_key=reference["ref_key"],
                     subgroup_key=organization["subgroup_key"], npi=str(value))
        elif prefix.startswith("provider_references.item.provider_groups.item.tin.") and event in scalar:
            name = prefix.rsplit(".", 1)[1]
            key = {"type": "tin_type", "value": "tin_value", "business_name": "business_name"}.get(name)
            if key:
                organization[key] = str(value)
        elif prefix == "provider_references.item.provider_groups.item" and event == "end_map":
            if not organization.get("tin_type") or not organization.get("tin_value"):
                raise InvalidSource("Provider organization lacks a typed TIN")
            sink.add("organizations", **organization)
        elif prefix == "provider_references.item" and event == "end_map":
            if "group_id" not in reference:
                raise InvalidSource("Provider reference lacks provider_group_id")
            sink.add("groups", **reference)
        elif prefix == "in_network.item" and event == "start_map":
            service_number += 1
            set_number = 0
            service = {"service_key": f"{source_id}:c{service_number}"}
        elif prefix == "in_network.item.severity_of_illness":
            raise InvalidSource("Severity-dependent services require an adapter that preserves their context")
        elif prefix in {"in_network.item.bundled_codes", "in_network.item.covered_services"} and event == "start_array":
            key = "bundled_json" if prefix.endswith("bundled_codes") else "covered_json"
            service[key] = json.dumps(small_value(events, event, value), default=str)
        elif prefix.startswith("in_network.item.") and prefix.count(".") == 2 and event in scalar:
            key = {
                "billing_code_type": "code_type", "billing_code_type_version": "code_version",
                "billing_code": "code", "name": "name", "description": "description",
                "negotiation_arrangement": "arrangement",
            }.get(prefix.rsplit(".", 1)[1])
            if key:
                service[key] = str(value)
        elif prefix == "in_network.item.negotiated_rates.item" and event == "start_map":
            set_number += 1
            price_number = 0
            set_key = f"{service['service_key']}:s{set_number}"
            sink.add("sets", set_key=set_key, service_key=service["service_key"])
        elif prefix == "in_network.item.negotiated_rates.item.provider_references.item" and event in scalar:
            sink.add("set_groups", set_key=set_key, group_id=str(value))
        elif prefix == "in_network.item.negotiated_rates.item.provider_groups":
            raise InvalidSource("Inline legacy provider groups require a separate schema adapter")
        elif prefix == "in_network.item.negotiated_rates.item.negotiated_prices.item" and event == "start_map":
            price_number += 1
            price = small_value(events, event, value)
            required = {"negotiated_rate", "negotiated_type", "expiration_date", "billing_class", "setting"}
            if not required <= price.keys():
                raise InvalidSource(f"Price missing fields: {sorted(required - price.keys())}")
            amount = Decimal(str(price["negotiated_rate"]))
            if not amount.is_finite() or amount < 0:
                raise InvalidSource("Price must be finite and nonnegative")
            if amount != amount.quantize(Decimal("0.00000001")):
                raise InvalidSource("Price exceeds supported decimal precision")
            sink.add("prices", price_key=f"{set_key}:p{price_number}", set_key=set_key,
                     amount=amount, price_type=price["negotiated_type"],
                     expiration=price["expiration_date"], billing_class=price["billing_class"],
                     setting=price["setting"],
                     service_codes=strings(price.get("service_code", []), "service_code"),
                     modifiers=strings(price.get("billing_code_modifier", []), "billing_code_modifier"),
                     additional_information=price.get("additional_information"))
        elif prefix == "in_network.item" and event == "end_map":
            required = {"code_type", "code_version", "code", "arrangement"}
            if not required <= service.keys():
                raise InvalidSource("Service lacks billing code context")
            sink.add("services", **service)
    if not {"provider_references", "in_network"} <= seen:
        raise InvalidSource("Missing root provider_references or in_network arrays")
    if not metadata.get("version", "").startswith("2."):
        raise InvalidSource(f"Unsupported schema version: {metadata.get('version')}")
    if not metadata.get("last_updated_on") or not sink.counts["prices"]:
        raise InvalidSource("Source has no data date or negotiated prices")
    sink.close()
    return {"metadata": metadata, "counts": dict(sink.counts)}
