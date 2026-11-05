import copy
import gzip
import io
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from etl.projection import build, connect, key, public_json, validate, write_pages
from etl.source import SourceError, read_gzip_json, select_files
from etl.stream import InvalidSource, ingest


def fixture():
    return {
        "reporting_entity_name": "Test Network",
        "last_updated_on": "2026-10-01",
        "version": "2.0.0",
        "provider_references": [{
            "provider_group_id": 1, "network_name": ["BLUECHOICE PPO"],
            "provider_groups": [{"npi": [1111111111, 2222222222], "tin": {
                "type": "ein", "value": "12-3456789", "business_name": "Test practice",
            }}],
        }],
        "in_network": [{
            "billing_code_type": "CPT", "billing_code_type_version": "2026",
            "billing_code": "00123", "name": "Source service", "description": "Private source description",
            "negotiation_arrangement": "ffs",
            "negotiated_rates": [{
                "provider_references": [1],
                "negotiated_prices": [{
                    "negotiated_rate": 123.45, "negotiated_type": "negotiated",
                    "expiration_date": "9999-12-31", "billing_class": "professional",
                    "setting": "outpatient", "service_code": ["11"], "billing_code_modifier": ["26"],
                }],
            }],
        }],
    }


def write_fixture(tmp_path, data=None, source="test", batch_rows=2):
    folder = tmp_path / source
    content = json.dumps(data or fixture()).encode()
    result = ingest(io.BytesIO(content), folder, source, batch_rows=batch_rows)
    return folder, result


def table(folder, name):
    return pq.read_table(folder / name).to_pylist()


def lookup(path):
    return read_gzip_json(Path(str(path) + ".gz"))


def test_stream_preserves_code_money_and_context(tmp_path):
    folder, result = write_fixture(tmp_path)
    assert result["metadata"]["version"] == "2.0.0"
    assert table(folder, "services")[0]["code"] == "00123"
    price = table(folder, "prices")[0]
    assert str(price["amount"]) == "123.45000000"
    assert price["modifiers"] == ["26"]
    assert price["service_codes"] == ["11"]
    con = connect([folder], tmp_path / "scratch")
    validate(con)
    con.close()


def test_arrays_stream_in_batches_and_field_order_is_irrelevant(tmp_path):
    data = fixture()
    group = data["provider_references"][0]
    group["provider_groups"][0]["npi"] = list(range(1000000000, 1000000101))
    data["provider_references"][0] = {
        "provider_groups": group["provider_groups"],
        "network_name": group["network_name"], "provider_group_id": group["provider_group_id"],
    }
    data = dict(reversed(list(data.items())))
    folder, _ = write_fixture(tmp_path, data, batch_rows=10)
    files = list((folder / "members").glob("*.parquet"))
    assert len(files) == 1
    assert pq.ParquetFile(files[0]).metadata.num_row_groups == 11
    assert len(table(folder, "members")) == 101
    con = connect([folder], tmp_path / "scratch")
    validate(con)
    con.close()


def test_context_objects_are_bounded(tmp_path):
    data = fixture()
    data["in_network"][0]["negotiated_rates"][0]["negotiated_prices"][0]["additional_information"] = "x" * 1100000
    with pytest.raises(InvalidSource, match="exceeds"):
        write_fixture(tmp_path, data)


def test_corrupt_gzip_does_not_finish(tmp_path):
    compressed = gzip.compress(json.dumps(fixture()).encode())
    with pytest.raises((EOFError, OSError)):
        with gzip.GzipFile(fileobj=io.BytesIO(compressed[:-5])) as stream:
            ingest(stream, tmp_path / "bad", "bad")


def test_unknown_schema_fails(tmp_path):
    data = fixture()
    data["version"] = "9.0.0"
    with pytest.raises(InvalidSource, match="Unsupported"):
        write_fixture(tmp_path, data)


def test_missing_setting_fails(tmp_path):
    data = fixture()
    del data["in_network"][0]["negotiated_rates"][0]["negotiated_prices"][0]["setting"]
    with pytest.raises(InvalidSource, match="missing"):
        write_fixture(tmp_path, data)


def test_severity_dependent_prices_do_not_silently_lose_context(tmp_path):
    data = fixture()
    data["in_network"][0]["severity_of_illness"] = "major"
    with pytest.raises(InvalidSource, match="Severity-dependent"):
        write_fixture(tmp_path, data)


def test_unresolved_reference_blocks_publication(tmp_path):
    data = fixture()
    data["in_network"][0]["negotiated_rates"][0]["provider_references"] = [999]
    folder, _ = write_fixture(tmp_path, data)
    with pytest.raises(InvalidSource, match="unresolved"):
        build([folder], tmp_path / "public", [], "snapshot", tmp_path / "scratch")
    assert not (tmp_path / "public" / "manifest.json.gz").exists()


def test_same_group_id_in_different_shards_is_scoped(tmp_path):
    first, _ = write_fixture(tmp_path, source="first")
    second, _ = write_fixture(tmp_path, source="second")
    con = connect([first, second], tmp_path / "scratch")
    validate(con)
    con.close()


def test_public_projection_and_provider_reverse_index(tmp_path):
    folder, _ = write_fixture(tmp_path)
    out = tmp_path / "public"
    source = {"id": "test", "plans": [{"plan_name": "Test plan"}], "data_date": "2026-10-01"}
    manifest = build([folder], out, [source], "snapshot", tmp_path / "scratch")
    assert manifest["code_count"] == 1
    code = key("CPT:00123")
    catalogue = lookup(out / "catalogue" / f"{code[:2]}.json")
    assert catalogue["CPT:00123"]["pages"] == 1
    page = lookup(out / "codes" / code / "0.json")
    assert page[0]["groups"] == ["test:1"]
    assert page[0]["amount"] == "123.45000000"
    assert page[0]["networks"] == ["BLUECHOICE PPO"]
    assert page[0]["group_networks"] == {"test:1": ["BLUECHOICE PPO"]}
    provider = lookup(out / "providers" / f"{key('1111111111')[:3]}.json")
    assert provider["1111111111"] == ["test:1"]
    group = lookup(out / "groups" / key("test:1") / "0.json")
    assert group[0]["code"] == "00123"
    for path in out.rglob("*.json.gz"):
        text = json.dumps(read_gzip_json(path))
        assert "12-3456789" not in text
        assert "Private source description" not in text


def test_price_type_is_not_coerced_to_dollars(tmp_path):
    data = fixture()
    data["in_network"][0]["negotiated_rates"][0]["negotiated_prices"][0]["negotiated_type"] = "percentage"
    folder, _ = write_fixture(tmp_path, data)
    assert table(folder, "prices")[0]["price_type"] == "percentage"


def test_index_selection_preserves_both_plans_and_deduplicates_signatures():
    config = {"publisher": "P", "plan_names": ["A", "B"], "file_description": "Georgia"}
    item = {
        "in_network_files": [{"description": "Georgia", "location": "https://example.com/a.gz?signature=first"}],
        "reporting_plans": [{"plan_name": "A", "plan_id": "1"}, {"plan_name": "B", "plan_id": "1"}],
    }
    duplicate = copy.deepcopy(item)
    duplicate["in_network_files"][0]["location"] = "https://example.com/a.gz?signature=second"
    selected = select_files({"reporting_entity_name": "P", "reporting_structure": [item, duplicate]}, config)
    assert len(selected) == 1
    assert len(selected[0]["plans"]) == 2
    assert "signature" not in selected[0]["path"]
    with pytest.raises(SourceError, match="publisher"):
        select_files({"reporting_entity_name": "Wrong"}, config)


def test_npi_bucket_contains_all_np_is_in_same_hash_bucket(tmp_path):
    data = fixture()
    candidates = {}
    pair = None
    for number in range(1000000000, 1000001000):
        prefix = key(str(number))[:3]
        if prefix in candidates:
            pair = [candidates[prefix], number]
            break
        candidates[prefix] = number
    data["provider_references"][0]["provider_groups"][0]["npi"] = pair
    folder, _ = write_fixture(tmp_path, data)
    out = tmp_path / "public"
    build([folder], out, [], "snapshot", tmp_path / "scratch")
    bucket = lookup(out / "providers" / f"{key(str(pair[0]))[:3]}.json")
    assert set(bucket) == set(map(str, pair))


def test_duplicate_npi_membership_does_not_duplicate_public_group(tmp_path):
    data = fixture()
    data["provider_references"][0]["provider_groups"] *= 2
    folder, _ = write_fixture(tmp_path, data)
    out = tmp_path / "public"
    build([folder], out, [], "snapshot", tmp_path / "scratch")
    bucket = lookup(out / "providers" / f"{key('1111111111')[:3]}.json")
    assert bucket["1111111111"] == ["test:1"]


def test_public_pages_obey_row_and_byte_limits(tmp_path):
    rows = ({"id": number} for number in range(201))
    assert write_pages(tmp_path, "small", rows) == {"pages": 3, "count": 201}
    large = ({"id": number, "context": "x" * (1100 * 1024)} for number in range(3))
    assert write_pages(tmp_path, "large", large) == {"pages": 3, "count": 3}
    assert all(len(gzip.decompress(path.read_bytes())) <= 2 * 1024 * 1024
               for path in (tmp_path / "large").glob("*.json.gz"))
    with pytest.raises(InvalidSource, match="size limit"):
        public_json(tmp_path / "oversized.json", {"context": "x" * (4 * 1024 * 1024)})
    assert not (tmp_path / "oversized.json.gz").exists()


def test_unused_provider_groups_are_not_publicly_indexed(tmp_path):
    data = fixture()
    unused = copy.deepcopy(data["provider_references"][0])
    unused["provider_group_id"] = 2
    unused["provider_groups"][0]["npi"] = [3333333333]
    data["provider_references"].append(unused)
    folder, _ = write_fixture(tmp_path, data)
    out = tmp_path / "public"
    build([folder], out, [], "snapshot", tmp_path / "scratch")
    bucket = out / "providers" / f"{key('3333333333')[:3]}.json.gz"
    assert not bucket.exists() or "3333333333" not in read_gzip_json(bucket)
