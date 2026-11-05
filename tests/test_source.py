import gzip
import pytest

from etl import source


def test_compressed_local_index_is_read_with_uncompressed_size_limit(tmp_path):
    path = tmp_path / "index.json.gz"
    path.write_bytes(gzip.compress(b'{"reporting_entity_name":"P"}'))
    assert source.read_index(str(path), None)["reporting_entity_name"] == "P"
    path.write_bytes(gzip.compress(b"x" * (32 * 1024 * 1024 + 1)))
    with pytest.raises(source.SourceError, match="Decompressed index exceeds"):
        source.read_index(str(path), None)


def test_discovery_uses_published_paths_not_date_substitution(monkeypatch):
    config = {
        "publisher": "P", "index_discovery": {
            "metadata_url": "https://example.com/latest_metadata.json",
            "base_url": "https://example.com/published/",
            "filename_suffix": "EMPLOYER_index.json.gz",
        },
    }
    metadata = {"files": [{
        "fileName": f"{stamp}_EMPLOYER_index.json.gz",
        "fileSchema": "TABLE_OF_CONTENTS", "reportingEntityName": "P",
        "lastUpdatedOn": stamp, "filePath": f"{stamp}/tableOfContents/{stamp}_EMPLOYER_index.json.gz",
    } for stamp in ("2026-10-02", "2026-11-02")]}
    monkeypatch.setattr(source, "read_index", lambda *_: metadata)
    assert source.discover_index(config, None) == (
        "https://example.com/published/2026-11-02/tableOfContents/2026-11-02_EMPLOYER_index.json.gz"
    )
    metadata["files"][-1]["filePath"] = "../unexpected"
    with pytest.raises(source.SourceError, match="inconsistent"):
        source.discover_index(config, None)
    metadata["files"] = []
    with pytest.raises(source.SourceError, match="no matching"):
        source.discover_index(config, None)
