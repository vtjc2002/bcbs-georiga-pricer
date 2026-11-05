import pytest

from etl.budget import DiskBudget
from etl.source import SourceError


def test_disk_budget_stops_work_before_publication(tmp_path):
    (tmp_path / "output").write_bytes(b"x" * 100)
    budget = DiskBudget(tmp_path, 99)
    with pytest.raises(SourceError, match="budget exceeded"):
        budget.check(force=True)
    assert budget.high_water_bytes == 100


def test_incremental_budget_does_not_rescan_for_each_write(tmp_path, monkeypatch):
    path = tmp_path / "output"
    path.write_bytes(b"x" * 50)
    budget = DiskBudget(tmp_path, 100)
    budget.check(force=True)

    def unexpected_scan(*_):
        raise AssertionError("Per-write budget accounting must not recursively scan the dataset")

    monkeypatch.setattr(type(tmp_path), "rglob", unexpected_scan)
    budget.check(path, 60)
    budget.check(path, 75)
    assert budget.total_bytes == 75
    with pytest.raises(SourceError, match="budget exceeded"):
        budget.check(tmp_path / "second", 26)
