"""Generate clearly synthetic browser-test data; never a production data fallback."""
import json
import sys
from pathlib import Path

from etl.projection import build, key
from .test_etl import write_fixture

root = Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=True)
folder, _ = write_fixture(root)
snapshot = key("synthetic-browser-fixture")
source = {
    "id": "test", "filename": "SYNTHETIC-TEST-FIXTURE.json", "data_date": "2026-10-01",
    "plans": [{"plan_name": "Synthetic test plan"}],
}
build([folder], root / "public" / "snapshots" / snapshot, [source], snapshot, root / "scratch")
(root / "public" / "active.json").write_text(json.dumps({
    "format_version": 1, "snapshot": snapshot,
    "manifest": f"snapshots/{snapshot}/manifest.json", "index_date": "2026-11-02",
}))
